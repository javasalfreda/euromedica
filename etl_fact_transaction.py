import os
import json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

# --- KONFIGURASI API ---
BASE_URL = "https://erpeuromedicagroup.com"
LOGIN_URL = f"{BASE_URL}/api/method/login"
REPORT_URL = f"{BASE_URL}/api/method/frappe.desk.query_report.run"
USERNAME = "denny.asarias.dei@skinplusclinic.com"
PASSWORD = "260628"
REPORT_NAME = "Item-wise Sales History"

def etl_erp_to_parquet():
    # Menggunakan datetime hari ini (karena dijalankan harian oleh cron)
    execution_date = datetime.utcnow()
    from_date = datetime(2026, 1, 1)
    #to_date = datetime(2026, 12, 31)
    #from_date = (execution_date - relativedelta(months=1)).replace(day=1)
    to_date = execution_date
    
    # Di GitHub Actions, kita bisa simpan langsung di folder workspace saat ini
    file_path = f"sales_history_up_to_{to_date.strftime('%Y%m%d')}.parquet"
    
    session = requests.Session()
    session.post(LOGIN_URL, data={"usr": USERNAME, "pwd": PASSWORD}).raise_for_status()

    FILTERS = {
        "company": "SKIN+ DEI",
        "from_date": from_date.strftime("%Y-%m-%d"),
        "to_date": to_date.strftime("%Y-%m-%d"),
        "item_group": None,
        "territory": None
    }

    payload = {
        "report_name": REPORT_NAME,
        "filters": json.dumps(FILTERS),
        "ignore_prepared_report": 1
    }

    print(f"📡 Mengambil data: {FILTERS['from_date']} s/d {FILTERS['to_date']}")
    response = session.post(REPORT_URL, json=payload)
    response.raise_for_status()
    
    data = response.json().get("message", {}).get("result", [])
    
    if not data or len(data) <= 1:
        print(f"⚠️ Tidak ada data ditemukan.")
        return None

    df = pd.DataFrame(data[:-1])
    df["_extracted_at"] = datetime.now()
    df.to_parquet(file_path, index=False)
    print(f"✅ Berhasil disimpan lokal: {file_path} | Rows: {len(df)}")
    return file_path

def load_parquet_to_bq(file_path, client):
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return

    table_id = "euromedica-495509.raw.raw_trx" 
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    
    job.result()
    print(f"🚀 Berhasil memuat {job.output_rows} baris ke BigQuery.")

# def transform_bq(client):
#     query = """
#         CREATE OR REPLACE TABLE `euromedica-495509.database.fact_transaction` AS
#         SELECT *
#         FROM `euromedica-495509.database.fact_transaction`
#         WHERE DATE(transaction_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH))

#         UNION ALL

#         SELECT * EXCEPT(_extracted_at)
#         FROM `euromedica-495509.raw.raw_trx`
#         WHERE DATE(transaction_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)

#         ORDER BY transaction_date DESC
#     """
#     print("⏳ Menjalankan transformasi SQL di BigQuery...")
#     query_job = client.query(query)
#     query_job.result()
#     print("🎉 Transformasi sukses! Tabel fact_transaction telah diperbarui.")

if __name__ == "__main__":
    # Autentikasi otomatis membaca file json kredensial yang disiapkan oleh GitHub Actions
    # Variabel GOOGLE_APPLICATION_CREDENTIALS akan diset di file workflow YAML
    client = bigquery.Client()
    
    # Jalankan berurutan layaknya task Airflow (task_extract -> task_load -> task_transform)
    saved_file = etl_erp_to_parquet()
    if saved_file:
        load_parquet_to_bq(saved_file, client)
        #transform_bq(client)
