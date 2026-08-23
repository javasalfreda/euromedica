import os
import json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

# ================================
# KONFIGURASI ERP & BIGQUERY
# ================================
BASE_URL = "https://erpeuromedicagroup.com"
LOGIN_URL = f"{BASE_URL}/api/method/login"
REPORT_URL = f"{BASE_URL}/api/method/frappe.desk.query_report.run"

PASSWORD = "260628"
REPORT_NAME = "Item-wise Sales History"

# Separasi Layer Data Warehouse
RAW_TABLE_ID = "euromedica-495509.raw.raw_trx_esc_esl"
PROD_TABLE_ID = "euromedica-495509.database.fact_transaction_esc_esl"

ACCOUNTS = [
    {"username": "denny.asarias.ehl@euromedicagroup.com", "company": "EUROHAIRLAB"},
    {"username": "denny.asarias.esc@euromedicagroup.com", "company": "European Slimming Centre"},
    {"username": "denny.asarias.esl@euromedicagroup.com", "company": "EUROSKINLAB"},
]

def extract_erp_dynamic():
    execution_date = datetime.utcnow()
    from_date = datetime(2025, 1, 1)
    to_date = datetime(2025, 12, 31)
    # from_date = (execution_date - relativedelta(months=1)).replace(day=1)
    # to_date = execution_date

    print(f"📅 Periode ETL (Rolling Window): {from_date.strftime('%Y-%m-%d')} s/d {to_date.strftime('%Y-%m-%d')}")
    all_data = []

    for account in ACCOUNTS:
        USERNAME = account["username"]
        COMPANY = account["company"]

        print(f"\n🔐 Login ke ERP: {USERNAME} ({COMPANY})")
        session = requests.Session()
        
        try:
            login = session.post(LOGIN_URL, data={"usr": USERNAME, "pwd": PASSWORD})
            login.raise_for_status()
        except Exception as e:
            print(f"❌ Login gagal untuk {USERNAME}: {e}")
            continue

        FILTERS = {
            "company": COMPANY,
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

        print(f"📡 Menarik data {COMPANY}...")
        
        try:
            response = session.post(REPORT_URL, json=payload)
            response.raise_for_status()
            result = response.json()
            data = result.get("message", {}).get("result", [])
        except Exception as e:
            print(f"❌ Gagal menarik data {COMPANY}: {e}")
            continue

        if len(data) > 1:
            cleaned_batch = data[:-1]
            for row in cleaned_batch:
                row["_company"] = COMPANY
                row["_year"] = execution_date.year
                row["_month"] = execution_date.month
                row["_extracted_at"] = str(datetime.now())

            all_data.extend(cleaned_batch)
            print(f"✅ Berhasil mengambil {len(cleaned_batch)} baris data {COMPANY}")
        else:
            print(f"⚠️ Tidak ada data ditemukan untuk {COMPANY} di periode ini.")

    return all_data

def load_data_to_staging(all_data, client):
    if not all_data:
        print("❌ Tidak ada data yang akan di-load ke BigQuery Staging.")
        return False

    print(f"\n🚀 Memuat data rolling ke Staging ({RAW_TABLE_ID})...")
    df = pd.DataFrame(all_data)

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).replace(["nan", "None", "<NA>"], "")

    today_str = datetime.utcnow().strftime("%Y%m%d")
    temp_parquet = f"temp_esc_esl_{today_str}.parquet"
    df.to_parquet(temp_parquet, index=False)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Menimpa staging lama
    )

    with open(temp_parquet, "rb") as source_file:
        job = client.load_table_from_file(source_file, RAW_TABLE_ID, job_config=job_config)
    
    job.result()
    print(f"✅ Berhasil memuat {job.output_rows} baris ke tabel Staging.")
    
    if os.path.exists(temp_parquet):
        os.remove(temp_parquet)
    return True

# def transform_bq(client):
#     query = f"""
#         CREATE OR REPLACE TABLE `{PROD_TABLE_ID}` AS
#         SELECT *
#         FROM `{PROD_TABLE_ID}`
#         WHERE DATE(transaction_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH))

#         UNION ALL

#         -- Menggunakan EXCEPT untuk membuang kolom internal, dan REPLACE untuk mengubah tipe project menjadi FLOAT64
#         SELECT * EXCEPT(_company, _year, _month, _extracted_at)
#             REPLACE(SAFE_CAST(project AS FLOAT64) AS project)
#         FROM `{RAW_TABLE_ID}`
#         WHERE DATE(transaction_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)

#         ORDER BY transaction_date DESC
#     """
#     print(f"⏳ Menjalankan transformasi SQL Incremental ke {PROD_TABLE_ID}...")
#     query_job = client.query(query)
#     query_job.result()
#     print("🎉 Transformasi sukses! Tabel fact_transaction_esc_esl telah diperbarui dengan aman.")

if __name__ == "__main__":
    bq_client = bigquery.Client()
    
    # Alur Eksekusi Pipeline: Extract -> Load Staging -> Transform Prod
    extracted_data = extract_erp_dynamic()
    load_data_to_staging(extracted_data, bq_client)
    #if load_data_to_staging(extracted_data, bq_client):
        #transform_bq(bq_client)
