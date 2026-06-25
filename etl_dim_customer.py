import os
import json
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# =========================
# 1. KONFIGURASI API & TARGET
# =========================
BASE_URL = "https://erpeuromedicagroup.com"
LOGIN_URL = f"{BASE_URL}/api/method/login"
DATA_URL = f"{BASE_URL}/api/resource/Customer"

ACCOUNTS = [
    {"username": "denny.asarias.ehl@euromedicagroup.com", "password": "260628", "brand": "EUROHAIRLAB"},
    {"username": "denny.asarias.esc@euromedicagroup.com", "password": "260628", "brand": "ESC"},
    {"username": "denny.asarias.esl@euromedicagroup.com", "password": "260628", "brand": "ESL"},
    {"username": "denny.asarias.dei@skinplusclinic.com", "password": "260628", "brand": "SKIN SLIM"}
]

TARGET_FIELDS = [
    "name", "creation", "modified", "modified_by", "owner",
    "customer_name", "gender", "customer_type", "default_bank_account",
    "customer_group", "territory", "disabled", "is_frozen",
    "id_no", "date_of_birth", "registration_territory",
    "how_did_you_come_to_know_about_us", "contact_no",
    "country", "province", "city", "district", "alamat",
    "join_date", "customer_number", "goapp_account_id"
]

TABLE_ID = "euromedica-495509.database.dim_customer"

# =========================
# 2. ETL FUNCTIONS
# =========================

def fetch_and_save_parquet():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    final_data = []
    # Mengganti context['ds_nodash'] Airflow dengan tanggal UTC harian murni
    execution_date = datetime.utcnow().strftime('%Y%m%d')
    
    for acc in ACCOUNTS:
        brand = acc["brand"]
        print(f"🔑 Login sebagai {acc['username']} ({brand})")
        
        login_res = session.post(LOGIN_URL, data={"usr": acc["username"], "pwd": acc["password"]})
        if login_res.status_code != 200:
            print(f"❌ Login gagal ({brand})")
            continue

        limit_start = 0
        limit_page = 1000

        while True:
            print(f"⏳ [{brand}] Ambil data {limit_start}...")
            params = {
                "fields": json.dumps(TARGET_FIELDS),
                "limit_start": limit_start,
                "limit_page_length": limit_page
            }
            
            res = session.get(DATA_URL, params=params, timeout=60)
            data_batch = res.json().get("data", [])
            
            if not data_batch:
                break

            for row in data_batch:
                row["brand"] = brand
            
            final_data.extend(data_batch)
            if len(data_batch) < limit_page:
                break
            limit_start += limit_page

    if final_data:
        df = pd.DataFrame(final_data)
        
        # Disimpan langsung di working directory GitHub Runner saat ini
        file_path = f"dim_customer_{execution_date}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"✅ Berhasil menarik total {len(df)} customer.")
        return file_path
    else:
        print("❌ Tidak ada data yang berhasil ditarik.")
        return None

def load_customer_to_bq(file_path, client):
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Create or Replace
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, TABLE_ID, job_config=job_config)
    
    job.result()
    print(f"🚀 Master Customer berhasil di-replace ke BigQuery. Total: {job.output_rows} baris.")

# =========================
# 3. MAIN EXECUTION
# =========================
if __name__ == "__main__":
    client = bigquery.Client()
    
    saved_file = fetch_and_save_parquet()
    if saved_file:
        load_customer_to_bq(saved_file, client)
