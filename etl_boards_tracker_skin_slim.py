import os
import time
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# =========================
# 1. KONFIGURASI
# =========================
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJidXNpbmVzc19pZCI6ImFjNGQ3OTQ1LThkZmUtNGI1NS04MzE4LTEyZjY5ZGE3MzAzYiIsImVtYWlsIjoiaXQuYWRtaW5pc3RyYXRvckBldXJvbWVkaWNhZ3JvdXAuY29tIiwiYnVzaW5lc3NfbmFtZSI6IkV1cm9tZWRpY2FnrumVjY2Ex... [JWT KEY ANDA]"
BASE_URL = "https://api.cekat.ai"
TABLE_ID = "euromedica-495509.raw.boards_tracker_skin_slim"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

TARGET_BOARDS = [
    {"id": "7df005f0-793b-43ea-8ca6-1192726fa4d1", "name": "Tracker SKIN+ New"},
    {"id": "c607fdb8-cb9c-430f-9084-632666b97b07", "name": "Tracker SLIM+ New"}
]

# =========================
# 2. FUNCTIONS
# =========================

def extract_cekat_raw():
    all_data = []
    
    # Menggantikan context['ds_nodash'] bawaan Airflow dengan date UTC harian
    execution_date = datetime.utcnow().strftime('%Y%m%d')
    
    for board in TARGET_BOARDS:
        board_id = board["id"]
        board_name = board["name"]
        
        print(f"🚀 Memulai extraction untuk board: {board_name}")
        page = 1
        
        while True:
            url = f"{BASE_URL}/api/crm/boards/{board_id}/items?limit=5000&page={page}"
            response = requests.get(url, headers=HEADERS)
            
            if response.status_code != 200:
                print(f"❌ Error pada {board_name} hal {page}: {response.text}")
                break
            
            data = response.json().get("data", [])
            if not data:
                break

            for row in data:
                row["source_board_id"] = board_id
                row["board_name"] = board_name
                
                for key, value in row.items():
                    if isinstance(value, (list, dict)):
                        row[key] = str(value)
            
            all_data.extend(data)
            print(f"✅ {board_name} - Page {page} berhasil ditarik ({len(data)} rows)")

            if len(data) < 5000:
                break
            
            page += 1
            time.sleep(1)

    if not all_data:
        print("❌ No data fetched from Cekat AI")
        return None

    df = pd.DataFrame(all_data)
    
    if "item_id" in df.columns:
        df = df.drop_duplicates(subset=["item_id"])

    # Di GitHub Actions, file disimpan langsung di working directory runner saat ini
    file_path = f"cekat_raw_tracker_{execution_date}.parquet"
    df.to_parquet(file_path, index=False)
    
    print(f"✅ Raw data saved: {len(df)} rows")
    return file_path

def load_to_bq(file_path, client):
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, TABLE_ID, job_config=job_config)
    
    job.result()
    print(f"🚀 Data Loaded to BQ: {job.output_rows} rows")

# =========================
# 3. MAIN EXECUTION
# =========================
if __name__ == "__main__":
    # Inisialisasi Google BigQuery Client murni
    client = bigquery.Client()
    
    # Jalankan alur secara sekuensial (Mengganti operator >> Airflow)
    saved_file = extract_cekat_raw()
    if saved_file:
        load_to_bq(saved_file, client)
