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

# Cukup daftarkan nama board-nya saja di sini
TARGET_BOARDS = [
    "Tracker SKIN+ New",
    "Tracker SLIM+ New"
]

# =========================
# 2. FUNCTIONS
# =========================

def get_board_mapping():
    """Mengambil semua daftar board dari API untuk mendapatkan mapping Name -> ID"""
    print("🔄 Mengambil daftar seluruh board dari Cekat AI...")
    url = f"{BASE_URL}/api/crm/boards"
    
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Gagal mengambil daftar board: {response.text}")
            return {}
        
        # Asumsi response structure standar: {"data": [{"id": "...", "name": "..."}, ...]}
        boards_data = response.json().get("data", [])
        
        # Buat dictionary mapping { "Nama Board": "ID Board" }
        mapping = {board["name"]: board["id"] for board in boards_data if "name" in board and "id" in board}
        return mapping
    except Exception as e:
        print(f"❌ Error saat fetch board mapping: {e}")
        return {}

def extract_cekat_raw():
    all_data = []
    
    # 1. Dapatkan mapping ID secara dinamis berdasarkan nama
    board_mapping = get_board_mapping()
    if not board_mapping:
        print("❌ Tidak ada mapping board yang ditemukan. Proses dihentikan.")
        return None
        
    execution_date = datetime.utcnow().strftime('%Y%m%d')
    
    # 2. Iterasi berdasarkan list nama yang ditargetkan
    for board_name in TARGET_BOARDS:
        board_id = board_mapping.get(board_name)
        
        if not board_id:
            print(f"⚠️ Board '{board_name}' tidak ditemukan di sistem Cekat AI. Skipping...")
            continue
            
        print(f"🚀 Memulai extraction untuk board: {board_name} (ID: {board_id})")
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
    client = bigquery.Client()
    
    saved_file = extract_cekat_raw()
    if saved_file:
        load_to_bq(saved_file, client)
