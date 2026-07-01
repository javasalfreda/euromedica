import os
import time
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# =========================
# 1. KONFIGURASI
# =========================
API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai"
TABLE_ID = "euromedica-495509.raw.all_boards_cekat"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

EXCLUDED_BOARDS = [
    "Tracker SKIN+ New", 
    "Tracker SLIM+ New", 
    "Tracker SKIN+", 
    "Tracker SLIM+"
]

# =========================
# 2. FUNCTIONS
# =========================

def get_all_target_boards():
    url = f"{BASE_URL}/api/crm/boards"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        raise Exception(f"❌ Gagal mengambil daftar board dari API: {response.text}")
        
    boards_data = response.json().get("data", [])
    filtered_boards = []
    
    for board in boards_data:
        board_name = board.get("name")
        board_id = board.get("id")
        
        if board_name not in EXCLUDED_BOARDS:
            filtered_boards.append({"id": board_id, "name": board_name})
            
    print(f"📋 Total board ditemukan di API: {len(boards_data)}")
    print(f"🎯 Total board setelah difilter (Target): {len(filtered_boards)}")
    return filtered_boards

def extract_cekat_raw():
    all_data = []
    execution_date = datetime.utcnow().strftime('%Y%m%d')
    
    target_boards = get_all_target_boards()
    
    if not target_boards:
        print("⚠️ Tidak ada board target yang memenuhi kriteria filtrasi.")
        return None

    for board in target_boards:
        board_id = board["id"]
        board_name = board["name"]
        
        print(f"🚀 Memulai extraction untuk board: {board_name} ({board_id})")
        page = 1
        
        while True:
            url = f"{BASE_URL}/api/crm/boards/{board_id}/items?limit=1000&page={page}"
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

            if len(data) <= 0:
                break
            
            page += 1
            time.sleep(1)

    if not all_data:
        print("❌ No data fetched from Cekat AI")
        return None

    df = pd.DataFrame(all_data)
    
    # FIX ERROR PYARROW: Paksa semua kolom bertipe object/mixed menjadi string
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).replace(["nan", "None", "<NA>"], "")

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
