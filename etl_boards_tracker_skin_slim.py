import os
import time
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# ==============================================================================
# 1. KONFIGURASI
# ==============================================================================
API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai"
TABLE_ID = "euromedica-495509.raw.boards_tracker_skin_slim"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# Mapping Name -> ID untuk menghindari error 500 dari endpoint list boards API
BOARD_CONFIG = {
    "Tracker SKIN+ New": "7df005f0-793b-43ea-8ca6-1192726fa4d1",
    "Tracker SLIM+ New": "c607fdb8-cb9c-430f-9084-632666b97b07"
}

# Tentukan board mana saja yang ingin dieksekusi berdasarkan namanya
TARGET_BOARDS = [
    "Tracker SKIN+ New",
    "Tracker SLIM+ New"
]

# ==============================================================================
# 2. FUNCTIONS
# ==============================================================================

def extract_cekat_raw():
    all_data = []
    
    # Menggantikan context['ds_nodash'] bawaan Airflow dengan date UTC harian
    execution_date = datetime.utcnow().strftime('%Y%m%d')
    
    for board_name in TARGET_BOARDS:
        # Ambil ID secara dinamis dari BOARD_CONFIG berdasarkan nama
        board_id = BOARD_CONFIG.get(board_name)
        
        if not board_id:
            print(f"⚠️ Board '{board_name}' tidak terdaftar di BOARD_CONFIG. Skipping...")
            continue
        
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
                
                # Normalisasi nested data (list/dict) menjadi string agar aman di Parquet/BigQuery
                for key, value in row.items():
                    if isinstance(value, (list, dict)):
                        row[key] = str(value)
            
            all_data.extend(data)
            print(f"✅ {board_name} - Page {page} berhasil ditarik ({len(data)} rows)")

            # Jika data yang ditarik kurang dari limit, berarti sudah halaman terakhir
            if len(data) < 5000:
                break
            
            page += 1
            time.sleep(1)

    if not all_data:
        print("❌ No data fetched from Cekat AI")
        return None

    # Transformasi ke DataFrame & Hapus Duplikat
    df = pd.DataFrame(all_data)
    if "item_id" in df.columns:
        df = df.drop_duplicates(subset=["item_id"])

    # Simpan file di working directory runner GitHub Actions
    file_path = f"cekat_raw_tracker_{execution_date}.parquet"
    df.to_parquet(file_path, index=False)
    
    print(f"✅ Raw data saved ke Parquet: {len(df)} rows")
    return file_path


def load_to_bq(file_path, client):
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return

    # Konfigurasi Load Job (Menggunakan WRITE_TRUNCATE untuk full load harian)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
    )

    print(f"🔄 Memulai proses loading {file_path} ke BigQuery...")
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, TABLE_ID, job_config=job_config)
    
    job.result()  # Menunggu job selesai
    print(f"🚀 Data sukses di-load ke BQ: {job.output_rows} rows")

# ==============================================================================
# 3. MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    # Inisialisasi Google BigQuery Client murni
    bq_client = bigquery.Client()
    
    # Jalankan alur data pipeline secara sekuensial
    saved_file = extract_cekat_raw()
    if saved_file:
        load_to_bq(saved_file, bq_client)
