import os
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# =========================
# 1. KONFIGURASI
# =========================
API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai/api/messages"
MAIN_TABLE_ID = "euromedica-495509.raw.ads_cekat"
STAGING_TABLE_ID = "euromedica-495509.raw.ads_cekat_staging"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

if not API_KEY:
    raise ValueError("❌ Eror: Variabel lingkungan CEKAT_API_KEY tidak ditemukan atau kosong!")

# =========================
# 2. FUNCTIONS
# =========================

def extract_messages_cekat():
    all_data = []
    limit_per_page = 100
    current_page = 1
    
    # Mengganti logical_date Airflow dengan datetime UTC harian murni
    today_dt = datetime.utcnow()
    start_date_str = (today_dt - timedelta(days=2)).strftime('%Y-%m-%d')
    end_date_str = today_dt.strftime('%Y-%m-%d')
    
    print(f"📌 Menarik data Ads dengan rentang fixed rolling: {start_date_str} s/d {end_date_str}")

    while True:
        params = {
            'page': current_page, 
            'is_ads': 'true', 
            'limit': limit_per_page,
            'start_date': start_date_str, 
            'end_date': end_date_str
        }
        
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=60)
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error ke API Cekat: {e}")
            break

        if response.status_code == 200:
            items = response.json().get('data', [])
            if not items:
                print("✅ Semua halaman data berhasil ditarik.")
                break
                
            # Stringify object/array agar tidak merusak skema Parquet/BigQuery
            for row in items:
                for key, value in row.items():
                    if isinstance(value, (list, dict)):
                        row[key] = json.dumps(value)
                        
            all_data.extend(items)
            print(f"📄 Page {current_page}: Berhasil menarik {len(items)} baris data.")
            
            if len(items) < limit_per_page:
                print("✅ Halaman terakhir tercapai.")
                break
                
            current_page += 1
            time.sleep(7)
            
        elif response.status_code == 429:
            print("⏳ Terkena rate limit (429). Sleep 60 detik...")
            time.sleep(60)
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            break

    if not all_data:
        print("ℹ️ Tidak ada data baru untuk ditarik.")
        return None

    df = pd.DataFrame(all_data)
    
    # Deduplikasi internal hasil API berdasarkan ID sebelum masuk staging
    for col in ['id', 'message_id']:
        if col in df.columns:
            df = df.drop_duplicates(subset=[col])
            break

    # Simpan ke file parquet lokal runner
    execution_date_str = today_dt.strftime('%Y%m%d')
    file_path = f"ads_cekat_new_{execution_date_str}.parquet"
    df.to_parquet(file_path, index=False)
    print(f"✅ Cleaned data saved ke Parquet lokal: {len(df)} rows")
    return file_path


def load_staging_and_upsert(file_path, client):
    if not file_path or not os.path.exists(file_path):
        print("ℹ️ Skip proses load karena file parquet kosong atau tidak ditemukan.")
        return

    # 1. Load data snapshot ke Staging Table (WRITE_TRUNCATE)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, STAGING_TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"📥 Berhasil menulis data baru ke staging: {STAGING_TABLE_ID}")

    # Check apakah tabel utama sudah ada
    try:
        client.get_table(MAIN_TABLE_ID)
        table_exists = True
    except NotFound:
        table_exists = False

    if not table_exists:
        print(f"🗂️ Tabel utama `{MAIN_TABLE_ID}` belum ada. Membuat tabel baru dari staging...")
        create_query = f"CREATE TABLE `{MAIN_TABLE_ID}` AS SELECT * FROM `{STAGING_TABLE_ID}`"
        client.query(create_query).result()
        print(f"🎉 Sukses membuat tabel utama awal.")
    else:
# 2. FIX LOGIC MERGE: Menggunakan pola Delete-then-Insert murni berbasis ID
        upsert_query = f"""
            -- Hapus data lama di tabel utama yang ID-nya masuk dalam update terbaru di staging
            DELETE FROM `{MAIN_TABLE_ID}`
            WHERE id IN (SELECT id FROM `{STAGING_TABLE_ID}`);

            -- Masukkan seluruh data baru/ter-update dari staging ke tabel utama
            -- Menggunakan REPLACE agar tipe data ads_data diubah ke STRING tanpa merubah urutan kolom
            INSERT INTO `{MAIN_TABLE_ID}`
            SELECT * REPLACE (SAFE_CAST(interactive AS INT64) AS interactive, CAST(ads_data AS STRING) AS ads_data, CAST(NULL AS INT64) AS location)
            FROM `{STAGING_TABLE_ID}`;
        """
        client.query(upsert_query).result()
        print(f"🚀 Data Upserted successfully into `{MAIN_TABLE_ID}`.")

    # Bersihkan file lokal runner
    if os.path.exists(file_path):
        os.remove(file_path)

# =========================
# 3. MAIN EXECUTION
# =========================
if __name__ == "__main__":
    bq_client = bigquery.Client()
    saved_parquet = extract_messages_cekat()
    if saved_parquet:
        load_staging_and_upsert(saved_parquet, bq_client)
