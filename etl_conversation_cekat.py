import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# =========================
# 1. KONFIGURASI
# =========================
API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai"

# Separasi Layer Data Warehouse (Staging vs Production)
STAGING_TABLE_ID = "euromedica-495509.raw.staging_conversation"
PROD_TABLE_ID = "euromedica-495509.database.conversation"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

LIMIT = 100

if not API_KEY:
    raise ValueError("❌ Eror: Variabel lingkungan CEKAT_API_KEY tidak ditemukan atau kosong!")

# =========================
# 2. FUNCTIONS
# =========================

def extract_and_clean_conversations():
    all_data = []
    request_count = 1  
    
    today_dt = datetime.utcnow()
    end_date = today_dt.strftime('%Y-%m-%d')                             
    start_date = (today_dt - timedelta(days=3)).strftime('%Y-%m-%d')  
    
    params = {
        "limit": LIMIT,
        "start_date": start_date,
        "end_date": end_date
    }
    
    url = f"{BASE_URL}/api/conversations"
    print("==================================================")
    print(f"🔄 Memulai penarikan data dari {start_date} s.d {end_date}")
    print("==================================================")

    while True:
        print(f"\n🚀 Fetching batch ke-{request_count}...")
        
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if response.status_code != 200:
                print(f"❌ API Bermasalah (Status: {response.status_code}). Menghentikan loop.")
                break
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            break
            
        result = response.json()
        data = result.get("data", [])

        if not data:
            print("✅ No more data dari API. Loop selesai.")
            break

        print(f"📄 Batch {request_count}: Berhasil mendapatkan {len(data)} baris data.")
        all_data.extend(data)

        if len(data) < LIMIT:
            print("✅ Halaman terakhir dari API tercapai. Loop selesai.")
            break

        metadata = result.get("metadata", {})
        next_cursor = metadata.get("next_cursor", {}) if metadata else {}
        
        cursor_id = next_cursor.get("cursor_id") if next_cursor else None
        cursor_ts = next_cursor.get("cursor_ts") if next_cursor else None

        if not cursor_id or not cursor_ts:
            print("✅ Metadata menyatakan tidak ada cursor lagi. Selesai.")
            break

        cursor_dt = pd.to_datetime(cursor_ts)
        if cursor_dt.tz is None:
            cursor_dt = cursor_dt.tz_localize('UTC')
        cursor_wib = cursor_dt.tz_convert('Asia/Jakarta')
        cursor_date_str = cursor_wib.strftime('%Y-%m-%d')

        print(f"📌 Posisi Bookmark API Saat Ini -> {cursor_wib.strftime('%Y-%m-%d %H:%M:%S')} WIB")

        if cursor_date_str < start_date:
            print(f"🛑 [REM] Waktu data terakhir ({cursor_date_str}) sudah melewati batas awal {start_date}.")
            break

        params["cursor_id"] = cursor_id
        params["cursor_ts"] = cursor_ts

        request_count += 1
        time.sleep(2)

    if not all_data:
        print(f"❌ Tidak ada data yang berhasil ditarik untuk rentang {start_date} s.d {end_date}")
        return None

    print("\n==================================================")
    print("📊 PROSES PENYIMPANAN DATA")
    print("==================================================")
    
    df = pd.DataFrame(all_data)
    df['created_at_wib'] = pd.to_datetime(df['created_at']).dt.tz_convert('Asia/Jakarta')
    df['tanggal_wib'] = df['created_at_wib'].dt.strftime('%Y-%m-%d')

    df = df[(df['tanggal_wib'] >= start_date) & (df['tanggal_wib'] <= end_date)]
    print(f"Total baris masuk rentang {start_date} s.d {end_date}: {len(df)}")

    if df.empty:
        print("⚠️ Data kosong setelah difilter berdasarkan rentang tanggal.")
        return None

    if 'labels' in df.columns:
        df['labels'] = df['labels'].apply(
            lambda x: ", ".join([label.get('name', '') for label in x]) if isinstance(x, list) else ""
        )

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
        df[col] = df[col].astype(str).replace(["nan", "None", "<NA>"], "")

    print(f"Total baris unik siap disimpan: {len(df)}")

    df = df.drop(columns=['tanggal_wib'])
    df['created_at_wib'] = df['created_at_wib'].dt.strftime('%Y-%m-%d %H:%M:%S')

    execution_date_str = today_dt.strftime('%Y%m%d')
    file_path = f"cekat_conversations_{execution_date_str}.parquet"
    df.to_parquet(file_path, index=False)
    
    print(f"✅ Cleaned data saved ke Parquet: {len(df)} rows")
    return file_path


def load_to_staging(file_path, client):
    """Memuat snapshot data 3 hari terakhir ke tabel transit (Staging)"""
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return False

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Timpa staging lama dengan snapshot baru
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, STAGING_TABLE_ID, job_config=job_config)
    
    job.result()
    print(f"🚀 Data sukses dimuat ke Staging ({STAGING_TABLE_ID}): {job.output_rows} rows")
    return True


def upsert_to_production(client):
    """Menjalankan logika UPSERT murni berbasis ID ke tabel produksi"""
    query = f"""
        -- 1. Hapus data lama di tabel produksi yang ID-nya match dengan data baru/update di staging
        DELETE FROM `{PROD_TABLE_ID}`
        WHERE id IN (SELECT id FROM `{STAGING_TABLE_ID}`);

        -- 2. Masukkan semua data baru/update dari staging ke tabel produksi
        INSERT INTO `{PROD_TABLE_ID}`
        SELECT * FROM `{STAGING_TABLE_ID}`;
    """
    print(f"⏳ Menjalankan SQL UPSERT (Merge) ke tabel produksi {PROD_TABLE_ID}...")
    query_job = client.query(query)
    query_job.result() # Tunggu hingga transaksi SQL selesai
    print("🎉 SUCCESS! Data percakapan berhasil di-UPSERT (Update & Insert) berdasarkan ID.")


# =========================
# 3. MAIN EXECUTION
# =========================
if __name__ == "__main__":
    bq_client = bigquery.Client()
    
    saved_parquet = extract_and_clean_conversations()
    if saved_parquet:
        # Jalankan berurutan: Load Staging -> Jalankan Query UPSERT ke Prod
        if load_to_staging(saved_parquet, bq_client):
            upsert_to_production(bq_client)
