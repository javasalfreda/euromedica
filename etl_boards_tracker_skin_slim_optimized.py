import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional

API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai"
TABLE_ID = "euromedica-495509.raw.boards_tracker_skin_slim_test"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

BOARD_CONFIG = {
    "Tracker SKIN+ New": "7df005f0-793b-43ea-8ca6-1192726fa4d1",
    "Tracker SLIM+ New": "c607fdb8-cb9c-430f-9084-632666b97b07"
}

TARGET_BOARDS = [
    "Tracker SKIN+ New",
    "Tracker SLIM+ New"
]

MAX_WORKERS_PAGES = 5
MAX_WORKERS_BOARDS = 2
SLEEP_BETWEEN_REQUESTS = 0.1
RETRY_ATTEMPTS = 3

def fetch_single_page(board_id: str, board_name: str, page: int) -> Tuple[int, List[Dict], bool]:
    url = f"{BASE_URL}/api/crm/boards/{board_id}/items?limit=1000&page={page}"
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            
            if response.status_code != 200:
                print(f"❌ Error pada {board_name} page {page} (attempt {attempt+1}): {response.status_code}")
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(2 ** attempt)
                    continue
                return page, [], False
            
            data = response.json().get("data", [])
            
            for row in data:
                row["source_board_id"] = board_id
                row["board_name"] = board_name
            
            is_last_page = len(data) == 0
            return page, data, is_last_page
            
        except Exception as e:
            print(f"⚠️ Exception pada {board_name} page {page} (attempt {attempt+1}): {str(e)}")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2 ** attempt)
                continue
            return page, [], False
    
    return page, [], False

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    return df

def fetch_board_parallel(board_name: str, board_id: str) -> List[Dict]:
    print(f"🚀 Memulai extraction untuk board: {board_name}")
    start_time = time.time()
    
    all_data = []
    page = 1
    
    first_page, first_data, is_empty = fetch_single_page(board_id, board_name, page)
    
    if is_empty or not first_data:
        print(f"✅ {board_name} - Tidak ada data")
        return []
    
    all_data.extend(first_data)
    print(f"✅ {board_name} - Page {page} berhasil ditarik ({len(first_data)} rows)")
    
    page = 2
    batch_size = 10
    
    while True:
        page_batch = list(range(page, page + batch_size))
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_PAGES) as executor:
            futures = {
                executor.submit(fetch_single_page, board_id, board_name, p): p 
                for p in page_batch
            }
            
            batch_results = []
            for future in as_completed(futures):
                page_num, data, is_last = future.result()
                batch_results.append((page_num, data, is_last))
            
            batch_results.sort(key=lambda x: x[0])
            
            found_last_page = False
            for page_num, data, is_last in batch_results:
                if data:
                    all_data.extend(data)
                    print(f"✅ {board_name} - Page {page_num} berhasil ditarik ({len(data)} rows)")
                
                if is_last or not data:
                    found_last_page = True
                    break
            
            if found_last_page:
                break
            
            page += batch_size
        
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    
    elapsed = time.time() - start_time
    print(f"⏱️  {board_name} selesai dalam {elapsed:.2f} detik - Total {len(all_data)} rows")
    
    return all_data

def extract_cekat_raw() -> Optional[str]:
    execution_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    
    print(f"🚀 Memulai extraction paralel untuk {len(TARGET_BOARDS)} boards...")
    start_time = time.time()
    
    all_data = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_BOARDS) as executor:
        futures = {}
        for board_name in TARGET_BOARDS:
            board_id = BOARD_CONFIG.get(board_name)
            if not board_id:
                print(f"⚠️ Board '{board_name}' tidak terdaftar di BOARD_CONFIG. Skipping...")
                continue
            
            futures[executor.submit(fetch_board_parallel, board_name, board_id)] = board_name
        
        for future in as_completed(futures):
            board_name = futures[future]
            try:
                board_data = future.result()
                all_data.extend(board_data)
            except Exception as e:
                print(f"❌ Error processing board {board_name}: {str(e)}")
    
    if not all_data:
        print("❌ No data fetched from Cekat AI")
        return None
    
    print(f"🔄 Membuat DataFrame dari {len(all_data)} rows...")
    df = pd.DataFrame(all_data)
    
    print(f"🔄 Normalisasi data...")
    df = normalize_dataframe(df)
    
    if "item_id" in df.columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["item_id"])
        after_dedup = len(df)
        if before_dedup != after_dedup:
            print(f"🔄 Removed {before_dedup - after_dedup} duplicates")
    
    file_path = f"cekat_raw_tracker_optimized_{execution_date}.parquet"
    df.to_parquet(file_path, index=False)
    
    elapsed = time.time() - start_time
    print(f"✅ Raw data saved ke Parquet: {len(df)} rows dalam {elapsed:.2f} detik ({elapsed/60:.2f} menit)")
    
    return file_path

def load_to_bq(file_path: str, client: bigquery.Client):
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    print(f"🔄 Memulai proses loading {file_path} ke BigQuery...")
    start_time = time.time()
    
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, TABLE_ID, job_config=job_config)
    
    job.result()
    
    elapsed = time.time() - start_time
    print(f"🚀 Data sukses di-load ke BQ: {job.output_rows} rows dalam {elapsed:.2f} detik")

if __name__ == "__main__":
    overall_start = time.time()
    
    bq_client = bigquery.Client()
    
    saved_file = extract_cekat_raw()
    if saved_file:
        load_to_bq(saved_file, bq_client)
    
    overall_elapsed = time.time() - overall_start
    print(f"\n⏱️  TOTAL EXECUTION TIME: {overall_elapsed:.2f} detik ({overall_elapsed/60:.2f} menit)")
