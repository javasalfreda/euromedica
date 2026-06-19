import os
import time
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import bigquery

# =========================
# 1. KONFIGURASI
# =========================
API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai"
TABLE_ID = "euromedica-495509.raw.boards_calendar"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

EXCLUDE_BOARDS = [
    'Leads', 'Sempvrna', 'Sempvrna Plaza Kalibata',
    'Tracker SKIN+', 'Tracker SLIM+', 'Tracker SKIN+ New', 'Tracker SLIM+ New'
]

# =========================
# 2. FUNCTIONS
# =========================

def extract_and_clean_cekat():
    all_data = []
    execution_date = datetime.utcnow().strftime('%Y%m%d')
    
    # --- A. Ambil List Boards ---
    boards_url = f"{BASE_URL}/api/crm/boards"
    res_boards = requests.get(boards_url, headers=HEADERS)
    res_boards.raise_for_status()
    boards_list = res_boards.json().get("data", res_boards.json())

    # --- B. Loop Items per Board ---
    for board in boards_list:
        board_id = board.get("id")
        board_name = board.get("name")
        
        if board_name in EXCLUDE_BOARDS:
            continue

        page = 1
        while True:
            url = f"{BASE_URL}/api/crm/boards/{board_id}/items?limit=5000&page={page}"
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200: break
            
            data = response.json().get("data", [])
            if not data: break

            for row in data:
                row["source_board_id"] = board_id
                row["board_name"] = board_name
            
            all_data.extend(data)
            if len(data) < 5000: break
            page += 1
            time.sleep(1)

    if not all_data:
        print("❌ No data fetched from Cekat AI")
        return None

    # --- C. Data Cleaning ---
    df = pd.DataFrame(all_data)
    if "item_id" in df.columns:
        df = df.drop_duplicates(subset=["item_id"])

    # Convert dates
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.date
    df['booking_date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

    # Clean Collection
    df['raw_collection'] = df['Collection'].astype(str).str.replace(',', '', regex=False)
    df['raw_collection'] = pd.to_numeric(df['raw_collection'], errors='coerce')

    # Normalisasi Rupiah
    df['collection'] = np.where(
        df['raw_collection'].isna(), 0,
        np.where(
            (df['raw_collection'] >= 0) & (df['raw_collection'] <= 10000),
            df['raw_collection'] * 1000,
            df['raw_collection']
        )
    ).astype('int64')

    # --- D. Dynamic Mapping Visit & Conversion ---
    def get_dynamic_label(row, value_col, options_col):
        val_list = row.get(value_col)
        opt_list = row.get(options_col)
        
        if isinstance(val_list, list) and len(val_list) > 0 and isinstance(opt_list, list):
            target_id = str(val_list[0])
            for opt in opt_list:
                if isinstance(opt, dict) and str(opt.get('id')) == target_id:
                    return opt.get('name')
        return None

    if 'New Visit' in df.columns and 'New Visit Label Options' in df.columns:
        df['visit'] = df.apply(lambda r: get_dynamic_label(r, 'New Visit', 'New Visit Label Options'), axis=1)
    else:
        df['visit'] = None

    if 'Conversion Status' in df.columns and 'Conversion Status Label Options' in df.columns:
        df['conv_status'] = df.apply(lambda r: get_dynamic_label(r, 'Conversion Status', 'Conversion Status Label Options'), axis=1)
    else:
        df['conv_status'] = None

    df['kode_qris'] = df.get('Kode Voucher QRIS', None)

    # --- E. Aggregation ---
    df_final = (
        df.groupby([
            'created_at', 'booking_date', 'Phone', 
            'board_name', 'visit', 'conv_status', 'kode_qris'
        ], dropna=False)
        .agg({'collection': 'sum'})
        .reset_index()
    )

    # --- F. Simpan Parquet lokal ---
    file_path = f"cekat_calendar_{execution_date}.parquet"
    df_final.to_parquet(file_path, index=False)
    
    print(f"✅ Cleaned data saved: {len(df_final)} rows")
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
    print(f"🚀 Data Cekat Loaded to BQ: {job.output_rows} rows")

# =========================
# 3. MAIN EXECUTION
# =========================
if __name__ == "__main__":
    client = bigquery.Client()
    
    saved_file = extract_and_clean_cekat()
    if saved_file:
        load_to_bq(saved_file, client)
