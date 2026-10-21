from datetime import datetime, timedelta, timezone
import json
import os
import time
import uuid
from email.utils import parsedate_to_datetime

import pandas as pd
import requests
from google.cloud import bigquery


# =========================================================
# 1. KONFIGURASI
# =========================================================

API_KEY = os.getenv("CEKAT_API_KEY")
BASE_URL = "https://api.cekat.ai/api/messages"

START_DATE = "2026-09-26"
END_DATE = "2026-09-28"

PROJECT_ID = "euromedica-495509"
DATASET_ID = "database"
PROD_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.full_conversation_skin_slim"

LIMIT_PER_PAGE = 300
REQUEST_TIMEOUT = 90
MAX_RETRIES = 5
RETRY_DELAY = 5
PAGE_SLEEP = 1.5

COLUMNS = [
    "id",
    "conversation_id",
    "created_at",
    "updated_at",
    "phone_number",
    "chat_credits_used",
    "sent_by_name",
    "sent_by_type",
    "message",
    "status",
    "inbox",
    "ads_data",
]


# =========================================================
# 2. HELPER
# =========================================================

def convert_to_string(value):
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    if pd.isna(value):
        return None
    return str(value)


def retry_after_seconds(response, default_delay):
    """Honor Retry-After (seconds or HTTP-date) when available."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return default_delay
    try:
        return max(0, float(raw))
    except ValueError:
        try:
            retry_time = parsedate_to_datetime(raw)
            if retry_time.tzinfo is None:
                retry_time = retry_time.replace(tzinfo=timezone.utc)
            return max(0, (retry_time - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return default_delay


# =========================================================
# 3. FETCH DATA DARI CEKAT API
# =========================================================

def fetch_messages():
    if not API_KEY:
        raise ValueError("CEKAT_API_KEY belum tersedia di environment.")

    headers = {"Authorization": f"Bearer {API_KEY}"}
    all_messages = []
    current_page = 1

    print(f"📌 Menarik data pesan dari {START_DATE} s/d {END_DATE}", flush=True)

    with requests.Session() as session:
        while True:
            params = {
                "start_date": START_DATE,
                "end_date": END_DATE,
                "page": current_page,
                "limit": LIMIT_PER_PAGE,
            }

            items = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    response = session.get(
                        BASE_URL,
                        headers=headers,
                        params=params,
                        timeout=REQUEST_TIMEOUT,
                    )

                    # Retry only for rate limiting and transient server errors.
                    if response.status_code == 429 or 500 <= response.status_code <= 599:
                        if attempt == MAX_RETRIES:
                            response.raise_for_status()
                        wait_time = retry_after_seconds(
                            response, RETRY_DELAY * attempt
                        )
                        print(
                            f"⚠️ Page {current_page}: HTTP {response.status_code} "
                            f"(percobaan {attempt}/{MAX_RETRIES}). "
                            f"Retry dalam {wait_time:.1f} detik...",
                            flush=True,
                        )
                        time.sleep(wait_time)
                        continue

                    # Other HTTP errors (e.g., 400/401/403) fail immediately.
                    response.raise_for_status()
                    res_json = response.json()
                    if not isinstance(res_json, dict) or "data" not in res_json:
                        raise ValueError(
                            f"Page {current_page}: response API tidak memiliki field 'data'."
                        )
                    items = res_json["data"]
                    if not isinstance(items, list):
                        raise ValueError(
                            f"Page {current_page}: field 'data' bukan list."
                        )
                    break

                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                    if attempt == MAX_RETRIES:
                        raise RuntimeError(
                            f"Gagal mengambil page {current_page} setelah "
                            f"{MAX_RETRIES} percobaan: {exc}"
                        ) from exc
                    wait_time = RETRY_DELAY * attempt
                    print(
                        f"⚠️ Page {current_page}: {type(exc).__name__} "
                        f"(percobaan {attempt}/{MAX_RETRIES}). "
                        f"Retry dalam {wait_time} detik...",
                        flush=True,
                    )
                    time.sleep(wait_time)
                except (requests.exceptions.HTTPError, requests.exceptions.JSONDecodeError, ValueError) as exc:
                    raise RuntimeError(
                        f"Gagal mengambil page {current_page}: {exc}. "
                        "ETL dihentikan sebelum upload ke BigQuery."
                    ) from exc

            if items is None:
                raise RuntimeError(f"Page {current_page} tidak menghasilkan respons valid.")

            if not items:
                print("✅ Semua halaman berhasil ditarik.", flush=True)
                break

            for item in items:
                if not isinstance(item, dict):
                    raise ValueError(
                        f"Page {current_page}: ditemukan item yang bukan object."
                    )
                contact_info = item.get("contact")
                item["phone_number"] = (
                    contact_info.get("phone_number")
                    if isinstance(contact_info, dict)
                    else None
                )

            all_messages.extend(items)
            print(
                f"📄 Page {current_page}: {len(items)} baris berhasil ditarik. "
                f"Total sementara: {len(all_messages)}",
                flush=True,
            )

            if len(items) < LIMIT_PER_PAGE:
                print("✅ Halaman terakhir tercapai.", flush=True)
                break

            current_page += 1
            time.sleep(PAGE_SLEEP)

    return all_messages


# =========================================================
# 4. TRANSFORMASI DAN DEDUPLIKASI DATA
# =========================================================

def prepare_dataframe(all_messages):
    df = pd.DataFrame(all_messages)
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)

    df_upload = df.reindex(columns=COLUMNS).copy()
    for col in COLUMNS:
        df_upload[col] = df_upload[col].apply(convert_to_string)

    invalid_ids = (
        df_upload["id"].isna()
        | df_upload["id"].fillna("").str.strip().eq("")
    )
    if invalid_ids.any():
        raise ValueError(f"Ditemukan {invalid_ids.sum()} pesan tanpa ID valid.")

    total_before = len(df_upload)
    df_upload["_updated_ts"] = pd.to_datetime(
        df_upload["updated_at"], errors="coerce", utc=True
    )
    df_upload = (
        df_upload.sort_values(
            by="_updated_ts", ascending=True, na_position="first", kind="stable"
        )
        .drop_duplicates(subset=["id"], keep="last")
        .drop(columns=["_updated_ts"])
        .reset_index(drop=True)
    )

    print(f"📊 Total data API: {total_before}", flush=True)
    print(f"📊 Data unik: {len(df_upload)}", flush=True)
    print(f"📊 Duplikat dihapus: {total_before - len(df_upload)}", flush=True)
    return df_upload


# =========================================================
# 5. UPLOAD KE STAGING DAN MERGE KE PRODUKSI
# =========================================================

def upload_to_bigquery(df_upload):
    if df_upload.empty:
        print("⚠️ Tidak ada data untuk diupload.", flush=True)
        return

    client = bigquery.Client(project=PROJECT_ID)
    dataset = client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
    location = dataset.location

    staging_name = f"_staging_full_conversation_skin_slim_{uuid.uuid4().hex[:12]}"
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{staging_name}"

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(col, "STRING") for col in COLUMNS],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        print("📤 Upload data ke staging...", flush=True)
        load_job = client.load_table_from_dataframe(
            df_upload, staging_table_id, job_config=job_config, location=location
        )
        load_job.result()
        print(f"✅ {load_job.output_rows} baris berhasil masuk staging.", flush=True)

        update_columns = [col for col in COLUMNS if col != "id"]
        update_set = ",\n            ".join(
            f"T.`{col}` = S.`{col}`" for col in update_columns
        )
        insert_columns = ", ".join(f"`{col}`" for col in COLUMNS)
        insert_values = ", ".join(f"S.`{col}`" for col in COLUMNS)

        merge_query = f"""
        MERGE `{PROD_TABLE_ID}` AS T
        USING `{staging_table_id}` AS S
        ON T.id = S.id
        WHEN MATCHED AND (
            SAFE_CAST(S.updated_at AS TIMESTAMP)
                > SAFE_CAST(T.updated_at AS TIMESTAMP)
            OR (
                SAFE_CAST(T.updated_at AS TIMESTAMP) IS NULL
                AND SAFE_CAST(S.updated_at AS TIMESTAMP) IS NOT NULL
            )
        )
        THEN UPDATE SET
            {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        print("🔄 Menjalankan MERGE ke tabel produksi...", flush=True)
        merge_job = client.query(merge_query, location=location)
        merge_job.result()
        print("✅ MERGE berhasil dijalankan.", flush=True)
        print(
            f"📊 Total baris terdampak: {merge_job.num_dml_affected_rows}",
            flush=True,
        )
    finally:
        client.delete_table(staging_table_id, not_found_ok=True)
        print("🗑️ Staging table telah dibersihkan.", flush=True)


# =========================================================
# 6. MAIN EXECUTION
# =========================================================

def main():
    print("🚀 Memulai ETL Full Conversation SKIN SLIM", flush=True)
    all_messages = fetch_messages()
    df_upload = prepare_dataframe(all_messages)
    upload_to_bigquery(df_upload)
    print("🎉 ETL selesai!", flush=True)


if __name__ == "__main__":
    main()
