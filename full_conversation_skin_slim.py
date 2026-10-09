
from datetime import datetime, timezone
import json
import os
import time
import uuid

import pandas as pd
import requests
from google.cloud import bigquery


# =========================================================
# 1. KONFIGURASI
# =========================================================

API_KEY = os.getenv("CEKAT_API_KEY")

BASE_URL = "https://api.cekat.ai/api/messages"

START_DATE = "2026-10-05"
END_DATE = "2026-10-07"

PROJECT_ID = "euromedica-495509"
DATASET_ID = "database"

PROD_TABLE_ID = (
    f"{PROJECT_ID}.{DATASET_ID}.full_conversation_skin_slim"
)

LIMIT_PER_PAGE = 300

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
    "ads_data"
]


# =========================================================
# 2. HELPER: KONVERSI DATA KE STRING
# =========================================================

def convert_to_string(value):
    if value is None:
        return None

    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)

    if pd.isna(value):
        return None

    return str(value)


# =========================================================
# 3. FETCH DATA DARI CEKAT API
# =========================================================

def fetch_messages():

    if not API_KEY:
        raise ValueError(
            "CEKAT_API_KEY belum tersedia di environment."
        )

    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    all_messages = []
    current_page = 1

    print(
        f"📌 Menarik data pesan dari "
        f"{START_DATE} s/d {END_DATE}"
    )

    with requests.Session() as session:

        while True:

            params = {
                "start_date": START_DATE,
                "end_date": END_DATE,
                "page": current_page,
                "limit": LIMIT_PER_PAGE
            }

            try:
                response = session.get(
                    BASE_URL,
                    headers=headers,
                    params=params,
                    timeout=30
                )

                response.raise_for_status()

                res_json = response.json()
                items = res_json.get("data", [])

                if not isinstance(items, list):
                    raise ValueError(
                        "Format data API tidak sesuai."
                    )

                if not items:
                    print("✅ Semua halaman berhasil ditarik.")
                    break

                for item in items:

                    contact_info = item.get("contact")

                    if isinstance(contact_info, dict):
                        item["phone_number"] = (
                            contact_info.get("phone_number")
                        )
                    else:
                        item["phone_number"] = None

                all_messages.extend(items)

                print(
                    f"📄 Page {current_page}: "
                    f"{len(items)} baris berhasil ditarik."
                )

                if len(items) < LIMIT_PER_PAGE:
                    print("✅ Halaman terakhir tercapai.")
                    break

                current_page += 1
                time.sleep(1.5)

            except Exception as e:
                raise RuntimeError(
                    f"Gagal mengambil data pada "
                    f"page {current_page}: {e}"
                ) from e

    return all_messages


# =========================================================
# 4. TRANSFORMASI DAN DEDUPLIKASI DATA
# =========================================================

def prepare_dataframe(all_messages):

    df = pd.DataFrame(all_messages)

    if df.empty:
        return pd.DataFrame(columns=COLUMNS)

    # Pilih kolom sesuai schema BigQuery
    df_upload = df.reindex(columns=COLUMNS).copy()

    # Konversi seluruh kolom ke STRING
    for col in COLUMNS:
        df_upload[col] = df_upload[col].apply(
            convert_to_string
        )

    # Validasi ID pesan
    invalid_ids = (
        df_upload["id"].isna()
        | df_upload["id"].fillna("").str.strip().eq("")
    )

    if invalid_ids.any():
        raise ValueError(
            f"Ditemukan {invalid_ids.sum()} pesan "
            f"tanpa ID yang valid."
        )

    total_before = len(df_upload)

    # Konversi updated_at untuk menentukan versi terbaru
    df_upload["_updated_ts"] = pd.to_datetime(
        df_upload["updated_at"],
        errors="coerce",
        utc=True
    )

    # Urutkan berdasarkan updated_at
    df_upload = df_upload.sort_values(
        by="_updated_ts",
        ascending=True,
        na_position="first",
        kind="stable"
    )

    # Pertahankan satu pesan per ID (versi terbaru)
    df_upload = df_upload.drop_duplicates(
        subset=["id"],
        keep="last"
    )

    # Hapus kolom bantu
    df_upload = df_upload.drop(
        columns=["_updated_ts"]
    )

    print(f"📊 Total data API: {total_before}")
    print(f"📊 Data unik: {len(df_upload)}")
    print(
        f"📊 Duplikat dihapus: "
        f"{total_before - len(df_upload)}"
    )

    return df_upload


# =========================================================
# 5. UPLOAD KE STAGING DAN MERGE KE PRODUKSI
# =========================================================

def upload_to_bigquery(df_upload):

    if df_upload.empty:
        print("⚠️ Tidak ada data untuk diupload.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    dataset = client.get_dataset(
        f"{PROJECT_ID}.{DATASET_ID}"
    )

    location = dataset.location

    # Nama staging unik untuk setiap eksekusi
    staging_name = (
        f"_staging_full_conversation_skin_slim_"
        f"{uuid.uuid4().hex[:12]}"
    )

    STAGING_TABLE_ID = (
        f"{PROJECT_ID}.{DATASET_ID}.{staging_name}"
    )

    # Schema staging
    schema = [
        bigquery.SchemaField(col, "STRING")
        for col in COLUMNS
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    try:

        # -------------------------------------------------
        # A. UPLOAD DATA KE STAGING
        # -------------------------------------------------

        print("📤 Upload data ke staging...")

        load_job = client.load_table_from_dataframe(
            df_upload,
            STAGING_TABLE_ID,
            job_config=job_config,
            location=location
        )

        load_job.result()

        print(
            f"✅ {load_job.output_rows} baris "
            f"berhasil masuk staging."
        )

        # -------------------------------------------------
        # B. GENERATE QUERY MERGE
        # -------------------------------------------------

        update_columns = [
            col for col in COLUMNS
            if col != "id"
        ]

        update_set = ",\n        ".join(
            f"T.`{col}` = S.`{col}`"
            for col in update_columns
        )

        insert_columns = ", ".join(
            f"`{col}`"
            for col in COLUMNS
        )

        insert_values = ", ".join(
            f"S.`{col}`"
            for col in COLUMNS
        )

        # -------------------------------------------------
        # C. MERGE KE TABEL PRODUKSI
        # -------------------------------------------------

        merge_query = f"""
        MERGE `{PROD_TABLE_ID}` AS T
        USING `{STAGING_TABLE_ID}` AS S

        ON T.id = S.id

        WHEN MATCHED AND (
            SAFE_CAST(S.updated_at AS TIMESTAMP)
                > SAFE_CAST(T.updated_at AS TIMESTAMP)

            OR (
                SAFE_CAST(T.updated_at AS TIMESTAMP) IS NULL
                AND
                SAFE_CAST(S.updated_at AS TIMESTAMP) IS NOT NULL
            )
        )
        THEN UPDATE SET
            {update_set}

        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        print("🔄 Menjalankan MERGE ke tabel produksi...")

        merge_job = client.query(
            merge_query,
            location=location
        )

        merge_job.result()

        print("✅ MERGE berhasil dijalankan.")
        print(
            f"📊 Total baris terdampak: "
            f"{merge_job.num_dml_affected_rows}"
        )

    finally:

        # -------------------------------------------------
        # D. HAPUS STAGING
        # -------------------------------------------------

        client.delete_table(
            STAGING_TABLE_ID,
            not_found_ok=True
        )

        print("🗑️ Staging table telah dibersihkan.")


# =========================================================
# 6. MAIN EXECUTION
# =========================================================

def main():

    print("🚀 Memulai ETL Full Conversation SKIN SLIM")

    all_messages = fetch_messages()

    df_upload = prepare_dataframe(all_messages)

    upload_to_bigquery(df_upload)

    print("🎉 ETL selesai!")


if __name__ == "__main__":
    main()
