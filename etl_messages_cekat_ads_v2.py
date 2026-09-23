import os
import time
import json
import random
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
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

REQUEST_TIMEOUT = 60          # detik per request
MAX_RETRIES = 5                # percobaan ulang maksimum untuk error transient
BASE_BACKOFF = 5                # detik, dipakai untuk exponential backoff
RATE_LIMIT_MAX_RETRIES = 8      # batas retry khusus untuk 429, jangan infinite loop
PAGE_SLEEP = 7                   # jeda antar halaman (menghindari rate limit)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("cekat_to_bq")

if not API_KEY:
    raise ValueError("❌ Eror: Variabel lingkungan CEKAT_API_KEY tidak ditemukan atau kosong!")


# =========================
# 2. HELPER: RETRY DENGAN EXPONENTIAL BACKOFF + JITTER
# =========================
def retry_with_backoff(func, *, max_retries=MAX_RETRIES, base_backoff=BASE_BACKOFF,
                        retriable_exceptions=(requests.exceptions.RequestException,),
                        description="operasi"):
    """
    Menjalankan `func` (callable tanpa argumen) dengan retry otomatis
    jika terjadi exception yang termasuk dalam `retriable_exceptions`.
    Menggunakan exponential backoff + jitter agar tidak membebani API/BQ
    saat terjadi error sementara (timeout, connection reset, dsb).
    """
    attempt = 0
    while True:
        try:
            return func()
        except retriable_exceptions as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"❌ Gagal permanen setelah {max_retries} percobaan pada {description}: {e}")
                raise
            wait = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 2)
            logger.warning(
                f"⏳ Error transient pada {description} (percobaan {attempt}/{max_retries}): {e}. "
                f"Retry dalam {wait:.1f} detik..."
            )
            time.sleep(wait)


# =========================
# 3. FUNCTIONS
# =========================

def fetch_page(params):
    """Single HTTP call, dibungkus retry untuk timeout/connection error."""
    def _do_request():
        response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        # Lempar exception khusus untuk 5xx supaya kena retry_with_backoff juga
        if response.status_code >= 500:
            raise requests.exceptions.RequestException(
                f"Server error {response.status_code}: {response.text[:200]}"
            )
        return response

    return retry_with_backoff(
        _do_request,
        retriable_exceptions=(requests.exceptions.RequestException,),
        description=f"GET {BASE_URL} (page={params.get('page')})"
    )


def extract_messages_cekat():
    all_data = []
    limit_per_page = 100
    current_page = 1

    today_dt = datetime.now(timezone.utc)
    # start_date_str = (today_dt - timedelta(days=2)).strftime('%Y-%m-%d')
    # end_date_str = today_dt.strftime('%Y-%m-%d')

    start_date_str = '2026-09-01'
    end_date_str = '2026-09-30'

    logger.info(f"📌 Menarik data Ads dengan rentang fixed rolling: {start_date_str} s/d {end_date_str}")

    rate_limit_attempts = 0

    while True:
        params = {
            'page': current_page,
            'is_ads': 'true',
            'limit': limit_per_page,
            'start_date': start_date_str,
            'end_date': end_date_str
        }

        try:
            response = fetch_page(params)
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request ke API Cekat gagal permanen di page {current_page}: {e}")
            break

        if response.status_code == 200:
            rate_limit_attempts = 0  # reset counter setelah sukses
            try:
                items = response.json().get('data', [])
            except ValueError as e:
                logger.error(f"❌ Response bukan JSON valid di page {current_page}: {e}")
                break

            if not items:
                logger.info("✅ Semua halaman data berhasil ditarik.")
                break

            for row in items:
                for key, value in row.items():
                    if isinstance(value, (list, dict)):
                        row[key] = json.dumps(value)

            all_data.extend(items)
            logger.info(f"📄 Page {current_page}: Berhasil menarik {len(items)} baris data.")

            if len(items) < limit_per_page:
                logger.info("✅ Halaman terakhir tercapai.")
                break

            current_page += 1
            time.sleep(PAGE_SLEEP)

        elif response.status_code == 429:
            rate_limit_attempts += 1
            if rate_limit_attempts > RATE_LIMIT_MAX_RETRIES:
                logger.error(
                    f"❌ Rate limit (429) terus terjadi setelah {RATE_LIMIT_MAX_RETRIES} percobaan. "
                    f"Menghentikan proses untuk mencegah infinite loop."
                )
                break
            wait = 60 * rate_limit_attempts  # backoff bertahap: 60, 120, 180, ...
            logger.warning(f"⏳ Terkena rate limit (429). Percobaan {rate_limit_attempts}/{RATE_LIMIT_MAX_RETRIES}. Sleep {wait} detik...")
            time.sleep(wait)

        else:
            logger.error(f"❌ API Error {response.status_code}: {response.text[:300]}")
            break

    if not all_data:
        logger.info("ℹ️ Tidak ada data baru untuk ditarik.")
        return None

    df = pd.DataFrame(all_data)

    # Deduplikasi lebih menyeluruh: pakai id + message_id sekaligus kalau ada,
    # supaya tidak hanya bergantung pada salah satu kolom.
    dedup_cols = [c for c in ['id', 'message_id'] if c in df.columns]
    if dedup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_cols)
        logger.info(f"🧹 Dedup berdasarkan {dedup_cols}: {before} -> {len(df)} baris")

    execution_date_str = today_dt.strftime('%Y%m%d')
    file_path = f"ads_cekat_new_{execution_date_str}.parquet"
    df.to_parquet(file_path, index=False)
    logger.info(f"✅ Cleaned data saved ke Parquet lokal: {len(df)} rows")
    return file_path


def load_staging_and_upsert(file_path, client):
    if not file_path or not os.path.exists(file_path):
        logger.info("ℹ️ Skip proses load karena file parquet kosong atau tidak ditemukan.")
        return

    try:
        # 1. Load data snapshot ke Staging Table (WRITE_TRUNCATE), dengan retry
        def _load_staging():
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True
            )
            with open(file_path, "rb") as f:
                load_job = client.load_table_from_file(f, STAGING_TABLE_ID, job_config=job_config)
            load_job.result()

        retry_with_backoff(
            _load_staging,
            retriable_exceptions=(Exception,),
            description=f"Load ke staging {STAGING_TABLE_ID}"
        )
        logger.info(f"📥 Berhasil menulis data baru ke staging: {STAGING_TABLE_ID}")

        # Check apakah tabel utama sudah ada
        def _check_table():
            try:
                client.get_table(MAIN_TABLE_ID)
                return True
            except NotFound:
                return False

        table_exists = retry_with_backoff(
            _check_table,
            retriable_exceptions=(Exception,),
            description=f"Cek keberadaan tabel {MAIN_TABLE_ID}"
        )

        if not table_exists:
            logger.info(f"🗂️ Tabel utama `{MAIN_TABLE_ID}` belum ada. Membuat tabel baru dari staging...")
            create_query = f"CREATE TABLE `{MAIN_TABLE_ID}` AS SELECT * FROM `{STAGING_TABLE_ID}`"

            def _create_table():
                client.query(create_query).result()

            retry_with_backoff(
                _create_table,
                retriable_exceptions=(Exception,),
                description="CREATE TABLE tabel utama"
            )
            logger.info("🎉 Sukses membuat tabel utama awal.")
        else:
            # 2. FIX LOGIC MERGE: Delete-then-Insert, dibungkus TRANSACTION
            # agar atomik — kalau salah satu statement gagal, semua di-rollback
            # dan tabel utama tidak kehilangan data.
            upsert_query = f"""
                BEGIN TRANSACTION;

                DELETE FROM `{MAIN_TABLE_ID}`
                WHERE id IN (SELECT id FROM `{STAGING_TABLE_ID}`);

                INSERT INTO `{MAIN_TABLE_ID}`
                SELECT 
                    id, message, sent_by, sent_by_name, sent_by_type, conversation_id, media_url, media_type, platform_mid, status, business_id, created_at, updated_at, additional_data,
                    CAST(NULL AS INT64) AS location, SAFE_CAST(interactive AS INT64) AS interactive, action, chat_credits_used, contact, inbox, CAST(ads_data AS STRING) AS ads_data
                FROM `{STAGING_TABLE_ID}`;

                COMMIT TRANSACTION;
            """

            def _upsert():
                client.query(upsert_query).result()

            retry_with_backoff(
                _upsert,
                retriable_exceptions=(Exception,),
                description="Upsert (DELETE+INSERT transaction) ke tabel utama"
            )
            logger.info(f"🚀 Data Upserted successfully into `{MAIN_TABLE_ID}`.")

    finally:
        # Selalu bersihkan file lokal runner, baik sukses maupun gagal,
        # supaya tidak menumpuk file parquet di disk runner.
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"🧹 File lokal {file_path} dibersihkan.")


# =========================
# 4. MAIN EXECUTION
# =========================
if __name__ == "__main__":
    bq_client = bigquery.Client()
    saved_parquet = extract_messages_cekat()
    if saved_parquet:
        load_staging_and_upsert(saved_parquet, bq_client)
