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

# Backfill manual: 20 September - 4 Oktober 2026.
# Setelah backfill selesai, kembalikan tanggal dinamis
# untuk menjalankan ETL harian.
START_DATE = "2026-09-20"
END_DATE = "2026-10-04"

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
