import argparse
import os
import time
from datetime import datetime

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DB_USER = os.getenv("MYSQL_USER", "")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_HOST = os.getenv("MYSQL_HOST", "100.87.194.79")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_NAME = os.getenv("MYSQL_DATABASE", "")

PROJECT = "euromedica-495509"
DATASET = "database"
TABLE_ID = f"{PROJECT}.{DATASET}.customer_reactivation"
TABLE_EXPIRATION = "2099-01-01 00:00:00+00"

BQ_SCHEMA = [
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("contact_no", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("leaver_date", "DATE"),
    bigquery.SchemaField("reactivation_date", "DATE"),
    bigquery.SchemaField("days_to_reactivate", "INT64"),
]

COLUMNS = [field.name for field in BQ_SCHEMA]

QUERY = """
WITH base AS (
    SELECT
        ccd.customer,
        c.customer_name,
        c.contact_no,
        CASE
            WHEN INSTR(LOWER(h.status_territory), 'sempvrna') > 0 THEN 'Sempvrna'
            WHEN INSTR(LOWER(h.status_territory), 'skin+') > 0 THEN 'SKIN+'
            WHEN INSTR(LOWER(h.status_territory), 'slim+') > 0 THEN 'SLIM+'
            WHEN INSTR(LOWER(h.status_territory), 'esc') > 0 THEN 'ESC'
            WHEN INSTR(LOWER(h.status_territory), 'esl') > 0 THEN 'ESL'
            WHEN INSTR(LOWER(h.status_territory), 'ehl') > 0 THEN 'EHL'
            ELSE 'UNKNOWN'
        END AS brand,
        h.status,
        h.status_date,
        h.modified,
        h.creation,
        h.idx
    FROM `tabCustomer History Category` h
    JOIN `tabClient Category Data` ccd ON ccd.name = h.parent
    JOIN `tabCustomer` c ON c.name = ccd.customer
    WHERE c.disabled = 0
),
new_leavers AS (
    SELECT
        customer,
        customer_name,
        contact_no,
        brand,
        status AS leaver_status,
        status_date AS leaver_date
    FROM base
    WHERE status = 'new leaver'
),
candidates AS (
    SELECT
        l.customer,
        l.customer_name,
        l.contact_no,
        l.brand,
        l.leaver_status,
        l.leaver_date,
        (
            SELECT MIN(a.status_date)
            FROM base a
            WHERE a.customer = l.customer
              AND a.brand = l.brand
              AND a.status = 'active'
              AND a.status_date > l.leaver_date
        ) AS reactivation_date
    FROM new_leavers l
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer, brand, reactivation_date
            ORDER BY leaver_date DESC
        ) AS rn
    FROM candidates
    WHERE reactivation_date IS NOT NULL
      AND DATEDIFF(reactivation_date, leaver_date) >= 90
)
SELECT
    customer_name,
    contact_no,
    brand,
    leaver_date,
    reactivation_date,
    DATEDIFF(reactivation_date, leaver_date) AS days_to_reactivate
FROM ranked
WHERE rn = 1
ORDER BY reactivation_date DESC, customer
"""

def build_engine():
    url = URL.create(
        "mysql+pymysql",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)


def extract_data():
    engine = build_engine()
    try:
        print("Menarik data customer reactivation...", flush=True)
        df = pd.read_sql_query(QUERY, engine)
    finally:
        engine.dispose()

    if df.empty:
        return df

    for column in ["leaver_date", "reactivation_date"]:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.date

    df["days_to_reactivate"] = pd.to_numeric(
        df["days_to_reactivate"], errors="coerce"
    ).astype("Int64")

    for column in ["customer_name", "contact_no", "brand"]:
        df[column] = df[column].apply(
            lambda value: None if pd.isna(value) else str(value)
        )

    return df


def get_target_table(client):
    try:
        table = client.get_table(TABLE_ID)
    except NotFound as exc:
        raise RuntimeError(
            f"Tabel target {TABLE_ID} belum ada. "
            "Jalankan DDL customer_reactivation terlebih dahulu."
        ) from exc

    actual_schema = [(field.name, field.field_type) for field in table.schema]
    expected_schema = [(field.name, field.field_type) for field in BQ_SCHEMA]
    if actual_schema != expected_schema:
        raise RuntimeError(
            f"Schema target {TABLE_ID} berbeda. "
            f"Expected={expected_schema} Actual={actual_schema}"
        )
    return table


def load_to_bigquery(df):
    client = bigquery.Client()
    get_target_table(client)

    if df.empty:
        client.query(f"TRUNCATE TABLE `{TABLE_ID}`").result()
        print("Data kosong, target dikosongkan.", flush=True)
    else:
        df_out = df[COLUMNS].copy()
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=BQ_SCHEMA,
        )
        job = client.load_table_from_dataframe(
            df_out, TABLE_ID, job_config=job_config
        )
        job.result()
        print(f"Loaded {job.output_rows} rows ke {TABLE_ID}", flush=True)

    client.query(
        f"ALTER TABLE `{TABLE_ID}` SET OPTIONS "
        f"(expiration_timestamp = TIMESTAMP '{TABLE_EXPIRATION}')"
    ).result()

    target_count = list(
        client.query(f"SELECT COUNT(*) AS c FROM `{TABLE_ID}`").result()
    )[0][0]
    if target_count != len(df):
        raise RuntimeError(
            f"Count target ({target_count}) != source ({len(df)})"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Customer reactivation ETL (MariaDB -> BigQuery)"
    )
    parser.add_argument("--no-load", action="store_true")
    args = parser.parse_args()

    if not (DB_USER and DB_PASS and DB_NAME):
        raise SystemExit("Set env MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")

    t0 = time.time()
    df = extract_data()

    if df.empty:
        print("Tidak ada data customer reactivation.", flush=True)
    else:
        print(f"Rows extracted: {len(df):,}", flush=True)
        print(
            df.groupby(["brand"]).size().sort_values(ascending=False).to_string(),
            flush=True,
        )

    if not args.no_load:
        load_to_bigquery(df)

    print(f"Selesai dalam {(time.time() - t0) / 60:.1f} menit", flush=True)


if __name__ == "__main__":
    main()
