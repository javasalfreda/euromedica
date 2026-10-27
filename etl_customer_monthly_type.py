import argparse
import os
import time
from datetime import datetime

import pandas as pd
import pendulum
import pymysql
from dateutil.relativedelta import relativedelta
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

DB_USER = os.getenv("MYSQL_USER", "")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_HOST = os.getenv("MYSQL_HOST", "100.87.194.79")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_NAME = os.getenv("MYSQL_DATABASE", "")

JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")

DEFAULT_TABLE_ID = "euromedica-495509.database.customer_monthly_type"
STAGING_TABLE_ID = "euromedica-495509.database.customer_monthly_type_staging"
TABLE_EXPIRATION = "2099-01-01 00:00:00+00"

BQ_SCHEMA = [
    bigquery.SchemaField("status_date", "DATE"),
    bigquery.SchemaField("customer", "STRING"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("contact_no", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("client_type", "STRING"),
]

STRING_COLUMNS = [
    "customer",
    "customer_name",
    "contact_no",
    "brand",
    "client_type",
]

QUERY_TEMPLATE = """
WITH base AS (
    SELECT
        ccd.customer,
        h.status_date,
        DATE_FORMAT(h.status_date, '%Y-%m') AS month_key,
        CASE
            WHEN LOWER(h.status_territory) LIKE '%sempvrna%' THEN 'Sempvrna'
            WHEN LOWER(h.status_territory) LIKE '%skin+%'   THEN 'SKIN+'
            WHEN LOWER(h.status_territory) LIKE '%slim+%'   THEN 'SLIM+'
            WHEN LOWER(h.status_territory) LIKE '%esc%'     THEN 'ESC'
            WHEN LOWER(h.status_territory) LIKE '%esl%'     THEN 'ESL'
            WHEN LOWER(h.status_territory) LIKE '%ehl%'     THEN 'EHL'
        END AS brand,
        CASE
            WHEN h.status = 'new'    THEN 1
            WHEN h.status = 'active' THEN 2
            WHEN h.status = 'trial'  THEN 3
        END AS priority,
        CASE
            WHEN h.status = 'new'    THEN 'new'
            WHEN h.status = 'active' THEN 'active'
            WHEN h.status = 'trial'  THEN 'trial'
        END AS status_type,
        c.customer_name,
        c.contact_no
    FROM `tabCustomer History Category` h
    JOIN `tabClient Category Data` ccd ON ccd.name = h.parent
    JOIN `tabCustomer` c ON c.name = ccd.customer
    WHERE c.disabled = 0
      AND h.status_date >= '{start_date}'
      AND h.status_date <= '{end_date}'
),
ranked AS (
    SELECT
        customer,
        month_key,
        status_date,
        brand,
        status_type,
        customer_name,
        contact_no,
        ROW_NUMBER() OVER (
            PARTITION BY customer, brand, month_key
            ORDER BY priority, status_date DESC
        ) AS rn
    FROM base
    WHERE status_type IS NOT NULL
),
latest AS (
    SELECT
        customer,
        status_date,
        brand,
        status_type,
        customer_name,
        contact_no
    FROM ranked
    WHERE rn = 1
),
detail AS (
    SELECT
        customer,
        status_date,
        brand,
        status_type,
        customer_name,
        REGEXP_REPLACE(contact_no, '[^0-9]', '') AS phone_digits
    FROM latest
)
SELECT
    status_date,
    customer,
    customer_name,
    CASE
        WHEN phone_digits LIKE '62%' THEN phone_digits
        WHEN phone_digits LIKE '08%' THEN CONCAT('62', SUBSTR(phone_digits, 2))
        WHEN phone_digits LIKE '8%'  THEN CONCAT('62', phone_digits)
        ELSE phone_digits
    END AS contact_no,
    brand,
    status_type AS client_type
FROM detail
ORDER BY status_date, brand, customer_name
"""


def default_start_date():
    today = pendulum.now(JAKARTA_TZ).date()
    return today.replace(day=1) - relativedelta(months=1)


def parse_date(value, label):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"{label} harus format YYYY-MM-DD: {value}") from exc


def extract_data(start_date, end_date):
    query = QUERY_TEMPLATE.format(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )

    connection = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        connect_timeout=15,
        read_timeout=1800,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSCursor,
    )

    try:
        with connection.cursor() as cursor:
            print("Menarik data customer monthly type...")
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
    finally:
        connection.close()

    df = pd.DataFrame(rows, columns=columns)

    for column in STRING_COLUMNS:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda value: None if value is None or pd.isna(value) else str(value)
            )

    if df.empty:
        return df

    df["status_date"] = pd.to_datetime(df["status_date"]).dt.date

    print(f"Rows extracted: {len(df):,}")
    print(f"Status date: {df['status_date'].min()} s/d {df['status_date'].max()}")
    print(df.groupby(["brand", "client_type"]).size().to_string())

    return df


def table_exists(client, table_id):
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def table_schema(client, table_id):
    table = client.get_table(table_id)
    return sorted((field.name, field.field_type) for field in table.schema)


def build_merge_sql(table_id, staging_id):
    return f"""
        MERGE `{table_id}` T
        USING `{staging_id}` S
        ON DATE_TRUNC(T.status_date, MONTH) = DATE_TRUNC(S.status_date, MONTH)
           AND T.customer = S.customer
           AND IFNULL(T.brand, '') = IFNULL(S.brand, '')
        WHEN MATCHED THEN
          UPDATE SET
            status_date = S.status_date,
            customer_name = S.customer_name,
            contact_no = S.contact_no,
            client_type = S.client_type
        WHEN NOT MATCHED THEN
          INSERT (status_date, customer, customer_name, contact_no, brand, client_type)
          VALUES (S.status_date, S.customer, S.customer_name, S.contact_no, S.brand, S.client_type)
    """


def load_to_bigquery(df, table_id, staging_id, mode="merge"):
    if df.empty:
        print("DataFrame kosong, tidak load ke BigQuery.")
        return

    if df["status_date"].isna().any() or df["customer"].isna().any():
        raise RuntimeError("status_date/customer tidak boleh NULL.")

    if mode == "merge":
        merge_key = df[["customer", "brand"]].copy()
        merge_key["brand"] = merge_key["brand"].fillna("")
        merge_key["month"] = pd.to_datetime(df["status_date"]).dt.strftime("%Y-%m")
        duplicate_count = int(merge_key.duplicated(keep=False).sum())
        if duplicate_count:
            raise RuntimeError(
                f"Ada {duplicate_count} baris dengan kunci MERGE duplikat "
                "(bulan, brand, customer)."
            )

    temp_parquet = (
        f"customer_monthly_type_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    try:
        df.to_parquet(temp_parquet, index=False)

        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=BQ_SCHEMA,
        )
        with open(temp_parquet, "rb") as source_file:
            job = client.load_table_from_file(source_file, staging_id, job_config=job_config)
        job.result()
        print(f"Loaded staging {staging_id}: {job.output_rows} rows")

        target_exists = table_exists(client, table_id)

        if mode == "replace" or not target_exists:
            client.query(
                f"CREATE OR REPLACE TABLE `{table_id}` AS SELECT * FROM `{staging_id}`"
            ).result()
            print(f"Tabel target diganti penuh: {table_id}")
        else:
            if table_schema(client, table_id) != table_schema(client, staging_id):
                raise RuntimeError(
                    "Schema target berbeda dengan staging. Samakan schema target "
                    "atau jalankan dengan --mode replace."
                )

            client.query(build_merge_sql(table_id, staging_id)).result()
            print(f"MERGE selesai: {table_id}")

        client.query(
            f"ALTER TABLE `{table_id}` SET OPTIONS "
            f"(expiration_timestamp = TIMESTAMP '{TABLE_EXPIRATION}')"
        ).result()

        try:
            client.query(f"TRUNCATE TABLE `{staging_id}`").result()
            print(f"Staging dikosongkan: {staging_id}")
        except Exception as exc:
            print(f"Warning: gagal cleanup staging: {exc}")
    finally:
        try:
            os.remove(temp_parquet)
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="Customer monthly type ETL (MariaDB -> BigQuery)"
    )
    parser.add_argument(
        "--start",
        default=os.getenv("START_DATE", ""),
        help="status_date awal (YYYY-MM-DD), kosong = awal bulan lalu",
    )
    parser.add_argument(
        "--end",
        default=os.getenv("END_DATE", ""),
        help="status_date akhir (YYYY-MM-DD), kosong = hari ini WIB",
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default=os.getenv("MODE", "merge"),
        help="merge = update/insert (default), replace = full replace",
    )
    parser.add_argument("--table", default=os.getenv("BQ_TABLE", DEFAULT_TABLE_ID))
    parser.add_argument(
        "--staging", default=os.getenv("BQ_STAGING_TABLE", STAGING_TABLE_ID)
    )
    parser.add_argument("--no-load", action="store_true")
    args = parser.parse_args()

    if not (DB_USER and DB_PASS and DB_NAME):
        raise SystemExit("Set env MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")

    start_date = parse_date(args.start, "start") or default_start_date()
    end_date = parse_date(args.end, "end") or pendulum.now(JAKARTA_TZ).date()

    if start_date > end_date:
        raise SystemExit("start tidak boleh lebih besar dari end")

    print(
        f"Window: {start_date.strftime('%Y-%m-%d')} s/d "
        f"{end_date.strftime('%Y-%m-%d')} | Mode: {args.mode}"
    )

    t0 = time.time()
    df = extract_data(start_date, end_date)

    if df.empty:
        print("Tidak ada data.")
        return

    if not args.no_load:
        load_to_bigquery(df, args.table, args.staging, mode=args.mode)

    print(f"Selesai dalam {(time.time() - t0) / 60:.1f} menit")


if __name__ == "__main__":
    main()
