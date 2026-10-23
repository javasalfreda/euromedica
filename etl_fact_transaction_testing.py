import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymysql
from google.cloud import bigquery

MYSQL_HOST = os.getenv("MYSQL_HOST", "100.87.194.79")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")

COMPANY = os.getenv("ERP_COMPANY", "SKIN+ DEI")
SOURCE_TIMEZONE = ZoneInfo("Asia/Jakarta")
BATCH_SIZE = int(os.getenv("EXTRACT_BATCH_SIZE", "5000"))

DEFAULT_TABLE_ID = "euromedica-495509.database.fact_transaction_testing"
TABLE_EXPIRATION = "2099-01-01 00:00:00+00"

DATE_COLUMNS = ["transaction_date", "posting_date"]
NUMERIC_COLUMNS = [
    "quantity", "net_rate", "net_amount", "price_list_rate",
    "delivered_quantity", "billed_amount", "dpp_retail", "ppn_retail", "collection",
]

COLUMNS = [
    "transaction_date", "sales_order", "item_code", "item_name", "item_group",
    "description", "quantity", "uom", "net_rate", "net_amount",
    "customer", "customer_name", "customer_group", "customer_number", "client_type",
    "sales_consultant", "employee_name", "doctor_name", "price_list", "pricing_rule",
    "order_type", "price_list_rate", "territory", "project",
    "delivered_quantity", "billed_amount", "company", "dpp_retail", "ppn_retail",
    "collection", "treatment_voucher", "event_name", "posting_date", "_extracted_at",
]

ARROW_SCHEMA = pa.schema([
    pa.field("transaction_date", pa.date32()),
    pa.field("sales_order", pa.string()),
    pa.field("item_code", pa.string()),
    pa.field("item_name", pa.string()),
    pa.field("item_group", pa.string()),
    pa.field("description", pa.string()),
    pa.field("quantity", pa.float64()),
    pa.field("uom", pa.string()),
    pa.field("net_rate", pa.float64()),
    pa.field("net_amount", pa.float64()),
    pa.field("customer", pa.string()),
    pa.field("customer_name", pa.string()),
    pa.field("customer_group", pa.string()),
    pa.field("customer_number", pa.string()),
    pa.field("client_type", pa.string()),
    pa.field("sales_consultant", pa.string()),
    pa.field("employee_name", pa.string()),
    pa.field("doctor_name", pa.string()),
    pa.field("price_list", pa.string()),
    pa.field("pricing_rule", pa.string()),
    pa.field("order_type", pa.string()),
    pa.field("price_list_rate", pa.float64()),
    pa.field("territory", pa.string()),
    pa.field("project", pa.string()),
    pa.field("delivered_quantity", pa.float64()),
    pa.field("billed_amount", pa.float64()),
    pa.field("company", pa.string()),
    pa.field("dpp_retail", pa.float64()),
    pa.field("ppn_retail", pa.float64()),
    pa.field("collection", pa.float64()),
    pa.field("treatment_voucher", pa.string()),
    pa.field("event_name", pa.string()),
    pa.field("posting_date", pa.date32()),
    pa.field("_extracted_at", pa.timestamp("us", tz="UTC")),
])

BQ_SCHEMA = [
    bigquery.SchemaField("transaction_date", "DATE"),
    bigquery.SchemaField("sales_order", "STRING"),
    bigquery.SchemaField("item_code", "STRING"),
    bigquery.SchemaField("item_name", "STRING"),
    bigquery.SchemaField("item_group", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("quantity", "FLOAT"),
    bigquery.SchemaField("uom", "STRING"),
    bigquery.SchemaField("net_rate", "FLOAT"),
    bigquery.SchemaField("net_amount", "FLOAT"),
    bigquery.SchemaField("customer", "STRING"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("customer_group", "STRING"),
    bigquery.SchemaField("customer_number", "STRING"),
    bigquery.SchemaField("client_type", "STRING"),
    bigquery.SchemaField("sales_consultant", "STRING"),
    bigquery.SchemaField("employee_name", "STRING"),
    bigquery.SchemaField("doctor_name", "STRING"),
    bigquery.SchemaField("price_list", "STRING"),
    bigquery.SchemaField("pricing_rule", "STRING"),
    bigquery.SchemaField("order_type", "STRING"),
    bigquery.SchemaField("price_list_rate", "FLOAT"),
    bigquery.SchemaField("territory", "STRING"),
    bigquery.SchemaField("project", "STRING"),
    bigquery.SchemaField("delivered_quantity", "FLOAT"),
    bigquery.SchemaField("billed_amount", "FLOAT"),
    bigquery.SchemaField("company", "STRING"),
    bigquery.SchemaField("dpp_retail", "FLOAT"),
    bigquery.SchemaField("ppn_retail", "FLOAT"),
    bigquery.SchemaField("collection", "FLOAT"),
    bigquery.SchemaField("treatment_voucher", "STRING"),
    bigquery.SchemaField("event_name", "STRING"),
    bigquery.SchemaField("posting_date", "DATE"),
    bigquery.SchemaField("_extracted_at", "TIMESTAMP"),
]

QUERY = """
WITH so AS (
  SELECT so.name, so.transaction_date, so.customer, so.customer_name, so.customer_group,
         so.territory, so.doctor_name, so.order_type, so.selling_price_list,
         so.client_type, so.company, so.project
  FROM `tabSales Order` so
  WHERE so.docstatus = 1 AND so.company = %s
    AND so.transaction_date >= %s AND so.transaction_date < %s
),
soi AS (
  SELECT soi.name AS so_item, soi.parent AS sales_order, soi.item_code, soi.item_name,
         soi.item_group, soi.description, soi.qty, soi.uom, soi.net_rate, soi.net_amount,
         soi.price_list_rate, soi.delivered_qty, soi.billed_amt, soi.treatment_voucher,
         soi.pricing_rules
  FROM `tabSales Order Item` soi
  JOIN so ON so.name = soi.parent
),
cust AS (
  SELECT name, customer_number FROM `tabCustomer`
  WHERE name IN (SELECT DISTINCT customer FROM so)
),
team AS (
  SELECT parent, sales_person, employee_name FROM (
    SELECT st.parent, st.sales_person, sp.employee_name,
           ROW_NUMBER() OVER (PARTITION BY st.parent ORDER BY st.idx) AS rn
    FROM `tabSales Team` st
    LEFT JOIN `tabSales Person` sp ON sp.name = st.sales_person
    WHERE st.parenttype = 'Sales Order'
      AND st.parent IN (SELECT name FROM so)
  ) x WHERE rn = 1
),
adv AS (
  SELECT per.reference_name AS sales_order, SUM(per.allocated_amount) AS advance
  FROM `tabPayment Entry Reference` per
  JOIN `tabPayment Entry` pe ON pe.name = per.parent AND pe.docstatus = 1
  JOIN so ON so.name = per.reference_name
  WHERE per.reference_doctype = 'Sales Order'
    AND pe.mode_of_payment NOT LIKE 'Convert%%'
  GROUP BY per.reference_name
),
si_links AS (
  SELECT sii.parent AS si, sii.sales_order, sii.so_detail AS so_item, sii.net_amount
  FROM `tabSales Invoice Item` sii
  JOIN so ON so.name = sii.sales_order
  WHERE sii.so_detail IS NOT NULL AND sii.so_detail <> ''
),
si_totals AS (
  SELECT sii.parent AS si, SUM(sii.net_amount) AS si_net
  FROM `tabSales Invoice Item` sii
  WHERE sii.parent IN (SELECT DISTINCT si FROM si_links)
  GROUP BY sii.parent
),
si_alloc AS (
  SELECT per.reference_name AS si, SUM(per.allocated_amount) AS alloc
  FROM `tabPayment Entry Reference` per
  JOIN `tabPayment Entry` pe ON pe.name = per.parent AND pe.docstatus = 1
  WHERE per.reference_doctype = 'Sales Invoice'
    AND per.reference_name IN (SELECT DISTINCT si FROM si_links)
    AND pe.mode_of_payment NOT LIKE 'Convert%%'
  GROUP BY per.reference_name
),
inv AS (
  SELECT l.sales_order, l.so_item,
         SUM(l.net_amount * a.alloc / NULLIF(t.si_net, 0)) AS inv_collected
  FROM si_links l
  JOIN si_alloc a ON a.si = l.si
  JOIN si_totals t ON t.si = l.si
  GROUP BY l.sales_order, l.so_item
),
pd_map AS (
  SELECT x.sales_order, MIN(x.posting_date) AS posting_date FROM (
    SELECT sii.sales_order, pe.posting_date
    FROM `tabPayment Entry Reference` per
    JOIN `tabPayment Entry` pe ON pe.name = per.parent AND pe.docstatus = 1
    JOIN `tabSales Invoice Item` sii ON sii.parent = per.reference_name
    JOIN so ON so.name = sii.sales_order
    WHERE per.reference_doctype = 'Sales Invoice'
      AND sii.so_detail IS NOT NULL AND sii.so_detail <> ''
    UNION ALL
    SELECT per.reference_name AS sales_order, pe.posting_date
    FROM `tabPayment Entry Reference` per
    JOIN `tabPayment Entry` pe ON pe.name = per.parent AND pe.docstatus = 1
    JOIN so ON so.name = per.reference_name
    WHERE per.reference_doctype = 'Sales Order'
  ) x
  GROUP BY x.sales_order
),
tv_voucher AS (
  SELECT itv.parent, itv.treatment_voucher, MAX(itv.event_name) AS event_name
  FROM `tabInvoice Treatment Voucher` itv
  WHERE itv.parent IN (SELECT name FROM so)
    AND itv.treatment_voucher IS NOT NULL AND itv.treatment_voucher <> ''
  GROUP BY itv.parent, itv.treatment_voucher
),
tv_item AS (
  SELECT itv.parent, itv.item_name, MAX(itv.event_name) AS event_name
  FROM `tabInvoice Treatment Voucher` itv
  WHERE itv.parent IN (SELECT name FROM so)
    AND itv.item_name IS NOT NULL AND itv.item_name <> ''
  GROUP BY itv.parent, itv.item_name
),
base AS (
  SELECT soi.*, so.transaction_date, so.customer, so.customer_name, so.customer_group,
         so.territory, so.doctor_name, so.order_type, so.selling_price_list,
         so.client_type, so.company, so.project,
         COALESCE(inv.inv_collected, 0) AS inv_collected,
         COALESCE(adv.advance, 0) AS advance
  FROM soi
  JOIN so ON so.name = soi.sales_order
  LEFT JOIN inv ON inv.so_item = soi.so_item
  LEFT JOIN adv ON adv.sales_order = soi.sales_order
),
calc AS (
  SELECT b.*,
         GREATEST(b.net_amount - b.inv_collected, 0) AS remaining,
         SUM(GREATEST(b.net_amount - b.inv_collected, 0))
           OVER (PARTITION BY b.sales_order) AS total_remaining,
         SUM(b.net_amount) OVER (PARTITION BY b.sales_order) AS total_net
  FROM base b
),
fin AS (
  SELECT c.*,
         c.inv_collected
         + CASE WHEN c.total_remaining > 0
                THEN c.advance * c.remaining / c.total_remaining
                ELSE 0 END
         + CASE WHEN c.total_remaining < c.advance AND c.total_net > 0
                THEN (c.advance - c.total_remaining) * c.net_amount / c.total_net
                ELSE 0 END AS collection
  FROM calc c
),
si_doc AS (
  SELECT sii.sales_order AS sales_order
  FROM `tabSales Invoice Item` sii
  WHERE sii.sales_order IN (SELECT name FROM so)
  UNION
  SELECT soi2.parent AS sales_order
  FROM `tabSales Invoice Item` sii
  JOIN `tabSales Order Item` soi2 ON soi2.name = sii.so_detail
  WHERE soi2.parent IN (SELECT name FROM so)
),
dn_doc AS (
  SELECT dni.against_sales_order AS sales_order
  FROM `tabDelivery Note Item` dni
  WHERE dni.against_sales_order IN (SELECT name FROM so)
  UNION
  SELECT soi3.parent AS sales_order
  FROM `tabDelivery Note Item` dni
  JOIN `tabSales Order Item` soi3 ON soi3.name = dni.so_detail
  WHERE soi3.parent IN (SELECT name FROM so)
),
pay_doc AS (
  SELECT per.reference_name AS sales_order
  FROM `tabPayment Entry Reference` per
  JOIN `tabPayment Entry` pe ON pe.name = per.parent AND pe.docstatus = 1
  WHERE per.reference_name IN (SELECT name FROM so)
  UNION
  SELECT soi4.parent AS sales_order
  FROM `tabPayment Entry Reference` per
  JOIN `tabPayment Entry` pe ON pe.name = per.parent AND pe.docstatus = 1
  JOIN `tabSales Invoice Item` sii ON sii.parent = per.reference_name
  JOIN `tabSales Order Item` soi4 ON soi4.name = sii.so_detail
  WHERE soi4.parent IN (SELECT name FROM so)
),
item_act AS (
  SELECT f.sales_order,
         MAX(CASE WHEN f.delivered_qty > 0 OR f.billed_amt > 0 OR f.collection > 0
                  THEN 1 ELSE 0 END) AS has_act
  FROM fin f
  GROUP BY f.sales_order
)
SELECT
  f.transaction_date,
  f.sales_order,
  f.item_code,
  f.item_name,
  f.item_group,
  f.description,
  f.qty                     AS quantity,
  f.uom,
  f.net_rate,
  f.net_amount,
  f.customer,
  f.customer_name,
  f.customer_group,
  c.customer_number,
  f.client_type,
  t.sales_person            AS sales_consultant,
  t.employee_name,
  f.doctor_name,
  f.selling_price_list      AS price_list,
  COALESCE(
    CASE WHEN JSON_VALID(f.pricing_rules)
         THEN JSON_UNQUOTE(JSON_EXTRACT(f.pricing_rules, '$[0]'))
         ELSE '' END, '')    AS pricing_rule,
  f.order_type,
  f.price_list_rate,
  f.territory,
  f.project,
  f.delivered_qty           AS delivered_quantity,
  f.billed_amt              AS billed_amount,
  f.company,
  0.0                       AS dpp_retail,
  0.0                       AS ppn_retail,
  f.collection,
  f.treatment_voucher,
  COALESCE(tvv.event_name, tvi.event_name) AS event_name,
  pdm.posting_date
FROM fin f
JOIN item_act ia ON ia.sales_order = f.sales_order
LEFT JOIN si_doc sd ON sd.sales_order = f.sales_order
LEFT JOIN dn_doc dd ON dd.sales_order = f.sales_order
LEFT JOIN pay_doc pdx ON pdx.sales_order = f.sales_order
LEFT JOIN cust c ON c.name = f.customer
LEFT JOIN team t ON t.parent = f.sales_order
LEFT JOIN pd_map pdm ON pdm.sales_order = f.sales_order
LEFT JOIN tv_voucher tvv
  ON tvv.parent = f.sales_order AND tvv.treatment_voucher = f.treatment_voucher
LEFT JOIN tv_item tvi
  ON tvi.parent = f.sales_order AND tvi.item_name = f.item_code
WHERE ia.has_act = 1
   OR f.net_amount = 0
   OR sd.sales_order IS NOT NULL
   OR dd.sales_order IS NOT NULL
   OR pdx.sales_order IS NOT NULL
"""


def parse_args():
    parser = argparse.ArgumentParser(description="ETL fact_transaction dari MariaDB ERP")
    parser.add_argument("--start", default=os.getenv("START_DATE", "2022-01-01"),
                        help="Tanggal awal (YYYY-MM-DD), default 2022-01-01")
    parser.add_argument("--end", default=os.getenv("END_DATE"),
                        help="Tanggal akhir eksklusif (YYYY-MM-DD), default besok (WIB)")
    parser.add_argument("--table", default=os.getenv("BQ_TABLE", DEFAULT_TABLE_ID),
                        help="Tabel target BigQuery")
    parser.add_argument("--staging", default=os.getenv("BQ_STAGING_TABLE"),
                        help="Tabel staging BigQuery (default: <table>_staging)")
    parser.add_argument("--no-load", action="store_true",
                        help="Hanya ekstrak ke parquet, tanpa load ke BigQuery")
    parser.add_argument("--keep-parquet", action="store_true",
                        help="Jangan hapus file parquet setelah load")
    return parser.parse_args()


def normalize_batch(df, extracted_at):
    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.date

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")

    for column in df.columns:
        if column in DATE_COLUMNS or column in NUMERIC_COLUMNS:
            continue
        if column == "_extracted_at":
            continue
        df[column] = df[column].map(
            lambda value: None if value is None or pd.isna(value) else str(value)
        )

    df["_extracted_at"] = pd.Timestamp(extracted_at)
    return df


def extract_to_parquet(args):
    end_date = args.end
    if not end_date:
        end_date = (datetime.now(SOURCE_TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")

    extracted_at = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
    execution_date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = f"fact_transaction_db_{execution_date}.parquet"

    connection = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        connect_timeout=15,
        read_timeout=3600,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSCursor,
    )

    writer = pq.ParquetWriter(file_path, ARROW_SCHEMA, compression="snappy")
    total_rows = 0
    batch_count = 0
    total_net = 0.0
    total_billed = 0.0
    total_collection = 0.0

    try:
        print(f"Menarik fact_transaction dari {MYSQL_DATABASE} periode {args.start} s/d {end_date}")
        with connection.cursor() as cursor:
            cursor.execute(QUERY, (COMPANY, args.start, end_date))
            column_names = [description[0] for description in cursor.description]
            print(f"Kolom hasil query: {len(column_names)}")

            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                df = pd.DataFrame(rows, columns=column_names)
                df = normalize_batch(df, extracted_at)

                table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False)
                writer.write_table(table)

                total_rows += len(df)
                total_net += float(df["net_amount"].sum())
                total_billed += float(df["billed_amount"].sum())
                total_collection += float(df["collection"].sum())
                batch_count += 1
                if batch_count % 10 == 0:
                    print(f"Extracted {total_rows} rows")
    finally:
        connection.close()
        writer.close()

    if total_rows == 0:
        raise ValueError("Tidak ada data fact_transaction pada rentang tanggal tersebut")

    print(f"Parquet saved: {file_path} ({total_rows} rows)")
    print(f"Summary: net={total_net:,.2f} billed={total_billed:,.2f} collection={total_collection:,.2f}")
    return file_path, total_rows


def load_to_staging(file_path, args, client):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=BQ_SCHEMA,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, args.staging, job_config=job_config)

    job.result()
    print(f"Data loaded to staging ({args.staging}): {job.output_rows} rows")


def replace_target(args, client):
    print(f"Mengganti seluruh data di {args.table}")
    client.query(f"CREATE OR REPLACE TABLE `{args.table}` AS SELECT * FROM `{args.staging}`").result()

    client.query(
        f"ALTER TABLE `{args.table}` "
        f"SET OPTIONS (expiration_timestamp = TIMESTAMP '{TABLE_EXPIRATION}')"
    ).result()
    print(f"Tabel diganti dan expiration di-set 2099: {args.table}")


def cleanup_staging(args, client):
    try:
        client.query(f"TRUNCATE TABLE `{args.staging}`").result()
        print(f"Staging dikosongkan: {args.staging}")
    except Exception as exc:
        print(f"Warning: gagal cleanup staging: {exc}")


def main():
    args = parse_args()
    if not (MYSQL_USER and MYSQL_PASSWORD and MYSQL_DATABASE):
        raise SystemExit("Set env MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")
    if not args.staging:
        args.staging = f"{args.table}_staging"
    start_time = time.time()

    file_path, rows = extract_to_parquet(args)

    if args.no_load:
        print(f"Selesai tanpa load BQ ({rows} rows) dalam {(time.time()-start_time)/60:.1f} menit")
        return

    client = bigquery.Client()
    load_to_staging(file_path, args, client)
    replace_target(args, client)
    cleanup_staging(args, client)

    if not args.keep_parquet:
        try:
            os.remove(file_path)
        except OSError:
            pass

    print(f"Selesai: {rows} rows dalam {(time.time()-start_time)/60:.1f} menit")


if __name__ == "__main__":
    main()



