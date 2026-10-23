import argparse
import json
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

SOURCE_TIMEZONE = ZoneInfo("Asia/Jakarta")
INVOICE_BATCH_SIZE = int(os.getenv("INVOICE_BATCH_SIZE", "1000"))
LOOKBACK_DAYS = int(os.getenv("PAYMENT_LOOKBACK_DAYS", "30"))

DEFAULT_TABLE_ID = "euromedica-495509.database.item_wise_sales"
DEFAULT_COMPANIES = [
    "SKIN+ DEI",
    "European Slimming Centre",
    "EUROHAIRLAB",
    "EUROSKINLAB",
]
# Business hanya jadi filter di company ini; report mengabaikan business untuk ESC/ESL/EHL.
BUSINESS_COMPANIES = {"SKIN+ DEI", "SKIN+ MBS", "SLIM+"}
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

# Default business untuk company. Mengikuti perilaku _apply_default_for_empty_business:
# business kosong/NULL dianggap business default company.
DEFAULT_BUSINESS = {
    "SKIN+ DEI": "SKIN+",
    "SKIN+ MBS": "SKIN+",
    "SLIM+": "SLIM+",
    "European Slimming Centre": "ESC+",
    "EUROSKINLAB": "ESL",
    "EUROHAIRLAB": "EHL",
    "DMI": "DMI",
}


def apply_default_business(company, business):
    if business is None or str(business).strip() == "":
        return DEFAULT_BUSINESS.get(company)
    return business


def business_condition(company, business):
    """Approximation get_business_query_condition: business NULL/'' ikut default company."""
    if not business:
        return "", []
    default = apply_default_business(company, None)
    if business == default:
        return " AND (so.business = %s OR so.business IS NULL OR so.business = '')", [business]
    return " AND so.business = %s", [business]


def parse_args():
    parser = argparse.ArgumentParser(description="ETL Item-wise Sales History dari MariaDB")
    parser.add_argument("--start", default=os.getenv("START_DATE", "2022-01-01"),
                        help="from_date report (YYYY-MM-DD)")
    parser.add_argument("--end", default=os.getenv("END_DATE"),
                        help="to_date report (YYYY-MM-DD), default hari ini WIB")
    parser.add_argument("--company", default=None,
                        help="Satu company (opsional)")
    parser.add_argument("--companies", default=os.getenv("ERP_COMPANIES"),
                        help="Daftar company dipisah koma "
                             "(default: SKIN+ DEI, European Slimming Centre, EUROHAIRLAB, EUROSKINLAB)")
    parser.add_argument("--business", default=os.getenv("BUSINESS"))
    parser.add_argument("--territory", default=os.getenv("TERRITORY"))
    parser.add_argument("--item-group", default=os.getenv("ITEM_GROUP"))
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS,
                        help="Lookback pembayaran sebelum from_date (default 30)")
    parser.add_argument("--table", default=os.getenv("BQ_TABLE", DEFAULT_TABLE_ID))
    parser.add_argument("--staging", default=os.getenv("BQ_STAGING_TABLE"),
                        help="Tabel staging (default <table>_staging)")
    parser.add_argument("--no-load", action="store_true")
    parser.add_argument("--keep-parquet", action="store_true")
    parser.add_argument("--batch-size", type=int, default=INVOICE_BATCH_SIZE,
                        help="Jumlah SO per query detail")
    return parser.parse_args()


def resolve_companies(args):
    if args.companies:
        raw = args.companies.split(",")
    elif args.company:
        raw = [args.company]
    else:
        raw = list(DEFAULT_COMPANIES)

    companies = []
    for item in raw:
        name = item.strip()
        if name and name not in companies:
            companies.append(name)
    if not companies:
        raise SystemExit("Daftar company kosong")
    return companies


def connect():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        connect_timeout=15,
        read_timeout=3600,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSDictCursor,
    )


# ──────────────────────────────────────────────
#  Report logic: payment hash & invoice ids
# ──────────────────────────────────────────────
def get_sales_order_payment(cursor, filters):
    invoice_ids = []
    invoice_set = set()
    payment_hash = {}

    from_date = filters["from_date"]
    to_date = filters["to_date"]
    lookback_date = from_date - timedelta(days=filters["lookback_days"])
    company = filters["company"]

    b_cond, b_params = business_condition(company, filters.get("business"))

    common_filters = ""
    common_params = []
    if filters.get("territory"):
        common_filters += " AND so.territory = %s"
        common_params.append(filters["territory"])

    item_group_filter = ""
    item_group_params = []
    if filters.get("item_group"):
        item_group_filter = (" AND EXISTS (SELECT 1 FROM `tabSales Order Item` soi "
                             "WHERE soi.parent = so.name AND soi.item_group = %s)")
        item_group_params.append(filters["item_group"])

    # 1) Payment Entry referenced langsung ke Sales Order
    q1 = f"""
        SELECT so.name AS so_name,
               pe.posting_date AS pe_posting_date,
               pe.paid_amount AS pe_paid_amount,
               pe.mode_of_payment AS pe_mode_of_payment,
               pe.name AS pe_name
        FROM `tabPayment Entry` pe
        JOIN `tabPayment Entry Reference` pee ON pee.parent = pe.name
        JOIN `tabSales Order` so ON so.name = pee.reference_name
        WHERE so.docstatus = 1
          AND pe.docstatus = 1
          AND pe.payment_type = 'Receive'
          AND pe.posting_date BETWEEN %s AND %s
          AND pe.company = %s
          AND so.advance_paid = so.rounded_total
          {common_filters}
          {item_group_filter}
          {b_cond}
        ORDER BY so.name, pe.posting_date
    """
    params = [lookback_date, to_date, company] + common_params + item_group_params + b_params
    cursor.execute(q1, params)
    collect_payments(cursor, from_date, to_date, invoice_ids, invoice_set, payment_hash)

    # 2) Payment Entry referenced ke Sales Invoice yang terhubung Sales Order
    q2 = f"""
        SELECT so.name AS so_name,
               pe.posting_date AS pe_posting_date,
               pe.paid_amount AS pe_paid_amount,
               pe.mode_of_payment AS pe_mode_of_payment,
               pe.name AS pe_name
        FROM `tabPayment Entry` pe
        JOIN `tabPayment Entry Reference` pee ON pee.parent = pe.name
        JOIN `tabSales Invoice` si ON si.name = pee.reference_name
        JOIN `tabSales Invoice Item` sii ON sii.parent = si.name
        JOIN `tabSales Order Item` soi ON soi.name = sii.so_detail
        JOIN `tabSales Order` so ON so.name = soi.parent
        WHERE so.docstatus = 1
          AND si.docstatus = 1
          AND pe.docstatus = 1
          AND pe.payment_type = 'Receive'
          AND pe.posting_date BETWEEN %s AND %s
          AND pe.company = %s
          AND so.advance_paid = so.rounded_total
          AND substring_index(pe.remarks, ' Sales Order ', -1) = so.name
          {common_filters}
          {item_group_filter}
          {b_cond}
        ORDER BY so.name, pe.posting_date
    """
    params = [lookback_date, to_date, company] + common_params + item_group_params + b_params
    cursor.execute(q2, params)
    collect_payments(cursor, from_date, to_date, invoice_ids, invoice_set, payment_hash)

    # 3) Sales Order nominal 0
    q3 = f"""
        SELECT DISTINCT so.name AS so_name
        FROM `tabSales Order Item` soi
        JOIN `tabSales Order` so ON so.name = soi.parent
        WHERE so.docstatus = 1
          AND so.transaction_date BETWEEN %s AND %s
          AND so.company = %s
          AND so.advance_paid = so.rounded_total
          AND so.rounded_total = 0
          {common_filters}
          {item_group_filter}
          {b_cond}
        ORDER BY so.name
    """
    params = [from_date, to_date, company] + common_params + item_group_params + b_params
    cursor.execute(q3, params)
    for row in cursor:
        so_name = row["so_name"]
        if so_name not in invoice_set:
            invoice_set.add(so_name)
            invoice_ids.append(so_name)

    return invoice_ids, payment_hash


def collect_payments(cursor, from_date, to_date, invoice_ids, invoice_set, payment_hash):
    for pr in cursor:
        so_name = pr["so_name"]
        pe_date = pr["pe_posting_date"]

        if so_name not in invoice_set and from_date <= pe_date <= to_date:
            invoice_set.add(so_name)
            invoice_ids.append(so_name)

        h = payment_hash.get(so_name)
        if h is None:
            h = {
                "payment_ids": set(),
                "convert_payment": 0.0,
                "advance_paid": 0.0,
                "used_convert": 0.0,
                "used_non_convert": 0.0,
                "posting_dates": [],
            }
            payment_hash[so_name] = h

        pe_name = pr["pe_name"]
        if pe_name not in h["payment_ids"]:
            h["payment_ids"].add(pe_name)
            paid = float(pr["pe_paid_amount"] or 0)
            h["advance_paid"] += paid
            h["posting_dates"].append(pe_date)
            if "Convert" in (pr["pe_mode_of_payment"] or ""):
                h["convert_payment"] += paid
                h["used_convert"] += paid
            else:
                h["used_non_convert"] += paid


def get_customer_details(cursor, company):
    cursor.execute("""
        SELECT name, customer_name, customer_group, customer_number, business
        FROM `tabCustomer`
        WHERE company = %s
    """, (company,))
    details = {}
    for d in cursor:
        details.setdefault(d["name"], {
            "customer_name": d["customer_name"],
            "customer_group": d["customer_group"],
            "customer_number": d["customer_number"],
            "business": apply_default_business(company, d["business"]),
        })
    return details


def get_treatment_vouchers(cursor, invoice_ids, batch_size):
    vouchers = {}
    for start in range(0, len(invoice_ids), batch_size):
        batch = invoice_ids[start:start + batch_size]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(f"""
            SELECT treatment_voucher, event_name, parent
            FROM `tabInvoice Treatment Voucher`
            WHERE parent IN ({placeholders})
        """, batch)
        for d in cursor:
            vouchers.setdefault(d["parent"], (d["treatment_voucher"], d["event_name"]))
    return vouchers


def iter_sales_order_details(cursor, invoice_ids, batch_size):
    for start in range(0, len(invoice_ids), batch_size):
        batch = invoice_ids[start:start + batch_size]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(f"""
            SELECT
                so_item.item_code, so_item.item_name, so_item.item_group,
                so_item.description, so_item.qty, so_item.uom,
                so_item.net_rate, so_item.net_amount,
                so.name AS so_name, so.transaction_date, so.customer, so.territory,
                so.project, so_item.delivered_qty, so_item.billed_amt, so.company,
                so.order_type, so.client_type, so.selling_price_list, so.business,
                so.doctor_name, so_item.pricing_rules, so.rounded_total,
                st.sales_person, e.employee_name, so_item.price_list_rate,
                i.retail_tax
            FROM `tabSales Order` so
            JOIN `tabSales Order Item` so_item ON so.name = so_item.parent
            LEFT JOIN `tabSales Team` st ON so.name = st.parent
            LEFT JOIN `tabSales Person` sp ON st.sales_person = sp.name
            LEFT JOIN `tabEmployee` e ON sp.employee = e.name
            LEFT JOIN `tabItem` i ON i.name = so_item.item_code
            WHERE so.nta_to_dei_migration_so_ref IS NULL
              AND so.name IN ({placeholders})
            ORDER BY so.name, so_item.net_amount
        """, batch)
        for row in cursor:
            yield row


def build_fact_rows(detail_rows, payment_hash, customers, vouchers, filters):
    item_group_filter = filters.get("item_group")

    for record in detail_rows:
        if item_group_filter and record["item_group"] != item_group_filter:
            continue

        so_name = record["so_name"]
        rounded_total = float(record["rounded_total"] or 0)

        h = payment_hash.get(so_name)
        completion_payment_status = (
            rounded_total == 0
            or (h is not None and h["advance_paid"] == rounded_total)
        )
        if not completion_payment_status:
            continue

        collection = float(record["net_amount"] or 0)
        if h is not None and h["convert_payment"] != 0:
            net_amount = float(record["net_amount"] or 0)
            if h["used_convert"] >= net_amount:
                collection = 0
                h["used_convert"] -= net_amount
            elif h["used_non_convert"] >= net_amount:
                collection = net_amount
                h["used_non_convert"] -= net_amount
            else:
                collection = h["used_non_convert"]

        record_business = apply_default_business(filters["company"], record["business"])
        customer_record = customers.get(record["customer"])
        if customer_record is None:
            continue
        if customer_record["business"] != record_business:
            continue

        pricing_rules = ""
        if record["pricing_rules"]:
            try:
                parsed = json.loads(record["pricing_rules"])
                pricing_rules = ", ".join(parsed)
            except Exception:
                pricing_rules = record["pricing_rules"]

        retail_tax = record.get("retail_tax") or 0
        net_rate = float(record["net_rate"] or 0)
        dpp_retail = net_rate * (100 / 111) if retail_tax else 0
        ppn_retail = dpp_retail * 11 / 100

        invoice_voucher = vouchers.get(so_name)

        posting_dates = h["posting_dates"] if h else []
        posting_date = min(posting_dates) if posting_dates else None

        yield {
            "transaction_date": record["transaction_date"],
            "sales_order": so_name,
            "item_code": record["item_code"],
            "item_name": record["item_name"],
            "item_group": record["item_group"],
            "description": record["description"],
            "quantity": record["qty"],
            "uom": record["uom"],
            "net_rate": record["net_rate"],
            "net_amount": record["net_amount"],
            "customer": record["customer"],
            "customer_name": customer_record["customer_name"],
            "customer_group": customer_record["customer_group"],
            "customer_number": customer_record["customer_number"],
            "client_type": record["client_type"],
            "sales_consultant": record["sales_person"],
            "employee_name": record["employee_name"],
            "doctor_name": record["doctor_name"],
            "price_list": record["selling_price_list"],
            "pricing_rule": pricing_rules,
            "order_type": record["order_type"],
            "price_list_rate": record["price_list_rate"],
            "territory": record["territory"],
            "project": record["project"],
            "delivered_quantity": record["delivered_qty"],
            "billed_amount": record["billed_amt"],
            "company": record["company"],
            "dpp_retail": dpp_retail,
            "ppn_retail": ppn_retail,
            "collection": collection,
            "treatment_voucher": invoice_voucher[0] if invoice_voucher else None,
            "event_name": invoice_voucher[1] if invoice_voucher else None,
            "posting_date": posting_date,
        }


# ──────────────────────────────────────────────
#  Parquet / BigQuery
# ──────────────────────────────────────────────
def normalize_batch(rows, extracted_at):
    df = pd.DataFrame(rows)

    for column in DATE_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce").dt.date

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")

    for column in df.columns:
        if column in DATE_COLUMNS or column in NUMERIC_COLUMNS or column == "_extracted_at":
            continue
        df[column] = df[column].map(
            lambda value: None if value is None or pd.isna(value) else str(value)
        )

    df["_extracted_at"] = pd.Timestamp(extracted_at)
    return df


def write_buffer(writer, rows_buffer, extracted_at):
    df = normalize_batch(rows_buffer, extracted_at)
    writer.write_table(pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False))
    return len(df)


def extract_to_parquet(args, base_filters, companies):
    extracted_at = datetime.now(timezone.utc)
    execution_date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = f"item_wise_sales_{execution_date}.parquet"

    connection = connect()
    writer = pq.ParquetWriter(file_path, ARROW_SCHEMA, compression="snappy")
    total_rows = 0
    total_net = 0.0
    total_collection = 0.0
    rows_buffer = []

    try:
        with connection.cursor() as cursor:
            print(f"Report window: {base_filters['from_date']} s/d {base_filters['to_date']} "
                  f"(lookback {base_filters['lookback_days']} hari)")
            print(f"Companies: {', '.join(companies)}")

            for company in companies:
                filters = dict(base_filters)
                filters["company"] = company
                if company not in BUSINESS_COMPANIES:
                    filters["business"] = None

                print(f"\n=== {company} ===")
                invoice_ids, payment_hash = get_sales_order_payment(cursor, filters)
                print(f"SO fully paid dalam window: {len(invoice_ids)}")

                if not invoice_ids:
                    print(f"  (tidak ada data untuk {company})")
                    continue

                customers = get_customer_details(cursor, company)
                vouchers = get_treatment_vouchers(cursor, invoice_ids, args.batch_size)
                print(f"Customer: {len(customers)} | Voucher: {len(vouchers)}")

                company_rows = 0
                company_net = 0.0
                company_collection = 0.0

                detail_rows = iter_sales_order_details(cursor, invoice_ids, args.batch_size)
                for row in build_fact_rows(detail_rows, payment_hash, customers, vouchers, filters):
                    rows_buffer.append(row)
                    company_rows += 1
                    company_net += float(row["net_amount"] or 0)
                    company_collection += float(row["collection"] or 0)

                    if len(rows_buffer) >= 10000:
                        total_rows += write_buffer(writer, rows_buffer, extracted_at)
                        rows_buffer = []
                        print(f"  Extracted {total_rows} rows")

                total_net += company_net
                total_collection += company_collection
                print(f"  {company}: {company_rows} rows | "
                      f"net={company_net:,.2f} | collection={company_collection:,.2f}")

            if rows_buffer:
                total_rows += write_buffer(writer, rows_buffer, extracted_at)
                print(f"Extracted {total_rows} rows")
    finally:
        connection.close()
        writer.close()

    if total_rows == 0:
        raise ValueError("Tidak ada data pada window/filter tersebut")

    print(f"\nParquet saved: {file_path} ({total_rows} rows)")
    print(f"Total: net={total_net:,.2f} collection={total_collection:,.2f}")
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
    from_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    to_date = (datetime.strptime(args.end, "%Y-%m-%d").date()
               if args.end else datetime.now(SOURCE_TIMEZONE).date())

    if from_date > to_date:
        raise SystemExit("from_date tidak boleh lebih besar dari to_date")

    filters = {
        "from_date": from_date,
        "to_date": to_date,
        "business": args.business,
        "territory": args.territory,
        "item_group": args.item_group,
        "lookback_days": args.lookback_days,
    }
    companies = resolve_companies(args)

    file_path, rows = extract_to_parquet(args, filters, companies)

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
