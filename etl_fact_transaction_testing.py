import os
import sys
import json
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

# --- KONFIGURASI API ---
BASE_URL = "https://erpeuromedicagroup.com"
LOGIN_URL = f"{BASE_URL}/api/method/login"
REPORT_URL = f"{BASE_URL}/api/method/frappe.desk.query_report.run"
REPORTVIEW_URL = f"{BASE_URL}/api/method/frappe.desk.reportview.get"
USERNAME = "rizka.dei@skinplusclinic.com"
PASSWORD = "090900"
REPORT_NAME = "Item-wise Sales History"
COMPANY = "SKIN+ DEI"

# --- KONFIGURASI ETL ---
PAGE_SIZE = 5000
START_YEAR = 2022
PROJECT = "euromedica-495509"
TABLE_ID = f"{PROJECT}.database.fact_transaction_testing"
MAPPING_TABLE_ID = f"{PROJECT}.database.dim_so_posting_date"


def login():
    session = requests.Session()
    session.post(LOGIN_URL, data={"usr": USERNAME, "pwd": PASSWORD}).raise_for_status()
    print("Login berhasil")
    return session


# ──────────────────────────────────────────────
#  EXTRACT: Sales History (report, per bulan)
# ──────────────────────────────────────────────
def extract_sales_history_month(session, year, month):
    from_date = f"{year}-{month:02d}-01"
    month_end = (datetime(year, month, 1) + relativedelta(months=1)) - timedelta(days=1)
    to_date = month_end.strftime("%Y-%m-%d")

    t0 = time.time()
    r = session.post(REPORT_URL, json={
        "report_name": REPORT_NAME,
        "filters": json.dumps({
            "company": COMPANY,
            "from_date": from_date,
            "to_date": to_date,
        }),
        "ignore_prepared_report": 1,
    }, timeout=120)
    r.raise_for_status()
    result = r.json().get("message", {}).get("result", [])

    if result and len(result) > 1:
        df = pd.DataFrame(result[:-1])
        print(f"  {from_date}: {len(df)} rows ({time.time()-t0:.1f}s)")
        return df

    print(f"  {from_date}: 0 rows ({time.time()-t0:.1f}s)")
    return pd.DataFrame()


# ──────────────────────────────────────────────
#  EXTRACT: Doctype (reportview paging)
# ──────────────────────────────────────────────
def extract_doctype(session, doctype, fields, filters=None, label=None):
    label = label or doctype
    all_rows = []
    start = 0
    page = 0

    while True:
        params = {
            "doctype": doctype,
            "fields": json.dumps(fields),
            "limit_start": str(start),
            "limit_page_length": str(PAGE_SIZE),
        }
        if filters:
            params["filters"] = json.dumps(filters)

        try:
            r = session.get(REPORTVIEW_URL, params=params, timeout=60)
            r.raise_for_status()
            msg = r.json().get("message", {})
            keys = msg.get("keys", [])
            values = msg.get("values", [])

            if not values:
                break

            all_rows.append(pd.DataFrame(values, columns=keys))
            start += PAGE_SIZE
            page += 1

            if page % 20 == 0 or len(values) < PAGE_SIZE:
                print(f"  {label}: {start} rows...")

            if len(values) < PAGE_SIZE:
                break

            time.sleep(0.1)
        except Exception as e:
            print(f"  {label}: ERROR at offset {start}: {e}")
            time.sleep(2)
            try:
                r = session.get(REPORTVIEW_URL, params=params, timeout=60)
                r.raise_for_status()
                msg = r.json().get("message", {})
                values = msg.get("values", [])
                if not values:
                    break
                all_rows.append(pd.DataFrame(values, columns=msg.get("keys", [])))
                start += PAGE_SIZE
                page += 1
                if len(values) < PAGE_SIZE:
                    break
            except Exception as e2:
                print(f"  {label}: RETRY FAILED: {e2}")
                break

    if all_rows:
        df = pd.concat(all_rows, ignore_index=True)
        print(f"  {label}: TOTAL {len(df)} rows")
        return df
    return pd.DataFrame()


# ──────────────────────────────────────────────
#  MODE 1: Build mapping -> BQ
# ──────────────────────────────────────────────
def build_mapping(session, client):
    print("\n=== [MODE] Build sales_order -> posting_date mapping ===")

    print("\nExtracting Payment Entry...")
    pe_df = extract_doctype(session, "Payment Entry",
        ["name", "posting_date", "paid_amount", "party_name", "mode_of_payment", "status", "company"],
        label="PE")

    print("\nExtracting Payment Entry Reference...")
    pe_ref_df = extract_doctype(session, "Payment Entry Reference",
        ["parent", "reference_doctype", "reference_name"],
        label="PE Ref")

    print("\nExtracting Sales Invoice Item (sales_order)...")
    si_item_df = extract_doctype(session, "Sales Invoice Item",
        ["parent", "sales_order"],
        filters=[["sales_order", "is", "set"]],
        label="SI Item")

    print("\nBuilding mapping...")

    pe_ref_so = pe_ref_df[pe_ref_df["reference_doctype"] == "Sales Order"][
        ["parent", "reference_name"]
    ].copy()
    pe_ref_so.columns = ["pe_name", "sales_order"]

    pe_ref_si = pe_ref_df[pe_ref_df["reference_doctype"] == "Sales Invoice"][
        ["parent", "reference_name"]
    ].copy()
    pe_ref_si.columns = ["pe_name", "si_name"]

    si_to_so = si_item_df[
        si_item_df["sales_order"].notna() & (si_item_df["sales_order"].astype(str).str.strip() != "")
    ][["parent", "sales_order"]].copy()
    si_to_so.columns = ["si_name", "sales_order"]
    si_to_so = si_to_so.drop_duplicates()
    print(f"  SI -> SO: {len(si_to_so)} unique SI")

    pe_ref_si_so = pe_ref_si.merge(si_to_so, on="si_name", how="inner")
    print(f"  PE Ref (SI -> SO): {len(pe_ref_si_so)} rows")

    so_pe = pd.concat([
        pe_ref_so[["pe_name", "sales_order"]],
        pe_ref_si_so[["pe_name", "sales_order"]],
    ], ignore_index=True)

    so_pe_posting = so_pe.merge(
        pe_df[["name", "posting_date"]],
        left_on="pe_name", right_on="name", how="inner",
    )
    print(f"  Joined with PE: {len(so_pe_posting)} rows")

    so_posting = (
        so_pe_posting
        .groupby("sales_order")["posting_date"]
        .min()
        .reset_index()
    )
    so_posting.columns = ["sales_order", "posting_date"]
    print(f"  Unique SO with posting_date: {len(so_posting)}")

    so_posting["posting_date"] = pd.to_datetime(so_posting["posting_date"])

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    client.load_table_from_dataframe(so_posting, MAPPING_TABLE_ID, job_config=job_config).result()
    print(f"\nMapping tersimpan -> {MAPPING_TABLE_ID}")


# ──────────────────────────────────────────────
#  MODE 2: Load mapping dari BQ
# ──────────────────────────────────────────────
def load_mapping_from_bq(client):
    query = f"SELECT sales_order, posting_date FROM `{MAPPING_TABLE_ID}`"
    df = client.query(query).to_dataframe()
    print(f"Mapping dimuat dari BQ: {len(df)} rows")
    return df


# ──────────────────────────────────────────────
#  Build mapping in-memory (untuk testing, tanpa BQ)
# ──────────────────────────────────────────────
def build_mapping_inmemory(session, from_posting_date=None):
    print("\n=== Build mapping in-memory ===")

    filters = None
    if from_posting_date:
        filters = [["posting_date", ">=", from_posting_date]]

    print("\nExtracting Payment Entry...")
    pe_df = extract_doctype(session, "Payment Entry",
        ["name", "posting_date"],
        filters=filters,
        label="PE")

    if pe_df.empty:
        return pd.DataFrame(columns=["sales_order", "posting_date"])

    print("\nExtracting Payment Entry Reference...")
    pe_ref_df = extract_doctype(session, "Payment Entry Reference",
        ["parent", "reference_doctype", "reference_name"],
        label="PE Ref")

    print("\nExtracting Sales Invoice Item (sales_order)...")
    si_item_df = extract_doctype(session, "Sales Invoice Item",
        ["parent", "sales_order"],
        filters=[["sales_order", "is", "set"]],
        label="SI Item")

    print("\nBuilding mapping...")

    pe_ref_so = pe_ref_df[pe_ref_df["reference_doctype"] == "Sales Order"][
        ["parent", "reference_name"]
    ].copy()
    pe_ref_so.columns = ["pe_name", "sales_order"]

    pe_ref_si = pe_ref_df[pe_ref_df["reference_doctype"] == "Sales Invoice"][
        ["parent", "reference_name"]
    ].copy()
    pe_ref_si.columns = ["pe_name", "si_name"]

    si_to_so = si_item_df[
        si_item_df["sales_order"].notna() & (si_item_df["sales_order"].astype(str).str.strip() != "")
    ][["parent", "sales_order"]].copy()
    si_to_so.columns = ["si_name", "sales_order"]
    si_to_so = si_to_so.drop_duplicates()
    print(f"  SI -> SO: {len(si_to_so)} unique SI")

    pe_ref_si_so = pe_ref_si.merge(si_to_so, on="si_name", how="inner")
    print(f"  PE Ref (SI -> SO): {len(pe_ref_si_so)} rows")

    so_pe = pd.concat([
        pe_ref_so[["pe_name", "sales_order"]],
        pe_ref_si_so[["pe_name", "sales_order"]],
    ], ignore_index=True)

    so_pe_posting = so_pe.merge(
        pe_df[["name", "posting_date"]],
        left_on="pe_name", right_on="name", how="inner",
    )
    print(f"  Joined with PE: {len(so_pe_posting)} rows")

    so_posting = (
        so_pe_posting
        .groupby("sales_order")["posting_date"]
        .min()
        .reset_index()
    )
    so_posting.columns = ["sales_order", "posting_date"]
    print(f"  Unique SO with posting_date: {len(so_posting)}")

    return so_posting


# ──────────────────────────────────────────────
#  MODE 3: Extract sales history per bulan -> merge -> parquet (+ optional BQ)
# ──────────────────────────────────────────────
def run_monthly(session, client, month_str):
    year, month = map(int, month_str.split("-"))
    print(f"\n=== [MODE] ETL bulan {month_str} ===")

    sales_df = extract_sales_history_month(session, year, month)
    if sales_df.empty:
        print("Tidak ada data.")
        return

    mapping_df = build_mapping_inmemory(session, from_posting_date=f"{year}-{month:02d}-01")

    result_df = sales_df.merge(mapping_df, on="sales_order", how="left")
    matched = result_df["posting_date"].notna().sum()
    print(f"\nMatched posting_date: {matched}/{len(result_df)} ({matched/len(result_df)*100:.1f}%)")

    result_df["_extracted_at"] = datetime.now()

    file_path = f"fact_txn_testing_{month_str}.parquet"
    result_df.to_parquet(file_path, index=False)
    print(f"Parquet tersimpan: {file_path} ({len(result_df)} rows, {len(result_df.columns)} cols)")

    if client:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, TABLE_ID, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows -> {TABLE_ID}")


def run_all_months(session, client):
    print(f"\n=== [MODE] ETL all months ({START_YEAR} -> sekarang) ===")
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    current = datetime(START_YEAR, 1, 1)
    all_dfs = []

    while current <= today:
        df = extract_sales_history_month(session, current.year, current.month)
        if not df.empty:
            all_dfs.append(df)
        month_end = (current + relativedelta(months=1)) - timedelta(days=1)
        current = month_end + timedelta(days=1)
        time.sleep(0.3)

    if not all_dfs:
        print("Tidak ada data.")
        return

    sales_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal: {len(sales_df)} rows")

    mapping_df = load_mapping_from_bq(client)
    result_df = sales_df.merge(mapping_df, on="sales_order", how="left")
    matched = result_df["posting_date"].notna().sum()
    print(f"Matched posting_date: {matched}/{len(result_df)} ({matched/len(result_df)*100:.1f}%)")

    result_df["_extracted_at"] = datetime.now()
    result_df["posting_date"] = result_df["posting_date"].dt.date

    file_path = "fact_txn_testing_all.parquet"
    result_df.to_parquet(file_path, index=False)
    print(f"Parquet: {file_path}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, TABLE_ID, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows -> {TABLE_ID}")
    os.remove(file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL fact_transaction_testing")
    parser.add_argument("--build-map", action="store_true",
                        help="Build mapping sales_order -> posting_date ke BQ (sekali jalan)")
    parser.add_argument("--month", type=str, help="ETL per bulan, format: YYYY-MM (contoh: 2026-01)")
    parser.add_argument("--all-months", action="store_true",
                        help="ETL semua bulan dari 2022 sampai sekarang")
    args = parser.parse_args()

    if not args.build_map and not args.month and not args.all_months:
        print("Gunakan salah satu:")
        print("  --build-map          Build mapping ke BQ (sekali jalan)")
        print("  --month 2026-01      ETL 1 bulan (cepat)")
        print("  --all-months         ETL semua bulan (backfill)")
        sys.exit(1)

    t_start = time.time()

    if args.build_map:
        client = bigquery.Client()
        session = login()
        build_mapping(session, client)
    elif args.month:
        session = login()
        client = None
        try:
            client = bigquery.Client()
        except Exception:
            print("BQ credentials tidak tersedia, hasil hanya disimpan ke parquet lokal.")
        run_monthly(session, client, args.month)
    elif args.all_months:
        client = bigquery.Client()
        session = login()
        run_all_months(session, client)

    elapsed = time.time() - t_start
    print(f"\nSelesai dalam {elapsed/60:.1f} menit")
