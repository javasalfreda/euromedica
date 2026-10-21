import os
import json
import time
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
TABLE_ID = "euromedica-495509.database.fact_transaction_testing"


def login():
    session = requests.Session()
    session.post(LOGIN_URL, data={"usr": USERNAME, "pwd": PASSWORD}).raise_for_status()
    print("✅ Login berhasil")
    return session


def extract_sales_history(session):
    all_dfs = []
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    current = datetime(START_YEAR, 1, 1)

    while current <= today:
        month_end = (current + relativedelta(months=1)) - timedelta(days=1)
        from_date = current.strftime("%Y-%m-%d")
        to_date = month_end.strftime("%Y-%m-%d")

        t0 = time.time()
        try:
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
                all_dfs.append(df)
                print(f"  {from_date} .. {to_date}: {len(df)} rows ({time.time()-t0:.1f}s)")
            else:
                print(f"  {from_date} .. {to_date}: 0 rows ({time.time()-t0:.1f}s)")
        except Exception as e:
            print(f"  {from_date} .. {to_date}: ERROR {e}")

        current = month_end + timedelta(days=1)
        time.sleep(0.3)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


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
            body = r.json()
            msg = body.get("message", {})
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


def build_posting_date_map(session):
    print("\n=== Extracting Payment Entry ===")
    pe_df = extract_doctype(session, "Payment Entry",
        ["name", "posting_date", "paid_amount", "party_name", "mode_of_payment", "status", "company"],
        label="PE")

    print("\n=== Extracting Payment Entry Reference ===")
    pe_ref_df = extract_doctype(session, "Payment Entry Reference",
        ["parent", "reference_doctype", "reference_name"],
        label="PE Ref")

    print("\n=== Extracting Sales Invoice Item (sales_order) ===")
    si_item_df = extract_doctype(session, "Sales Invoice Item",
        ["parent", "sales_order"],
        filters=[["sales_order", "is", "set"]],
        label="SI Item")

    print("\n=== Building sales_order -> posting_date mapping ===")

    pe_ref_so = pe_ref_df[pe_ref_df["reference_doctype"] == "Sales Order"][
        ["parent", "reference_name"]
    ].copy()
    pe_ref_so.columns = ["pe_name", "sales_order"]
    print(f"  PE Ref (Sales Order): {len(pe_ref_so)} rows")

    pe_ref_si = pe_ref_df[pe_ref_df["reference_doctype"] == "Sales Invoice"][
        ["parent", "reference_name"]
    ].copy()
    pe_ref_si.columns = ["pe_name", "si_name"]
    print(f"  PE Ref (Sales Invoice): {len(pe_ref_si)} rows")

    si_to_so = si_item_df[
        si_item_df["sales_order"].notna() & (si_item_df["sales_order"].astype(str).str.strip() != "")
    ][["parent", "sales_order"]].copy()
    si_to_so.columns = ["si_name", "sales_order"]
    si_to_so = si_to_so.drop_duplicates()
    print(f"  SI -> SO mapping: {len(si_to_so)} unique SI")

    pe_ref_si_so = pe_ref_si.merge(si_to_so, on="si_name", how="inner")
    print(f"  PE Ref (SI -> SO) joined: {len(pe_ref_si_so)} rows")

    so_pe = pd.concat([
        pe_ref_so[["pe_name", "sales_order"]],
        pe_ref_si_so[["pe_name", "sales_order"]],
    ], ignore_index=True)

    so_pe_posting = so_pe.merge(
        pe_df[["name", "posting_date"]],
        left_on="pe_name", right_on="name", how="inner",
    )
    print(f"  Joined with PE posting_date: {len(so_pe_posting)} rows")

    so_posting = (
        so_pe_posting
        .groupby("sales_order")["posting_date"]
        .min()
        .reset_index()
    )
    so_posting.columns = ["sales_order", "posting_date"]
    print(f"  Unique sales_orders with posting_date: {len(so_posting)}")

    return so_posting


def save_parquet(df, file_path):
    df["_extracted_at"] = datetime.now()
    df.to_parquet(file_path, index=False)
    print(f"\n✅ Parquet tersimpan: {file_path} ({len(df)} rows, {len(df.columns)} cols)")


def load_to_bq(file_path, client):
    if not file_path or not os.path.exists(file_path):
        print("❌ File parquet tidak ditemukan.")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, TABLE_ID, job_config=job_config)
    job.result()
    print(f"🚀 Loaded {job.output_rows} rows -> {TABLE_ID}")


if __name__ == "__main__":
    client = bigquery.Client()

    t_start = time.time()
    session = login()

    print("\n=== Extracting Item-wise Sales History (all time, per month) ===")
    sales_df = extract_sales_history(session)
    print(f"\nTotal sales history: {len(sales_df)} rows")

    so_posting = build_posting_date_map(session)

    print("\n=== Merging posting_date ===")
    result_df = sales_df.merge(so_posting, on="sales_order", how="left")
    matched = result_df["posting_date"].notna().sum()
    print(f"  Matched: {matched}/{len(result_df)} ({matched/len(result_df)*100:.1f}%)")

    file_path = f"fact_transaction_testing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    save_parquet(result_df, file_path)
    load_to_bq(file_path, client)

    elapsed = time.time() - t_start
    print(f"\n🎉 Selesai dalam {elapsed/60:.1f} menit")
