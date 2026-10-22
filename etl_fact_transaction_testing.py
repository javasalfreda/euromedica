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
from google.api_core.exceptions import NotFound

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
MAPPING_MAX_AGE_DAYS = 3
LATE_BUFFER_MONTHS = 1
REPORT_TIMEOUT = 300


def login():
    session = requests.Session()
    session.post(LOGIN_URL, data={"usr": USERNAME, "pwd": PASSWORD}).raise_for_status()
    print("Login berhasil")
    return session


def month_bounds(year, month):
    start = datetime(year, month, 1)
    end = start + relativedelta(months=1) - timedelta(days=1)
    return start, end


def parse_month(value):
    try:
        year, month = value.split("-")
        year, month = int(year), int(month)
        if not 1 <= month <= 12:
            raise ValueError
    except Exception:
        raise SystemExit(f"Format bulan salah: '{value}' (contoh: 2026-09)")
    return year, month


# ──────────────────────────────────────────────
#  EXTRACT: Sales History (report, per bulan)
# ──────────────────────────────────────────────
def extract_sales_history_window(session, from_date, to_date, retries=3):
    payload = {
        "report_name": REPORT_NAME,
        "filters": json.dumps({
            "company": COMPANY,
            "from_date": from_date,
            "to_date": to_date,
        }),
        "ignore_prepared_report": 1,
    }

    last_error = None
    for attempt in range(1, retries + 1):
        t0 = time.time()
        try:
            r = session.post(REPORT_URL, json=payload, timeout=REPORT_TIMEOUT)
            r.raise_for_status()
            result = r.json().get("message", {}).get("result", [])

            if not result or len(result) <= 1:
                print(f"  {from_date}..{to_date}: 0 rows ({time.time()-t0:.1f}s)")
                return pd.DataFrame()

            df = pd.DataFrame(result[:-1])
            print(f"  {from_date}..{to_date}: {len(df)} rows ({time.time()-t0:.1f}s)")
            return df
        except Exception as e:
            last_error = e
            print(f"  report gagal (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(f"Gagal ambil report {from_date}..{to_date}: {last_error}")


def extract_sales_history_month(session, year, month):
    from_date, month_end = month_bounds(year, month)

    today = datetime.now(timezone.utc).replace(tzinfo=None)
    if from_date > today:
        return pd.DataFrame()

    to_date = min(today, month_end + relativedelta(months=LATE_BUFFER_MONTHS))
    df = extract_sales_history_window(
        session,
        from_date.strftime("%Y-%m-%d"),
        to_date.strftime("%Y-%m-%d"),
    )

    if df.empty:
        return df

    if "transaction_date" in df.columns:
        td = pd.to_datetime(df["transaction_date"], errors="coerce")
        mask = (td >= pd.Timestamp(from_date)) & (td <= pd.Timestamp(month_end))
        outside = int((~mask).sum())
        df = df[mask].reset_index(drop=True)
        if outside:
            print(f"  filter transaction_date {from_date:%Y-%m-%d}..{month_end:%Y-%m-%d}: buang {outside} baris di luar bulan")

    return df


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

    try:
        client.delete_table(MAPPING_TABLE_ID, not_found_ok=True)
    except Exception:
        pass

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
#  Mapping otomatis: cache BQ (bila segar) -> else in-memory
# ──────────────────────────────────────────────
def load_mapping_auto(session, client, from_posting_date=None, max_age_days=MAPPING_MAX_AGE_DAYS):
    if client is not None:
        try:
            meta = client.query(
                f"SELECT last_modified_time FROM `{PROJECT}.database.__TABLES__` "
                f"WHERE table_id = 'dim_so_posting_date'"
            ).to_dataframe()
            if not meta.empty:
                ts = pd.Timestamp(meta.iloc[0, 0])
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                age_days = (pd.Timestamp.now(tz="UTC") - ts).total_seconds() / 86400
                if age_days <= max_age_days:
                    print(f"Mapping cache usia {age_days:.1f} hari (<= {max_age_days}) -> pakai BQ")
                    return load_mapping_from_bq(client)
                print(f"Mapping cache usia {age_days:.1f} hari (> {max_age_days}) -> build in-memory")
        except Exception as e:
            print(f"Mapping cache tidak terbaca ({e}) -> build in-memory")
    return build_mapping_inmemory(session, from_posting_date=from_posting_date)


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
#  TRANSFORM: merge mapping + normalisasi tipe
# ──────────────────────────────────────────────
def prepare_df(sales_df, mapping_df):
    result_df = sales_df.merge(mapping_df, on="sales_order", how="left")

    if "posting_date" in result_df.columns:
        matched = result_df["posting_date"].notna().sum()
        print(f"  matched posting_date: {matched}/{len(result_df)} ({matched/len(result_df)*100:.1f}%)")

    result_df["_extracted_at"] = datetime.now()

    if "transaction_date" in result_df.columns:
        result_df["transaction_date"] = pd.to_datetime(result_df["transaction_date"], errors="coerce").dt.date
    if "posting_date" in result_df.columns:
        result_df["posting_date"] = pd.to_datetime(result_df["posting_date"], errors="coerce").dt.date

    return result_df


# ──────────────────────────────────────────────
#  LOAD: per bulan (idempotent, partition transaction_date)
# ──────────────────────────────────────────────
def table_exists(client):
    try:
        client.get_table(TABLE_ID)
        return True
    except NotFound:
        return False


def delete_month(client, year, month):
    start, end = month_bounds(year, month)
    client.query(
        f"DELETE FROM `{TABLE_ID}` "
        f"WHERE transaction_date BETWEEN DATE '{start:%Y-%m-%d}' AND DATE '{end:%Y-%m-%d}'"
    ).result()


def count_month(client, year, month):
    start, end = month_bounds(year, month)
    rows = list(client.query(
        f"SELECT COUNT(*) AS c FROM `{TABLE_ID}` "
        f"WHERE transaction_date BETWEEN DATE '{start:%Y-%m-%d}' AND DATE '{end:%Y-%m-%d}'"
    ).result())
    return rows[0]["c"] if rows else 0


def load_month(client, file_path, year, month):
    exists = table_exists(client)
    if exists:
        delete_month(client, year, month)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(field="transaction_date"),
        )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, TABLE_ID, job_config=job_config)
    job.result()
    print(f"  loaded {job.output_rows} rows -> partisi {year}-{month:02d}")


# ──────────────────────────────────────────────
#  RUN: satu bulan
# ──────────────────────────────────────────────
def run_month(session, client, mapping_df, year, month, keep_parquet=False):
    month_str = f"{year}-{month:02d}"
    print(f"\n=== {month_str} ===")
    t0 = time.time()

    sales_df = extract_sales_history_month(session, year, month)

    if sales_df.empty:
        print(f"  {month_str}: tidak ada data")
        if client and table_exists(client):
            delete_month(client, year, month)
        return 0

    result_df = prepare_df(sales_df, mapping_df)

    file_path = f"fact_txn_testing_{month_str}.parquet"
    result_df.to_parquet(file_path, index=False)
    print(f"  parquet: {file_path} ({len(result_df)} rows, {len(result_df.columns)} cols)")

    if client:
        load_month(client, file_path, year, month)

    print(f"  {month_str}: selesai {time.time()-t0:.1f}s")

    if client and not keep_parquet:
        os.remove(file_path)

    return len(result_df)


# ──────────────────────────────────────────────
#  RUN: rentang bulan (backfill, resume-able)
# ──────────────────────────────────────────────
def run_range(session, client, start, end, force=False, map_max_age_days=MAPPING_MAX_AGE_DAYS):
    print(f"\n=== BACKFILL {start[0]}-{start[1]:02d} .. {end[0]}-{end[1]:02d} ===")

    mapping_df = load_mapping_auto(session, client, from_posting_date=None, max_age_days=map_max_age_days)

    today = datetime.now(timezone.utc).replace(tzinfo=None)
    month_start_today = datetime(today.year, today.month, 1)

    current = datetime(start[0], start[1], 1)
    last = datetime(end[0], end[1], 1)

    total_rows = 0
    processed = 0
    skipped = 0
    t_start = time.time()

    while current <= last:
        year, month = current.year, current.month
        month_start, _ = month_bounds(year, month)

        if month_start > today:
            break

        if client and not force and month_start < month_start_today:
            existing = count_month(client, year, month) if table_exists(client) else 0
            if existing:
                print(f"\n=== {year}-{month:02d} === skip ({existing} rows sudah ada)")
                skipped += 1
                current += relativedelta(months=1)
                continue

        total_rows += run_month(session, client, mapping_df, year, month)
        processed += 1

        elapsed = time.time() - t_start
        done = processed + skipped
        total_months = ((last.year - start[0]) * 12 + (last.month - start[1])) + 1
        eta = (elapsed / done) * (total_months - done) if done else 0
        print(f"  progress: {done}/{total_months} bulan | {total_rows} rows | {elapsed/60:.1f} menit | ETA {eta/60:.1f} menit")

        current += relativedelta(months=1)

    print(f"\nSelesai: {processed} bulan diproses, {skipped} dilewati, {total_rows} rows, {(time.time()-t_start)/60:.1f} menit")


# ──────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL fact_transaction_testing")
    parser.add_argument("--build-map", action="store_true",
                        help="Build mapping sales_order -> posting_date ke BQ (sekali jalan)")
    parser.add_argument("--month", type=str, help="ETL 1 bulan, format: YYYY-MM (contoh: 2026-09)")
    parser.add_argument("--range", nargs=2, metavar=("DARI", "SAMPAI"),
                        help="Backfill rentang bulan, contoh: --range 2022-01 2025-12")
    parser.add_argument("--all-months", action="store_true",
                        help=f"Backfill semua bulan dari {START_YEAR} sampai sekarang")
    parser.add_argument("--force", action="store_true",
                        help="Timpa bulan yang sudah ada (default: skip saat backfill)")
    parser.add_argument("--reset", action="store_true",
                        help="Hapus tabel target dulu (disarankan sekali sebelum backfill)")
    parser.add_argument("--map-max-age-days", type=int, default=MAPPING_MAX_AGE_DAYS,
                        help="Umur maksimal cache mapping (hari)")
    parser.add_argument("--no-load", action="store_true",
                        help="Hanya buat parquet lokal, tanpa load ke BQ")
    args = parser.parse_args()

    if not any([args.build_map, args.month, args.range, args.all_months]):
        print("Gunakan salah satu:")
        print("  --build-map                  Build mapping ke BQ (sekali jalan)")
        print("  --month 2026-01              ETL 1 bulan (replace bulan itu)")
        print("  --range 2022-01 2026-10      Backfill rentang bulan")
        print("  --all-months                 Backfill semua bulan sampai sekarang")
        sys.exit(1)

    t_start = time.time()

    if args.build_map:
        client = bigquery.Client()
        session = login()
        build_mapping(session, client)
    else:
        session = login()
        client = None
        if not args.no_load:
            try:
                client = bigquery.Client()
            except Exception as e:
                print(f"BQ credentials tidak tersedia: {e}")
                if not args.month:
                    sys.exit(1)
                print("Lanjut tanpa BQ: hasil hanya disimpan ke parquet lokal.")

        if args.reset:
            if client is None:
                sys.exit("--reset butuh kredensial BQ")
            client.delete_table(TABLE_ID, not_found_ok=True)
            print(f"Tabel dihapus: {TABLE_ID}")

        if args.month:
            year, month = parse_month(args.month)
            mapping_df = load_mapping_auto(
                session, client,
                from_posting_date=f"{year}-{month:02d}-01",
                max_age_days=args.map_max_age_days,
            )
            run_month(session, client, mapping_df, year, month)
        else:
            if args.all_months:
                start = (START_YEAR, 1)
                now = datetime.now(timezone.utc)
                end = (now.year, now.month)
            else:
                start = parse_month(args.range[0])
                end = parse_month(args.range[1])
            if start > end:
                sys.exit(f"Rentang tidak valid: {start} > {end}")
            run_range(session, client, start, end, force=args.force, map_max_age_days=args.map_max_age_days)

    print(f"\nSelesai dalam {(time.time() - t_start)/60:.1f} menit")
