import argparse
import json
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pendulum
import pymysql
from sqlalchemy import bindparam, create_engine, text
from google.cloud import bigquery

# --- KONFIGURASI MARIADB ---
DB_USER = os.getenv("MYSQL_USER", "")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_HOST = os.getenv("MYSQL_HOST", "100.87.194.79")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_NAME = os.getenv("MYSQL_DATABASE", "")

INVOICE_BATCH_SIZE = int(os.getenv("INVOICE_BATCH_SIZE", "1000"))
JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")

DEFAULT_TABLE_ID = "euromedica-495509.database.item_wise_sales"
STAGING_TABLE_ID = "euromedica-495509.database.item_wise_sales_staging"
TABLE_EXPIRATION = "2099-01-01 00:00:00+00"

COMPANIES = [
    "EUROHAIRLAB",
    "SKIN+ DEI",
    "EUROSKINLAB",
    "European Slimming Centre",
]

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def get_treatment_vouchers(conn, invoice_ids):
    treatment_vouchers_hash = {}
    if not invoice_ids:
        return treatment_vouchers_hash

    for start in range(0, len(invoice_ids), INVOICE_BATCH_SIZE):
        batch = invoice_ids[start:start + INVOICE_BATCH_SIZE]
        query = text("""
            SELECT treatment_voucher, event_name, parent
            FROM `tabInvoice Treatment Voucher`
            WHERE parent IN :invoice_ids
        """).bindparams(bindparam("invoice_ids", expanding=True))
        results = conn.execute(query, {"invoice_ids": batch}).mappings().all()
        for d in results:
            treatment_vouchers_hash[d['parent']] = (d['treatment_voucher'], d['event_name'])

    return treatment_vouchers_hash


def get_customer_details(conn, filters):
    query = text("""
        SELECT name, customer_name, customer_group, customer_number, business
        FROM `tabCustomer`
        WHERE company = :company
    """)
    results = conn.execute(query, {"company": filters.get('company')}).mappings().all()

    customer_details = {}
    for d in results:
        b_business = d.get('business') or filters.get('company')
        customer_details[d['name']] = {
            "customer_name": d['customer_name'],
            "customer_group": d['customer_group'],
            "customer_number": d['customer_number'],
            "business": b_business
        }
    return customer_details


def get_sales_order_details(conn, invoice_ids):
    if not invoice_ids:
        return []

    records = []
    for start in range(0, len(invoice_ids), INVOICE_BATCH_SIZE):
        batch = invoice_ids[start:start + INVOICE_BATCH_SIZE]
        query = text("""
            SELECT
                so_item.name as item_id,
                so_item.item_code, so_item.item_name, so_item.item_group,
                so_item.description, so_item.qty, so_item.uom,
                so_item.base_price_list_rate as net_rate, so_item.net_amount, so.name,
                so.transaction_date, so.customer, so.territory,
                so.project, so_item.delivered_qty,
                so_item.billed_amt, so.company, so.order_type,
                so.client_type, so.selling_price_list, so.business,
                so.doctor_name, so_item.pricing_rules, so.rounded_total,
                st.sales_person, e.employee_name, so_item.price_list_rate,
                i.retail_tax
            FROM
                `tabSales Order` so
            INNER JOIN `tabSales Order Item` so_item ON so.name = so_item.parent
            LEFT JOIN `tabSales Team` st ON so.name = st.parent
            LEFT JOIN `tabSales Person` sp ON st.sales_person = sp.name
            LEFT JOIN `tabEmployee` e ON sp.employee = e.name
            LEFT JOIN `tabItem` i ON i.name = so_item.item_code
            WHERE so.nta_to_dei_migration_so_ref IS NULL AND so.name IN :invoice_ids
            ORDER BY so.name, so_item.net_amount
        """).bindparams(bindparam("invoice_ids", expanding=True))
        records.extend([dict(row) for row in conn.execute(query, {"invoice_ids": batch}).mappings().all()])

    return records


def get_sales_order_payment(conn, filters):
    payment_hash = {}
    invoice_ids = []

    from_date_obj = datetime.strptime(filters['from_date'], '%Y-%m-%d') + timedelta(days=-30)
    from_date_str = from_date_obj.strftime('%Y-%m-%d')
    to_date_str = filters['to_date']
    f_company = filters['company']
    f_item_group = filters.get('item_group')
    f_territory = filters.get('territory')
    f_business = filters.get('business')

    # Q1: Payment Entry referenced langsung ke Sales Order
    query_1_str = """
        SELECT so.name as 'so_name',
               so.rounded_total as 'so_rounded_total',
               pe.posting_date as 'pe_posting_date',
               pe.paid_amount as 'pe_paid_amount',
               pe.mode_of_payment as 'pe_mode_of_payment',
               pe.name as 'pe_name'
        FROM `tabPayment Entry` pe
        JOIN `tabPayment Entry Reference` pee ON pee.parent = pe.name
        JOIN `tabSales Order` so ON so.name = pee.reference_name
        JOIN `tabSales Order Item` soi ON so.name = soi.parent
        WHERE so.docstatus = 1 AND
              pe.docstatus = 1 AND
              pe.payment_type = 'Receive' AND
              pe.posting_date BETWEEN :from_date AND :to_date AND
              pe.company = :company AND
              so.advance_paid = so.rounded_total
    """
    params = {"from_date": from_date_str, "to_date": to_date_str, "company": f_company}

    if f_item_group:
        query_1_str += " AND soi.item_group = :item_group"
        params['item_group'] = f_item_group
    if f_territory:
        query_1_str += " AND so.territory = :territory"
        params['territory'] = f_territory
    if f_business:
        query_1_str += " AND so.business = :business"
        params['business'] = f_business

    query_1_str += " ORDER BY so.name, pe.posting_date"
    payment_record = [dict(row) for row in conn.execute(text(query_1_str), params).mappings().all()]

    # Q2: Payment Entry referenced ke Sales Invoice
    query_2_str = """
        SELECT
            soi.name as 'item_id',
            so.name as 'so_name',
            so.rounded_total as 'so_rounded_total',
            pe.posting_date as 'pe_posting_date',
            pe.paid_amount as 'pe_paid_amount',
            pe.mode_of_payment as 'pe_mode_of_payment',
            pe.name as 'pe_name'
        FROM `tabPayment Entry` pe
        JOIN `tabPayment Entry Reference` pee ON pe.name = pee.parent
        JOIN `tabSales Invoice` si ON si.name = pee.reference_name
        JOIN `tabSales Invoice Item` sii ON sii.parent = si.name
        JOIN `tabSales Order` so ON so.name = sii.sales_order
        JOIN `tabSales Order Item` soi ON so.name = soi.parent AND soi.name = sii.so_detail
        WHERE so.docstatus = 1 AND
              si.docstatus = 1 AND
              pe.docstatus = 1 AND
              pe.payment_type = 'Receive' AND
              pe.posting_date BETWEEN :from_date AND :to_date AND
              pe.company = :company AND
              so.advance_paid = so.rounded_total AND
              substring_index(pe.remarks, ' Sales Order ', -1) = so.name
    """
    if f_item_group:
        query_2_str += " AND soi.item_group = :item_group"
    if f_territory:
        query_2_str += " AND so.territory = :territory"
    if f_business:
        query_2_str += " AND so.business = :business"

    query_2_str += " ORDER BY so.name, pe.posting_date"
    payment_record.extend([dict(row) for row in conn.execute(text(query_2_str), params).mappings().all()])

    filter_from_date_obj = datetime.strptime(filters['from_date'], "%Y-%m-%d").date()
    filter_to_date_obj = datetime.strptime(filters['to_date'], "%Y-%m-%d").date()

    for pr in payment_record:
        pe_date = pr['pe_posting_date']

        if pr['so_name'] not in invoice_ids:
            if filter_from_date_obj <= pe_date <= filter_to_date_obj:
                invoice_ids.append(pr['so_name'])

        if pr['so_name'] not in payment_hash:
            payment_hash[pr['so_name']] = {
                'payment_ids': [],
                'convert_payment': 0.0,
                'advance_paid': 0.0,
                'used_convert': 0.0,
                'used_non_convert': 0.0,
                'last_posting_date': None,
            }

        curr_hash = payment_hash[pr['so_name']]

        if pr['pe_name'] not in curr_hash['payment_ids']:
            curr_hash['payment_ids'].append(pr['pe_name'])
            paid_amt = float(pr['pe_paid_amount'] or 0.0)
            curr_hash['advance_paid'] += paid_amt
            if curr_hash['last_posting_date'] is None or pe_date > curr_hash['last_posting_date']:
                curr_hash['last_posting_date'] = pe_date

            if pr['pe_mode_of_payment'] and 'Convert' in pr['pe_mode_of_payment']:
                curr_hash['convert_payment'] += paid_amt
                curr_hash['used_convert'] += paid_amt
            else:
                curr_hash['used_non_convert'] += paid_amt

    # Q3: Sales Order nominal 0
    query_3_str = """
        SELECT DISTINCT so.name as 'so_name'
        FROM `tabSales Order` so
        JOIN `tabSales Order Item` soi ON so.name = soi.parent
        WHERE so.docstatus = 1 AND
              so.transaction_date BETWEEN :strict_from AND :to_date AND
              so.company = :company AND
              so.advance_paid = so.rounded_total AND
              so.rounded_total = 0
    """
    params_3 = {"strict_from": filters['from_date'], "to_date": to_date_str, "company": f_company}

    if f_item_group:
        query_3_str += " AND soi.item_group = :item_group"
        params_3['item_group'] = f_item_group
    if f_territory:
        query_3_str += " AND so.territory = :territory"
        params_3['territory'] = f_territory
    if f_business:
        query_3_str += " AND so.business = :business"
        params_3['business'] = f_business

    query_3_str += " ORDER BY so.name, so.transaction_date"

    so_zero_list = conn.execute(text(query_3_str), params_3).mappings().all()
    invoice_ids.extend([v['so_name'] for v in so_zero_list])

    return list(set(invoice_ids)), payment_hash


def get_data(conn, filters, invoice_ids, payment_hash):
    data = []
    if not invoice_ids:
        return data

    customer_details = get_customer_details(conn, filters)
    sales_order_records = get_sales_order_details(conn, invoice_ids)
    treatment_vouchers_hash = get_treatment_vouchers(conn, invoice_ids)

    for record in sales_order_records:
        if filters.get('item_group') and record['item_group'] != filters['item_group']:
            continue

        pricing_rules = ''
        b_business = record.get('business') or filters.get('company')

        if record.get('pricing_rules'):
            try:
                rules = json.loads(record['pricing_rules'])
                pricing_rules = ', '.join(rules) if isinstance(rules, list) else str(rules)
            except Exception:
                pricing_rules = record['pricing_rules']

        so_rounded_total = float(record.get('rounded_total') or 0.0)
        advance_paid = payment_hash.get(record['name'], {}).get('advance_paid', 0.0)

        completion_payment_status = (
            so_rounded_total == 0 or advance_paid == so_rounded_total
        )
        if not completion_payment_status:
            continue

        net_amount = float(record.get('net_amount') or 0.0)
        collection = net_amount

        p_hash = payment_hash.get(record['name'])
        if p_hash and p_hash.get('convert_payment', 0) != 0:
            convert_value = p_hash.get('used_convert', 0.0)
            if convert_value >= net_amount:
                collection = 0
                p_hash['used_convert'] -= net_amount
            else:
                if p_hash['used_non_convert'] >= net_amount:
                    collection = net_amount
                    p_hash['used_non_convert'] -= net_amount
                else:
                    collection = p_hash['used_non_convert']

        invoice_voucher = treatment_vouchers_hash.get(record['name'])
        net_rate = float(record.get('net_rate') or 0.0)
        dpp_retail = net_rate * (100 / 111) if record.get('retail_tax') else 0.0
        ppn_retail = dpp_retail * 11 / 100

        customer_record = customer_details.get(record['customer'])
        cust_business = customer_record.get('business') if customer_record else None

        if cust_business != b_business:
            continue

        row = {
            "item_id": record.get('item_id'),
            "item_code": record.get('item_code'),
            "item_name": record.get('item_name'),
            "item_group": record.get('item_group'),
            "description": record.get('description'),
            "quantity": record.get('qty'),
            "uom": record.get('uom'),
            "net_rate": net_rate,
            "net_amount": net_amount,
            "sales_order": record.get('name'),
            "transaction_date": record.get('transaction_date'),
            "posting_date": (payment_hash.get(record['name']) or {}).get('last_posting_date')
                            or record.get('transaction_date'),
            "customer": record.get('customer'),
            "customer_name": customer_record.get('customer_name') if customer_record else '',
            "customer_group": customer_record.get('customer_group') if customer_record else '',
            "customer_number": customer_record.get('customer_number') if customer_record else '',
            "client_type": record.get('client_type'),
            "sales_consultant": record.get('sales_person'),
            "employee_name": record.get('employee_name'),
            "doctor_name": record.get('doctor_name'),
            "price_list": record.get('selling_price_list'),
            "pricing_rule": pricing_rules,
            "order_type": record.get('order_type'),
            "price_list_rate": record.get('price_list_rate'),
            "territory": record.get('territory'),
            "project": record.get('project'),
            "delivered_quantity": record.get('delivered_qty'),
            "billed_amount": record.get('billed_amt'),
            "company": record.get('company'),
            "dpp_retail": dpp_retail,
            "ppn_retail": ppn_retail,
            "collection": collection,
            "treatment_voucher": invoice_voucher[0] if invoice_voucher else None,
            "event_name": invoice_voucher[1] if invoice_voucher else None
        }
        data.append(row)

    return data


def execute_extraction(filters, engine):
    if filters.get('from_date') > filters.get('to_date'):
        raise ValueError("From Date cannot be greater than To Date")

    with engine.connect() as conn:
        invoice_ids, payment_hash = get_sales_order_payment(conn, filters)
        data = get_data(conn, filters, invoice_ids, payment_hash)

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        int_columns = ['quantity', 'delivered_quantity']
        float_columns = ['net_rate', 'net_amount', 'price_list_rate', 'billed_amount',
                         'dpp_retail', 'ppn_retail', 'collection']

        int_columns = [col for col in int_columns if col in df.columns]
        float_columns = [col for col in float_columns if col in df.columns]

        df[int_columns] = df[int_columns].fillna(0).astype(int)
        df[float_columns] = df[float_columns].fillna(0.0).astype(float)
        df = df.replace({np.nan: None})

        return df


def extract_sales_df(extract_days=150, start_date=None, end_date=None):
    today = pendulum.now(JAKARTA_TZ).date()

    str_start = start_date or (today - timedelta(days=extract_days)).strftime('%Y-%m-%d')
    str_end = end_date or today.strftime('%Y-%m-%d')
    print(f"Rentang Ekstraksi: {str_start} s/d {str_end}")

    all_dfs = []
    for company in COMPANIES:
        filter_config = {
            "company": company,
            "from_date": str_start,
            "to_date": str_end,
            "type": "",
            "territory": "",
            "business": ""
        }
        print(f"Menarik data untuk {company}...")
        df_temp = execute_extraction(filter_config, engine)
        if not df_temp.empty:
            all_dfs.append(df_temp)

    if not all_dfs:
        print("Data kosong untuk seluruh company.")
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total data gabungan: {len(df)} baris.")

    df_transformed = pd.DataFrame({
        'transaction_date': df['transaction_date'],
        'posting_date': df['posting_date'],
        'invoice': df['sales_order'],
        'territory': df['territory'],
        'client_name': df['customer_name'],
        'client_number': df['customer_number'],
        'sales_consultant': df['sales_consultant'],
        'employee_name': df['employee_name'],
        'doctor_name': df['doctor_name'],
        'price_list': df['price_list'],
        'pricing_rule': df['pricing_rule'],
        'item_id': df['item_id'],
        'item_code': df['item_code'],
        'item_name': df['item_name'],
        'item_group': df['item_group'],
        'order_type': df['order_type'],
        'voucher_number': df['treatment_voucher'],
        'event_name': df['event_name'],
        'quantity': df['quantity'],
        'normal_rate': df['net_rate'],
        'rate': df['net_amount'],
        'ppn_retail': df['ppn_retail'],
        'dpp_retail': df['dpp_retail'],
        'amount': df['billed_amount'],
        'collection': df['collection']
    })

    df_transformed['transaction_date'] = pd.to_datetime(
        df_transformed['transaction_date'], errors='coerce'
    ).dt.date
    df_transformed['posting_date'] = pd.to_datetime(
        df_transformed['posting_date'], errors='coerce'
    ).dt.date

    df_transformed['pricing_rule'] = (
        df_transformed['pricing_rule']
        .astype(str)
        .str.replace(r'[\[\]"\'\n]', '', regex=True)
        .str.strip()
    )

    df_transformed["uniid"] = (
        df_transformed["transaction_date"].astype(str).str.strip()
        + df_transformed["invoice"].astype(str).str.strip()
        + df_transformed["item_id"].astype(str).str.strip()
    )
    df_transformed["updated_at"] = datetime.now(JAKARTA_TZ)

    string_columns = [
        'invoice', 'territory', 'client_name', 'client_number',
        'sales_consultant', 'employee_name', 'doctor_name', 'price_list',
        'pricing_rule', 'item_code', 'item_name', 'item_group', 'order_type',
        'voucher_number', 'event_name'
    ]
    for col in string_columns:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col].apply(
                lambda x: '' if pd.isna(x) or str(x).lower() in ('nan', 'none') else str(x)
            )

    df_transformed = df_transformed.replace({np.nan: None, pd.NaT: None})

    return df_transformed


def load_to_bigquery(df, table_id, staging_id):
    if df.empty:
        print("DataFrame kosong, tidak load ke BigQuery.")
        return

    temp_parquet = f"item_wise_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(temp_parquet, index=False)

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    with open(temp_parquet, "rb") as f:
        job = client.load_table_from_file(f, staging_id, job_config=job_config)
    job.result()
    print(f"Loaded staging {staging_id}: {job.output_rows} rows")

    client.query(f"CREATE OR REPLACE TABLE `{table_id}` AS SELECT * FROM `{staging_id}`").result()
    client.query(
        f"ALTER TABLE `{table_id}` SET OPTIONS (expiration_timestamp = TIMESTAMP '{TABLE_EXPIRATION}')"
    ).result()
    print(f"Tabel target diganti: {table_id}")

    try:
        client.query(f"TRUNCATE TABLE `{staging_id}`").result()
        print(f"Staging dikosongkan: {staging_id}")
    except Exception as exc:
        print(f"Warning: gagal cleanup staging: {exc}")

    try:
        os.remove(temp_parquet)
    except OSError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Item-wise Sales ETL (production, MariaDB)")
    parser.add_argument("--start", default=os.getenv("START_DATE", "2022-01-01"),
                        help="from_date (YYYY-MM-DD), kosong = extract-days")
    parser.add_argument("--end", default=os.getenv("END_DATE", ""),
                        help="to_date (YYYY-MM-DD), kosong = hari ini WIB")
    parser.add_argument("--extract-days", type=int, default=150,
                        help="Dipakai jika --start kosong")
    parser.add_argument("--table", default=os.getenv("BQ_TABLE", DEFAULT_TABLE_ID))
    parser.add_argument("--staging", default=os.getenv("BQ_STAGING_TABLE", STAGING_TABLE_ID))
    parser.add_argument("--no-load", action="store_true")
    args = parser.parse_args()

    if not (DB_USER and DB_PASS and DB_NAME):
        raise SystemExit("Set env MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")

    t0 = time.time()
    df = extract_sales_df(
        extract_days=args.extract_days,
        start_date=args.start or None,
        end_date=args.end or None,
    )

    if df.empty:
        print("Tidak ada data.")
        return

    print(f"Rows: {len(df)} | columns: {len(df.columns)}")
    print(f"Sum rate: {df['rate'].sum():,.2f} | collection: {df['collection'].sum():,.2f} | amount: {df['amount'].sum():,.2f}")

    if not args.no_load:
        load_to_bigquery(df, args.table, args.staging)

    print(f"Selesai dalam {(time.time()-t0)/60:.1f} menit")


if __name__ == "__main__":
    main()
