from datetime import datetime, timedelta
import json
import os
import pandas as pd
import requests
import time
from google.cloud import bigquery

url = "https://api.cekat.ai/api/messages"

API_KEY = os.getenv("CEKAT_API_KEY")

headers = {"Authorization": f"Bearer {API_KEY}"}

# Atur rentang tanggal sesuai kebutuhan Anda (format: YYYY-MM-DD)
today_dt = datetime.utcnow()
start_date_str = "2026-07-01"
end_date_str = "2026-07-31"

all_messages = []
current_page = 1
limit_per_page = 300

PROD_TABLE_ID = "euromedica-495509.database.full_conversation_skin_slim"

print(
    f"📌 Menarik data pesan dari tanggal {start_date_str} s/d"
    f" {end_date_str}..."
)

while True:
  params = {
      "start_date": start_date_str,
      "end_date": end_date_str,
      "page": current_page,
      "limit": limit_per_page,
  }

  try:
    response = requests.get(url, headers=headers, params=params, timeout=30)

    if response.status_code == 200:
      res_json = response.json()
      items = res_json.get("data", [])

      if not items:
        print("✅ Semua halaman data berhasil ditarik.")
        break

      for item in items:
        # Ambil phone_number dari nested contact jika ada
        contact_info = item.get("contact")
        if isinstance(contact_info, dict):
          item["phone_number"] = contact_info.get("phone_number")
        else:
          item["phone_number"] = None

      all_messages.extend(items)
      print(f"📄 Page {current_page}: Berhasil menarik {len(items)} baris data.")

      # Jika jumlah item kurang dari limit, berarti ini halaman terakhir
      if len(items) < limit_per_page:
        print("✅ Halaman terakhir tercapai.")
        break

      current_page += 1
      time.sleep(1.5)  # Jeda sejenak agar tidak terkena rate limit

    else:
      print(
          f"❌ Error {response.status_code} pada page {current_page} -"
          f" {response.text}"
      )
      break

  except Exception as e:
    print(f"❌ Exception pada page {current_page}: {e}")
    break


# Gabungkan semua hasil ke dalam DataFrame akhir
df_messages = pd.DataFrame(all_messages)

# Convert all DataFrame columns to STRING

def convert_to_string(x):
    if x is None:
        return None

    # Convert lists/dictionaries to JSON string
    if isinstance(x, (list, dict)):
        return json.dumps(x, ensure_ascii=False)

    # Convert NaN / NaT to None
    if pd.isna(x):
        return None

    return str(x)


df_upload = df_messages[["id","conversation_id","created_at","updated_at","phone_number","chat_credits_used","sent_by_name","sent_by_type","message","status","inbox","ads_data"]]

for col in df_upload.columns:
    df_upload[col] = df_upload[col].apply(convert_to_string)

# Upload to BigQuery

client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)

job = client.load_table_from_dataframe(
    df_upload,
    PROD_TABLE_ID,
    job_config=job_config
)

job.result()

print(
    f"✅ Successfully inserted {job.output_rows} rows "
    f"into {PROD_TABLE_ID}"
)
