from datetime import datetime, timedelta
import pandas as pd
import requests
import time

url = "https://api.cekat.ai/api/messages"

API_KEY = os.getenv("CEKAT_API_KEY")

headers = {"Authorization": f"api_key {api_key}"}

# Atur rentang tanggal sesuai kebutuhan Anda (format: YYYY-MM-DD)
start_date_str = "2026-08-01"
end_date_str = "2026-08-01"

all_messages = []
current_page = 1
limit_per_page = 250

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
      time.sleep(1)  # Jeda sejenak agar tidak terkena rate limit

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

# Pilih kolom yang ingin ditampilkan jika data tidak kosong
if not df_messages.empty:
  columns_to_show = [
      "conversation_id",
      "created_at",
      "phone_number",
      "sent_by_name",
      "sent_by_type",
      "message",
      "status",
  ]
  available_cols = [c for c in columns_to_show if c in df_messages.columns]
  display(df_messages[available_cols])
else:
  print("ℹ️ Tidak ada data pesan ditemukan pada rentang tanggal tersebut.")

execution_date_str = today_dt.strftime('%Y%m%d')
file_path = f"cekat_full_conversations_{execution_date_str}.parquet"
df_messages.to_parquet(file_path, index=False)
    
print(f"✅ Cleaned data saved ke Parquet: {len(df)} rows")
