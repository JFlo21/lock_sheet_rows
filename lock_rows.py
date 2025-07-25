import requests
import datetime
import pytz
import csv

# ========== CONFIGURATION ==========
API_TOKEN = 'YOUR_SMARTSHEET_API_TOKEN_HERE'  # <-- INSERT YOUR TOKEN HERE
SHEET_IDS = [
    "1964558450118532",
    "5905527830695812",
    "4126460034895748",
    "1732945426468740",
    "2230129632694148",
    "3239244454645636"
]
WEEK_ENDING_COL_NAME = "Weekly Reference Logged Date"
CENTRAL_TZ = pytz.timezone('America/Chicago')
CSV_LOG = 'locked_rows_log.csv'

# ========== API SETUP ==========
BASE_URL = 'https://api.smartsheet.com/2.0'
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# ========== DATE CALCULATION ==========
now = datetime.datetime.now(CENTRAL_TZ)
days_ahead = 6 - now.weekday()
if days_ahead < 0:
    days_ahead += 7
this_sunday = (now + datetime.timedelta(days=days_ahead)).replace(hour=23, minute=59, second=59, microsecond=0)
this_sunday_str = this_sunday.strftime("%Y-%m-%d")

# ========== LOGGING ==========
log_rows = []

for sheet_id in SHEET_IDS:
    # Get sheet info (for sheet name and columns)
    sheet_url = f"{BASE_URL}/sheets/{sheet_id}"
    resp = requests.get(sheet_url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Failed to fetch sheet {sheet_id}: {resp.text}")
        continue
    sheet = resp.json()
    sheet_name = sheet['name']

    # Find column ID for the date column
    week_ending_col_id = None
    for col in sheet['columns']:
        if col['title'] == WEEK_ENDING_COL_NAME:
            week_ending_col_id = col['id']
            break
    if not week_ending_col_id:
        print(f"Column '{WEEK_ENDING_COL_NAME}' not found in sheet {sheet_name}")
        continue

    # === PAGINATE THROUGH ALL ROWS ===
    all_rows = []
    page = 1
    page_size = 500
    total_rows = None
    while True:
        rows_url = f"{BASE_URL}/sheets/{sheet_id}?includeAll=false&pageSize={page_size}&page={page}"
        r = requests.get(rows_url, headers=HEADERS)
        if r.status_code != 200:
            print(f"Failed to fetch rows (page {page}) in {sheet_name}: {r.text}")
            break
        data = r.json()
        all_rows.extend(data.get('rows', []))
        total_rows = data.get('totalRowCount', None)
        if len(all_rows) >= (total_rows or 0):
            break
        if not data.get('rows'):
            break
        page += 1

    print(f"Found {len(all_rows)} rows in sheet {sheet_name}")

    # === PROCESS ALL ROWS ===
    for row in all_rows:
        row_id = row['id']
        row_number = row.get('rowNumber', '')
        locked = row.get('locked', False)
        week_ending_date = None

        for cell in row['cells']:
            if cell['columnId'] == week_ending_col_id:
                week_ending_date = cell.get('displayValue') or cell.get('value')
                break

        if not week_ending_date:
            continue

        # Parse date, allowing for multiple formats
        parsed_date = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                parsed_date = datetime.datetime.strptime(str(week_ending_date), fmt)
                break
            except Exception:
                continue
        if not parsed_date:
            print(f"Could not parse date: {week_ending_date} (row {row_number} in {sheet_name})")
            continue

        # Only lock if date <= this Sunday and not already locked
        if parsed_date.date() <= this_sunday.date() and not locked:
            # Lock the row
            lock_url = f"{BASE_URL}/sheets/{sheet_id}/rows"
            lock_body = [{
                "id": row_id,
                "locked": True
            }]
            lock_resp = requests.put(lock_url, headers=HEADERS, json=lock_body)
            if lock_resp.status_code in [200, 202]:
                print(f"Locked row {row_number} in {sheet_name}")
                log_rows.append({
                    "Sheet Name": sheet_name,
                    "Row ID": row_id,
                    "Row Number": row_number,
                    "Weekly Reference Logged Date": week_ending_date
                })
            else:
                print(f"Failed to lock row {row_number} in {sheet_name}: {lock_resp.text}")

# ========== SAVE LOG TO CSV ==========
if log_rows:
    with open(CSV_LOG, 'w', newline='') as csvfile:
        fieldnames = ["Sheet Name", "Row ID", "Row Number", "Weekly Reference Logged Date"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in log_rows:
            writer.writerow(row)
    print(f"\nLog saved to {CSV_LOG}")
else:
    print("\nNo rows were locked.")