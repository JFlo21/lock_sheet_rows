import requests
import datetime
import pytz
import csv
import os
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file if it exists
env_file = Path(__file__).parent / '.env'
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

# ========== CONFIGURATION ==========
API_TOKEN = os.getenv('SMARTSHEET_API_TOKEN', 'YOUR_SMARTSHEET_API_TOKEN_HERE')  # Gets from env var or fallback
BATCH_SIZE = 25  # Reduced batch size for better reliability
MAX_WORKERS = 2  # Reduced concurrency to avoid rate limiting
REQUEST_TIMEOUT = 60  # Increased timeout for large operations
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed requests
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

# ========== HELPER FUNCTIONS ==========
def batch_lock_rows(sheet_id, rows_to_lock, sheet_name):
    """Lock multiple rows in a single API call with retry logic"""
    if not rows_to_lock:
        return []
    
    lock_url = f"{BASE_URL}/sheets/{sheet_id}/rows"
    lock_body = [{"id": row_id, "locked": True} for row_id in rows_to_lock]
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            lock_resp = requests.put(lock_url, headers=HEADERS, json=lock_body, timeout=REQUEST_TIMEOUT)
            if lock_resp.status_code in [200, 202]:
                print(f"✓ Locked {len(rows_to_lock)} rows in batch for {sheet_name}")
                return rows_to_lock
            else:
                print(f"✗ Failed to lock batch in {sheet_name} (attempt {attempt + 1}): {lock_resp.text}")
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error locking batch in {sheet_name} (attempt {attempt + 1}): {e}")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    
    return []

def get_sheet_data(sheet_id):
    """Get sheet data with optimized parameters"""
    # Get only essential data to reduce payload size
    sheet_url = f"{BASE_URL}/sheets/{sheet_id}?include=columnType&exclude=nonexistentCells,filteredOutRows"
    try:
        resp = requests.get(sheet_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print(f"✗ Failed to fetch sheet {sheet_id}: {resp.text}")
            return None
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error fetching sheet {sheet_id}: {e}")
        return None

def process_sheet_rows(sheet_id, sheet_name, week_ending_col_id, this_sunday_date):
    """Process all rows for a single sheet with batch locking"""
    print(f"\n📋 Processing sheet: {sheet_name}")
    start_time = time.time()
    
    # Get all rows in one go with correct API endpoint
    rows_url = f"{BASE_URL}/sheets/{sheet_id}"
    try:
        resp = requests.get(rows_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print(f"✗ Failed to fetch rows for {sheet_name}: {resp.text}")
            return []
        
        sheet_data = resp.json()
        all_rows = sheet_data.get('rows', [])
        print(f"📊 Found {len(all_rows)} rows in {sheet_name}")
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error fetching rows for {sheet_name}: {e}")
        return []
    
    # Process rows and identify which ones need locking
    rows_to_lock = []
    rows_metadata = {}
    
    for row in all_rows:
        row_id = row['id']
        row_number = row.get('rowNumber', '')
        locked = row.get('locked', False)
        
        if locked:  # Skip already locked rows
            continue
            
        week_ending_date = None
        for cell in row.get('cells', []):
            if cell.get('columnId') == week_ending_col_id:
                week_ending_date = cell.get('displayValue') or cell.get('value')
                break
        
        if not week_ending_date:
            continue
        
        # Parse date with multiple format support
        parsed_date = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                parsed_date = datetime.datetime.strptime(str(week_ending_date), fmt)
                break
            except ValueError:
                continue
        
        if not parsed_date:
            continue
        
        # Check if row should be locked
        if parsed_date.date() <= this_sunday_date:
            rows_to_lock.append(row_id)
            rows_metadata[row_id] = {
                "row_number": row_number,
                "week_ending_date": week_ending_date
            }
    
    print(f"🔒 Found {len(rows_to_lock)} rows to lock in {sheet_name}")
    
    if not rows_to_lock:
        elapsed = time.time() - start_time
        print(f"⏱️  Completed {sheet_name} in {elapsed:.1f}s - No rows to lock")
        return []
    
    # Lock rows in batches
    locked_rows = []
    total_batches = (len(rows_to_lock) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(rows_to_lock), BATCH_SIZE):
        batch = rows_to_lock[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} rows) for {sheet_name}")
        
        successfully_locked = batch_lock_rows(sheet_id, batch, sheet_name)
        
        # Add metadata for successfully locked rows
        for row_id in successfully_locked:
            if row_id in rows_metadata:
                locked_rows.append({
                    "Sheet Name": sheet_name,
                    "Row ID": row_id,
                    "Row Number": rows_metadata[row_id]["row_number"],
                    "Weekly Reference Logged Date": rows_metadata[row_id]["week_ending_date"]
                })
        
        # Small delay between batches to avoid rate limiting
        if i + BATCH_SIZE < len(rows_to_lock):
            time.sleep(0.5)  # Increased delay for better reliability
    
    elapsed = time.time() - start_time
    print(f"⏱️  Completed {sheet_name} in {elapsed:.1f}s - Locked {len(locked_rows)} rows")
    return locked_rows

# ========== MAIN PROCESSING ==========
def main():
    print(f"🚀 Starting Smartsheet row locking process...")
    print(f"📅 Target date: {this_sunday_str} (this Sunday)")
    print(f"🔧 Batch size: {BATCH_SIZE} rows per batch")
    print(f"⚡ Max concurrent workers: {MAX_WORKERS}")
    
    total_start_time = time.time()
    all_log_rows = []
    
    # Prepare sheet processing tasks
    sheet_tasks = []
    for sheet_id in SHEET_IDS:
        # Get sheet metadata first
        sheet_data = get_sheet_data(sheet_id)
        if not sheet_data:
            continue
            
        sheet_name = sheet_data['name']
        
        # Find column ID for the date column
        week_ending_col_id = None
        for col in sheet_data['columns']:
            if col['title'] == WEEK_ENDING_COL_NAME:
                week_ending_col_id = col['id']
                break
                
        if not week_ending_col_id:
            print(f"⚠️  Column '{WEEK_ENDING_COL_NAME}' not found in sheet {sheet_name}")
            continue
            
        sheet_tasks.append((sheet_id, sheet_name, week_ending_col_id))
    
    print(f"📊 Processing {len(sheet_tasks)} sheets...")
    
    # Process sheets with controlled concurrency
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all sheet processing tasks
        future_to_sheet = {
            executor.submit(process_sheet_rows, sheet_id, sheet_name, week_ending_col_id, this_sunday.date()): 
            sheet_name for sheet_id, sheet_name, week_ending_col_id in sheet_tasks
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_sheet):
            sheet_name = future_to_sheet[future]
            try:
                locked_rows = future.result()
                all_log_rows.extend(locked_rows)
            except Exception as e:
                print(f"✗ Error processing {sheet_name}: {e}")
    
    # ========== SAVE LOG TO CSV ==========
    total_elapsed = time.time() - total_start_time
    
    if all_log_rows:
        with open(CSV_LOG, 'w', newline='') as csvfile:
            fieldnames = ["Sheet Name", "Row ID", "Row Number", "Weekly Reference Logged Date"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in all_log_rows:
                writer.writerow(row)
        print(f"\n✅ Successfully locked {len(all_log_rows)} rows across all sheets")
        print(f"📄 Log saved to {CSV_LOG}")
    else:
        print(f"\nℹ️  No rows were locked across all sheets")
    
    print(f"⏱️  Total execution time: {total_elapsed:.1f} seconds")

if __name__ == "__main__":
    main()