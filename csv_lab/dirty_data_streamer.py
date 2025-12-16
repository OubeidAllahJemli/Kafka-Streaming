import csv
import subprocess
from datetime import datetime

DIRTY_CSV = "/tmp/transactions_dirty.csv"
CLEAN_CSV = "/tmp/transactions_cleaned_dirty.csv"

def is_valid_int(value):
    try:
        int(value)
        return True
    except:
        return False

def is_valid_float(value):
    try:
        float(value)
        return True
    except:
        return False

def is_valid_timestamp(value):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            datetime.strptime(value, fmt)
            return True
        except:
            continue
    return False

def clean_dirty_data():
    seen = set()
    cleaned_rows = []

    with open(DIRTY_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_key = (row.get("transaction_id"), row.get("user_id"), row.get("amount"), row.get("timestamp"))
            if row_key in seen:
                continue
            seen.add(row_key)

            if not (is_valid_int(row.get("transaction_id")) and
                    is_valid_int(row.get("user_id")) and
                    is_valid_float(row.get("amount")) and
                    is_valid_timestamp(row.get("timestamp"))):
                continue

            cleaned_rows.append(row)

    with open(CLEAN_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["transaction_id","user_id","amount","timestamp"])
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f"Cleaned {len(cleaned_rows)} rows → {CLEAN_CSV}")
    return CLEAN_CSV

def stream_to_producers(clean_csv_path):
    # Stream to CSV producer
    subprocess.run(["python", "/tmp/producer_csv.py", clean_csv_path])
    # Stream to JSON producer
    subprocess.run(["python", "/tmp/producer_json.py", clean_csv_path])

if __name__ == "__main__":
    cleaned_file = clean_dirty_data()
    stream_to_producers(cleaned_file)
