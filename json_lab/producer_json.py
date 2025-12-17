import csv
import json
import time
import sys
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "transactions-json"

# Use CSV path from argument, default to old file
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/transactions_clean.csv"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                message = {
                    "transaction_id": int(row["transaction_id"]),
                    "customer_id": int(row["user_id"]),
                    "amount": float(row["amount"]),
                    "timestamp": row["timestamp"],
                    "currency": row.get("currency", "USD")
                }
                producer.send(TOPIC_NAME, value=message)
                print(f"Sent: {message}")
            except Exception as e:
                print(f"Skipping invalid row {row}: {e}")
            time.sleep(0.1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
