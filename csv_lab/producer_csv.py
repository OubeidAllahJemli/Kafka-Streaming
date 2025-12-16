import csv
import time
import sys
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "first-csv-topic"

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/transactions_clean.csv"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8")
    )

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = ",".join(row.values())
            producer.send(TOPIC_NAME, value=message)
            print(f"Sent → {message}")
            time.sleep(0.1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
