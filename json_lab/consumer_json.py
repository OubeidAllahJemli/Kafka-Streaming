from kafka import KafkaConsumer
import json

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "transactions-json"
GROUP_ID = "json-consumer-group-v2"

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("Starting JSON consumer...\n")
    for message in consumer:
        print(f"Offset: {message.offset} | Partition: {message.partition} | Value: {message.value}")

if __name__ == "__main__":
    main()
