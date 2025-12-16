from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "first-csv-topic"
GROUP_ID = "csv-consumer-group-v2"

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8")
    )

    print("Starting CSV consumer...\n")

    for message in consumer:
        print(
            f"Offset: {message.offset} | "
            f"Partition: {message.partition} | "
            f"Value: {message.value}"
        )

if __name__ == "__main__":
    main()