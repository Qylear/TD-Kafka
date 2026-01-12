from kafka import KafkaConsumer
import json

def main():
    consumer = KafkaConsumer(
        "weather_stream2",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    print("Consumer started...")
    for msg in consumer:
        print("Received:", msg.value)

if __name__ == "__main__":
    main()
