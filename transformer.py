from kafka import KafkaConsumer, KafkaProducer
import json

def main():
    consumer = KafkaConsumer(
        "weather_current",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Transformer started...")
    for msg in consumer:
        record = msg.value
        if "temperature" in record:
            record["temp_f"] = record["temperature"] * 9/5 + 32
        if record.get("windspeed", 0) > 10:
            record["high_wind_alert"] = True
        else:
            record["high_wind_alert"] = False
        print("Transformed:", record)
        producer.send("weather_transformed", record)

if __name__ == "__main__":
    main()
