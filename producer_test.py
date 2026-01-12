from kafka import KafkaProducer
import json
import time

def main():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    for i in range(5):
        msg = {"msg": f"Hello Kafka {i}"}
        producer.send("weather_stream2", msg)
        print("Sent:", msg)
        time.sleep(1)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
