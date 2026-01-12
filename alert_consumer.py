from kafka import KafkaConsumer
import json
from hdfs import InsecureClient
import os

# Kafka and HDFS config
KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"  # Docker service name
HDFS_DIR = "/user/jovyan/alerts"
LOCAL_TMP = "/tmp/alerts_tmp.json"

# HDFS client (assuming default webhdfs port 9870 on NameNode)
HDFS_CLIENT = InsecureClient("http://namenode:9870", user="root")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    os.makedirs("/tmp", exist_ok=True)
    print("Alert consumer started...")

    for msg in consumer:
        record = msg.value
        if record.get("high_wind_alert"):
            print("⚠️ High wind alert:", record)

            # Append locally
            with open(LOCAL_TMP, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Push batch to HDFS
            HDFS_CLIENT.makedirs(HDFS_DIR)
            HDFS_CLIENT.upload(f"{HDFS_DIR}/alerts.json", LOCAL_TMP, overwrite=True)

if __name__ == "__main__":
    main()
