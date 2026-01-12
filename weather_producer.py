import requests
import json
import time
from kafka import KafkaProducer

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 52.52, 13.41  # Berlin

def fetch_weather():
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current_weather": "true"
    }
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("current_weather", {})

def main():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    while True:
        weather = fetch_weather()
        if weather:
            producer.send("weather_current", weather)
            print("Sent:", weather)
        time.sleep(60)  # poll once per minute

if __name__ == "__main__":
    main()
