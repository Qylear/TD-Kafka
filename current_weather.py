import json
import sys
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


def create_producer(bootstrap_servers='localhost:9092'):
    """Créer un producteur Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        return producer
    except Exception as e:
        print(f" Erreur lors de la création du producteur: {e}")
        sys.exit(1)


def fetch_weather_data(latitude, longitude):
    """Récupérer les données météo depuis Open-Meteo API"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current': 'temperature_2m,windspeed_10m,weathercode,relative_humidity_2m,apparent_temperature',
        'timezone': 'auto'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Enrichir les données
        weather_data = {
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'timezone': data['timezone'],
            'elevation': data['elevation'],
            'current': data['current'],
            'timestamp': datetime.now().isoformat(),
            'fetched_at': data['current']['time']
        }
        
        return weather_data
    except requests.exceptions.RequestException as e:
        print(f" Erreur lors de la récupération des données météo: {e}")
        return None
    except KeyError as e:
        print(f" Erreur: Champ manquant dans la réponse API: {e}")
        return None


def send_to_kafka(producer, topic, data, key=None):
    """Envoyer les données dans Kafka"""
    try:
        future = producer.send(topic, value=data, key=key)
        record_metadata = future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f" Erreur lors de l'envoi vers Kafka: {e}")
        return False


def main():
    # Vérifier les arguments
    if len(sys.argv) < 3:
        print("Usage: python current_weather.py <latitude> <longitude> [interval_seconds]")
        print("Exemple: python current_weather.py 48.8566 2.3522 10")
        print("         (Paris: lat=48.8566, lon=2.3522, toutes les 10 secondes)")
        sys.exit(1)
    
    try:
        latitude = float(sys.argv[1])
        longitude = float(sys.argv[2])
        interval = int(sys.argv[3]) if len(sys.argv) > 3 else 30  # Par défaut: 30 secondes
    except ValueError:
        print(" Erreur: latitude et longitude doivent être des nombres")
        sys.exit(1)
    
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'weather_stream'
    
    print(f"  Démarrage du producteur météo")
    print(f"   Localisation: ({latitude}, {longitude})")
    print(f"   Topic Kafka: {topic}")
    print(f"   Intervalle: {interval} secondes")
    print(f"   Appuyez sur Ctrl+C pour arrêter\n")
    print("-" * 80)
    
    # Créer le producteur
    producer = create_producer(bootstrap_servers)
    
    message_count = 0
    
    try:
        while True:
            # Récupérer les données météo
            weather_data = fetch_weather_data(latitude, longitude)
            
            if weather_data:
                # Envoyer vers Kafka
                key = f"{latitude}_{longitude}"
                success = send_to_kafka(producer, topic, weather_data, key)
                
                if success:
                    message_count += 1
                    temp = weather_data['current']['temperature_2m']
                    wind = weather_data['current']['windspeed_10m']
                    print(f"✓ Message #{message_count} envoyé - "
                          f"Temp: {temp}°C, Vent: {wind} m/s - "
                          f"{datetime.now().strftime('%H:%M:%S')}")
                else:
                    print(f"✗ Échec de l'envoi du message #{message_count + 1}")
            else:
                print(f"✗ Impossible de récupérer les données météo")
            
            # Attendre avant la prochaine itération
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n Arrêt du producteur...")
    except Exception as e:
        print(f"\n Erreur: {e}")
    finally:
        producer.close()
        print(f"✓ Producteur fermé - {message_count} messages envoyés au total")


if __name__ == "__main__":
    main()