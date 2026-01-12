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
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        return producer
    except Exception as e:
        print(f"❌ Erreur lors de la création du producteur: {e}")
        sys.exit(1)


def geocode_city(city, country):
    """Récupérer les coordonnées d'une ville via l'API de géocodage Open-Meteo"""
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        'name': city,
        'count': 10,
        'language': 'fr',
        'format': 'json'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'results' not in data or len(data['results']) == 0:
            print(f"❌ Aucun résultat trouvé pour '{city}'")
            return None
        
        # Chercher la ville dans le pays spécifié
        for result in data['results']:
            if country.lower() in result.get('country', '').lower():
                geocode_info = {
                    'city': result['name'],
                    'country': result['country'],
                    'country_code': result.get('country_code', ''),
                    'latitude': result['latitude'],
                    'longitude': result['longitude'],
                    'admin1': result.get('admin1', ''),  # Région
                    'population': result.get('population', 0),
                    'timezone': result.get('timezone', '')
                }
                return geocode_info
        
        # Si pas trouvé dans le pays, prendre le premier résultat
        result = data['results'][0]
        print(f"⚠️  Pays exact non trouvé, utilisation de: {result['name']}, {result['country']}")
        geocode_info = {
            'city': result['name'],
            'country': result['country'],
            'country_code': result.get('country_code', ''),
            'latitude': result['latitude'],
            'longitude': result['longitude'],
            'admin1': result.get('admin1', ''),
            'population': result.get('population', 0),
            'timezone': result.get('timezone', '')
        }
        return geocode_info
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors du géocodage: {e}")
        return None


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
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la récupération des données météo: {e}")
        return None


def create_weather_message(geocode_info, weather_data):
    """Créer le message enrichi avec toutes les informations"""
    message = {
        # Informations géographiques
        'city': geocode_info['city'],
        'country': geocode_info['country'],
        'country_code': geocode_info['country_code'],
        'region': geocode_info['admin1'],
        'latitude': weather_data['latitude'],
        'longitude': weather_data['longitude'],
        'timezone': weather_data['timezone'],
        'elevation': weather_data['elevation'],
        'population': geocode_info['population'],
        
        # Données météo actuelles
        'current': weather_data['current'],
        
        # Métadonnées
        'timestamp': datetime.now().isoformat(),
        'fetched_at': weather_data['current']['time']
    }
    
    return message


def send_to_kafka(producer, topic, data, key=None):
    """Envoyer les données dans Kafka"""
    try:
        future = producer.send(topic, value=data, key=key)
        record_metadata = future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"❌ Erreur lors de l'envoi vers Kafka: {e}")
        return False


def main():
    # Vérifier les arguments
    if len(sys.argv) < 3:
        print("Usage: python enhanced_weather_producer.py <city> <country> [interval_seconds]")
        print("Exemple: python enhanced_weather_producer.py Paris France 10")
        print("         python enhanced_weather_producer.py London UK 30")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2]
    interval = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'weather_stream'
    
    print(f"🌍 Recherche des coordonnées de {city}, {country}...")
    
    # Géocoder la ville
    geocode_info = geocode_city(city, country)
    
    if not geocode_info:
        print("❌ Impossible de trouver les coordonnées de la ville")
        sys.exit(1)
    
    print(f"\n✓ Ville trouvée:")
    print(f"   Nom: {geocode_info['city']}")
    print(f"   Pays: {geocode_info['country']} ({geocode_info['country_code']})")
    print(f"   Région: {geocode_info['admin1']}")
    print(f"   Coordonnées: ({geocode_info['latitude']}, {geocode_info['longitude']})")
    if geocode_info['population'] > 0:
        print(f"   Population: {geocode_info['population']:,}")
    
    print(f"\n🌤️  Démarrage du producteur météo enrichi")
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
            weather_data = fetch_weather_data(
                geocode_info['latitude'],
                geocode_info['longitude']
            )
            
            if weather_data:
                # Créer le message enrichi
                message = create_weather_message(geocode_info, weather_data)
                
                # Envoyer vers Kafka avec clé pour partitionnement
                key = f"{geocode_info['country_code']}_{geocode_info['city']}"
                success = send_to_kafka(producer, topic, message, key)
                
                if success:
                    message_count += 1
                    temp = message['current']['temperature_2m']
                    wind = message['current']['windspeed_10m']
                    print(f"✓ Message #{message_count} envoyé - "
                          f"{geocode_info['city']}, {geocode_info['country']} - "
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
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        producer.close()
        print(f"✓ Producteur fermé - {message_count} messages envoyés au total")


if __name__ == "__main__":
    main()