import json
import sys
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pathlib import Path


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
    """Géocoder une ville"""
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {'name': city, 'count': 10, 'language': 'fr', 'format': 'json'}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'results' not in data or len(data['results']) == 0:
            return None
        
        for result in data['results']:
            if country.lower() in result.get('country', '').lower():
                return {
                    'city': result['name'],
                    'country': result['country'],
                    'country_code': result.get('country_code', ''),
                    'latitude': result['latitude'],
                    'longitude': result['longitude'],
                    'admin1': result.get('admin1', '')
                }
        
        result = data['results'][0]
        return {
            'city': result['name'],
            'country': result['country'],
            'country_code': result.get('country_code', ''),
            'latitude': result['latitude'],
            'longitude': result['longitude'],
            'admin1': result.get('admin1', '')
        }
    except Exception as e:
        print(f"❌ Erreur de géocodage: {e}")
        return None


def fetch_historical_weather(latitude, longitude, start_date, end_date):
    """Récupérer les données historiques via l'API Archive"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date,
        'end_date': end_date,
        'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,'
                'windspeed_10m_max,windspeed_10m_mean,'
                'precipitation_sum,weathercode,sunrise,sunset',
        'timezone': 'auto'
    }
    
    try:
        print(f"  📥 Téléchargement de {start_date} à {end_date}...")
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Erreur: {e}")
        return None


def save_to_hdfs(geocode_info, historical_data, base_path="/hdfs-data"):
    """Sauvegarder les données brutes dans HDFS"""
    base = Path(base_path)
    country = geocode_info['country'].replace(' ', '_').replace('/', '_')
    city = geocode_info['city'].replace(' ', '_').replace('/', '_')
    
    path = base / country / city
    path.mkdir(parents=True, exist_ok=True)
    
    filename = path / "weather_history_raw.json"
    
    full_data = {
        'geocode_info': geocode_info,
        'historical_data': historical_data,
        'downloaded_at': datetime.now().isoformat()
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(full_data, f, ensure_ascii=False, indent=2)
    
    print(f"  ✓ Sauvegardé dans: {filename}")
    return filename


def send_to_kafka(producer, topic, geocode_info, historical_data):
    """Envoyer les données historiques dans Kafka"""
    message = {
        'city': geocode_info['city'],
        'country': geocode_info['country'],
        'country_code': geocode_info['country_code'],
        'region': geocode_info['admin1'],
        'latitude': historical_data['latitude'],
        'longitude': historical_data['longitude'],
        'timezone': historical_data['timezone'],
        'elevation': historical_data['elevation'],
        'historical_data': historical_data,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        key = f"{geocode_info['country_code']}_{geocode_info['city']}"
        future = producer.send(topic, value=message, key=key)
        future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"  ❌ Erreur Kafka: {e}")
        return False


def main():
    if len(sys.argv) < 3:
        print("Usage: python historical_weather_producer.py <city> <country>")
        print("Exemple: python historical_weather_producer.py Paris France")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2]
    years = 10
    
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'weather_history'
    hdfs_base = '/home/claude/kafka-weather-project/data/hdfs-data'
    
    print(f"🌍 Téléchargement des données historiques")
    print(f"   Ville: {city}, {country}")
    print(f"   Période: {years} ans")
    print("-" * 80)
    
    # Géocoder
    geocode_info = geocode_city(city, country)
    if not geocode_info:
        print("❌ Ville non trouvée")
        sys.exit(1)
    
    print(f"\n✓ Ville: {geocode_info['city']}, {geocode_info['country']}")
    print(f"  Coordonnées: ({geocode_info['latitude']}, {geocode_info['longitude']})")
    
    # Calculer les dates
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=years*365)
    
    print(f"\n📅 Période de données: {start_date} à {end_date}")
    
    # Récupérer les données historiques
    print("\n📥 Téléchargement des données...")
    historical_data = fetch_historical_weather(
        geocode_info['latitude'],
        geocode_info['longitude'],
        start_date.isoformat(),
        end_date.isoformat()
    )
    
    if not historical_data:
        print("❌ Échec du téléchargement")
        sys.exit(1)
    
    # Compter les jours
    if 'daily' in historical_data and 'time' in historical_data['daily']:
        day_count = len(historical_data['daily']['time'])
        print(f"  ✓ {day_count} jours de données récupérés")
    
    # Sauvegarder dans HDFS
    print("\n💾 Sauvegarde dans HDFS...")
    hdfs_file = save_to_hdfs(geocode_info, historical_data, hdfs_base)
    
    # Envoyer dans Kafka
    print("\n📤 Envoi vers Kafka...")
    producer = create_producer(bootstrap_servers)
    
    success = send_to_kafka(producer, topic, geocode_info, historical_data)
    
    producer.close()
    
    if success:
        print(f"  ✓ Données envoyées dans le topic '{topic}'")
    
    print("\n✓ Exercice 9 terminé!")
    print(f"  Fichier HDFS: {hdfs_file}")


if __name__ == "__main__":
    main()