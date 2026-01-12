import json
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as spark_max, min as spark_min,
    sum as spark_sum, struct, to_json, lit
)
from kafka import KafkaProducer


def create_spark_session(app_name="ClimateRecordsDetection"):
    """Créer une session Spark"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def create_kafka_producer(bootstrap_servers='localhost:9092'):
    """Créer un producteur Kafka"""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def load_historical_data(spark, hdfs_path):
    """Charger les données historiques depuis HDFS"""
    all_files = list(Path(hdfs_path).rglob("weather_history_raw.json"))
    
    if not all_files:
        print("❌ Aucun fichier historique trouvé")
        return None
    
    print(f"📂 {len(all_files)} fichiers historiques trouvés")
    
    records = []
    
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            geocode = data['geocode_info']
            historical = data['historical_data']
            daily = historical.get('daily', {})
            
            # Créer un enregistrement par jour
            times = daily.get('time', [])
            temp_max = daily.get('temperature_2m_max', [])
            temp_min = daily.get('temperature_2m_min', [])
            wind_max = daily.get('windspeed_10m_max', [])
            precip = daily.get('precipitation_sum', [])
            
            for i in range(len(times)):
                record = {
                    'city': geocode['city'],
                    'country': geocode['country'],
                    'country_code': geocode['country_code'],
                    'latitude': historical['latitude'],
                    'longitude': historical['longitude'],
                    'date': times[i],
                    'temp_max': temp_max[i] if i < len(temp_max) and temp_max[i] is not None else None,
                    'temp_min': temp_min[i] if i < len(temp_min) and temp_min[i] is not None else None,
                    'wind_max': wind_max[i] if i < len(wind_max) and wind_max[i] is not None else None,
                    'precipitation': precip[i] if i < len(precip) and precip[i] is not None else None
                }
                records.append(record)
        
        except Exception as e:
            print(f"⚠️  Erreur lors de la lecture de {file_path}: {e}")
    
    if not records:
        return None
    
    # Créer un DataFrame Spark
    df = spark.createDataFrame(records)
    return df


def detect_records(df):
    """Détecter les records climatiques par ville"""
    
    # Filtrer les valeurs nulles
    df_clean = df.filter(
        col("temp_max").isNotNull() &
        col("temp_min").isNotNull() &
        col("wind_max").isNotNull() &
        col("precipitation").isNotNull()
    )
    
    # Agréger par ville
    records = df_clean.groupBy("city", "country", "country_code", "latitude", "longitude").agg(
        spark_max("temp_max").alias("hottest_temp"),
        spark_min("temp_min").alias("coldest_temp"),
        spark_max("wind_max").alias("strongest_wind"),
        spark_max("precipitation").alias("max_daily_precipitation"),
        spark_sum("precipitation").alias("total_precipitation")
    )
    
    # Trouver les dates correspondantes
    # Pour le jour le plus chaud
    hottest_day = df_clean.alias("a").join(
        records.alias("b"),
        (col("a.city") == col("b.city")) & 
        (col("a.temp_max") == col("b.hottest_temp"))
    ).select(
        col("a.city"),
        col("a.date").alias("hottest_day"),
        col("a.temp_max").alias("hottest_temp")
    ).dropDuplicates(["city"])
    
    # Pour le jour le plus froid
    coldest_day = df_clean.alias("a").join(
        records.alias("b"),
        (col("a.city") == col("b.city")) & 
        (col("a.temp_min") == col("b.coldest_temp"))
    ).select(
        col("a.city"),
        col("a.date").alias("coldest_day"),
        col("a.temp_min").alias("coldest_temp")
    ).dropDuplicates(["city"])
    
    # Pour la plus forte rafale
    windiest_day = df_clean.alias("a").join(
        records.alias("b"),
        (col("a.city") == col("b.city")) & 
        (col("a.wind_max") == col("b.strongest_wind"))
    ).select(
        col("a.city"),
        col("a.date").alias("windiest_day"),
        col("a.wind_max").alias("strongest_wind")
    ).dropDuplicates(["city"])
    
    # Jointures pour créer le résultat final
    result = records \
        .join(hottest_day, "city", "left") \
        .join(coldest_day, "city", "left") \
        .join(windiest_day, "city", "left")
    
    return result


def save_records_to_hdfs(records_df, base_path):
    """Sauvegarder les records dans HDFS"""
    records_list = records_df.collect()
    
    saved_files = []
    
    for record in records_list:
        country = record['country'].replace(' ', '_').replace('/', '_')
        city = record['city'].replace(' ', '_').replace('/', '_')
        
        path = Path(base_path) / country / city
        path.mkdir(parents=True, exist_ok=True)
        
        filename = path / "weather_records.json"
        
        record_data = {
            'city': record['city'],
            'country': record['country'],
            'country_code': record['country_code'],
            'latitude': record['latitude'],
            'longitude': record['longitude'],
            'records': {
                'hottest_day': {
                    'date': record['hottest_day'],
                    'temperature': float(record['hottest_temp'])
                },
                'coldest_day': {
                    'date': record['coldest_day'],
                    'temperature': float(record['coldest_temp'])
                },
                'windiest_day': {
                    'date': record['windiest_day'],
                    'windspeed': float(record['strongest_wind'])
                },
                'precipitation': {
                    'max_daily': float(record['max_daily_precipitation']),
                    'total_period': float(record['total_precipitation'])
                }
            },
            'computed_at': str(col("current_timestamp"))
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record_data, f, ensure_ascii=False, indent=2)
        
        saved_files.append(str(filename))
        print(f"  ✓ {record['city']}, {record['country']}: {filename}")
    
    return saved_files


def send_to_kafka(producer, topic, records_df):
    """Envoyer les records dans Kafka"""
    records_list = records_df.collect()
    
    count = 0
    for record in records_list:
        message = {
            'city': record['city'],
            'country': record['country'],
            'country_code': record['country_code'],
            'latitude': record['latitude'],
            'longitude': record['longitude'],
            'records': {
                'hottest_day': {
                    'date': record['hottest_day'],
                    'temperature': float(record['hottest_temp'])
                },
                'coldest_day': {
                    'date': record['coldest_day'],
                    'temperature': float(record['coldest_temp'])
                },
                'windiest_day': {
                    'date': record['windiest_day'],
                    'windspeed': float(record['strongest_wind'])
                },
                'precipitation': {
                    'max_daily': float(record['max_daily_precipitation'])
                }
            }
        }
        
        try:
            key = f"{record['country_code']}_{record['city']}"
            producer.send(topic, value=message, key=key)
            count += 1
        except Exception as e:
            print(f"  ⚠️  Erreur Kafka pour {record['city']}: {e}")
    
    producer.flush()
    return count


def main():
    # Configuration
    hdfs_base = '/home/claude/kafka-weather-project/data/hdfs-data'
    kafka_bootstrap = 'localhost:9092'
    kafka_topic = 'weather_records'
    
    print("🚀 Détection des records climatiques")
    print(f"   Source HDFS: {hdfs_base}")
    print(f"   Topic Kafka: {kafka_topic}")
    print("-" * 80)
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Charger les données
    print("\n📂 Chargement des données historiques...")
    df = load_historical_data(spark, hdfs_base)
    
    if df is None:
        print("❌ Aucune donnée chargée")
        spark.stop()
        sys.exit(1)
    
    print(f"✓ {df.count()} enregistrements chargés")
    
    # Détecter les records
    print("\n🔍 Analyse des records climatiques...")
    records_df = detect_records(df)
    
    print("\n📊 Records détectés:")
    records_df.show(truncate=False)
    
    # Sauvegarder dans HDFS
    print("\n💾 Sauvegarde des records dans HDFS...")
    saved_files = save_records_to_hdfs(records_df, hdfs_base)
    print(f"✓ {len(saved_files)} fichiers sauvegardés")
    
    # Envoyer dans Kafka
    print("\n📤 Envoi vers Kafka...")
    producer = create_kafka_producer(kafka_bootstrap)
    count = send_to_kafka(producer, kafka_topic, records_df)
    producer.close()
    print(f"✓ {count} records envoyés dans le topic '{kafka_topic}'")
    
    # Fermer Spark
    spark.stop()
    
    print("\n✓ Exercice 10 terminé!")


if __name__ == "__main__":
    main()