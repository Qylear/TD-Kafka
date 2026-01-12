import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, when, lit,
    struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType
)


def create_spark_session(app_name="WeatherAlertDetection"):
    """Créer une session Spark avec configuration Kafka"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def get_weather_schema():
    """Définir le schéma des données météo"""
    current_schema = StructType([
        StructField("time", StringType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("windspeed_10m", DoubleType(), True),
        StructField("weathercode", IntegerType(), True),
        StructField("relative_humidity_2m", DoubleType(), True),
        StructField("apparent_temperature", DoubleType(), True)
    ])
    
    return StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("current", current_schema, True),
        StructField("timestamp", StringType(), True),
        StructField("fetched_at", StringType(), True)
    ])


def classify_wind_alert(windspeed):
    """Classifier les alertes de vent"""
    return when(windspeed < 10, "level_0") \
        .when((windspeed >= 10) & (windspeed <= 20), "level_1") \
        .when(windspeed > 20, "level_2") \
        .otherwise("unknown")


def classify_heat_alert(temperature):
    """Classifier les alertes de chaleur"""
    return when(temperature < 25, "level_0") \
        .when((temperature >= 25) & (temperature <= 35), "level_1") \
        .when(temperature > 35, "level_2") \
        .otherwise("unknown")


def transform_weather_data(df):
    """Transformer les données et ajouter les alertes"""
    
    # Extraire les champs du current
    transformed_df = df.select(
        col("latitude"),
        col("longitude"),
        col("timezone"),
        col("elevation"),
        col("current.time").alias("event_time"),
        col("current.temperature_2m").alias("temperature"),
        col("current.windspeed_10m").alias("windspeed"),
        col("current.weathercode").alias("weathercode"),
        col("current.relative_humidity_2m").alias("humidity"),
        col("current.apparent_temperature").alias("apparent_temperature"),
        col("timestamp"),
        col("fetched_at")
    )
    
    # Ajouter les alertes
    transformed_df = transformed_df.withColumn(
        "wind_alert_level",
        classify_wind_alert(col("windspeed"))
    ).withColumn(
        "heat_alert_level",
        classify_heat_alert(col("temperature"))
    ).withColumn(
        "processing_time",
        current_timestamp()
    )
    
    return transformed_df


def main():
    # Configuration
    kafka_bootstrap_servers = "localhost:9092"
    input_topic = "weather_stream"
    output_topic = "weather_transformed"
    
    print(" Démarrage du job Spark Streaming")
    print(f"   Input Topic: {input_topic}")
    print(f"   Output Topic: {output_topic}")
    print("-" * 80)
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Lire depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parser le JSON
    weather_schema = get_weather_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), weather_schema).alias("data")
    ).select("data.*")
    
    # Transformer les données
    transformed_df = transform_weather_data(parsed_df)
    
    # Afficher le schéma
    print("\n Schéma des données transformées:")
    transformed_df.printSchema()
    
    # Préparer pour l'envoi vers Kafka
    kafka_output = transformed_df.select(
        to_json(struct([col(c) for c in transformed_df.columns])).alias("value")
    )
    
    # Écrire vers Kafka
    query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/kafka-checkpoint-weather-transformed") \
        .outputMode("append") \
        .start()
    
    # Afficher sur la console (pour debug)
    console_query = transformed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()
    
    print("\n✓ Streaming démarré - Appuyez sur Ctrl+C pour arrêter")
    print("=" * 80)
    
    # Attendre la fin
    try:
        query.awaitTermination()
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n Arrêt du job Spark...")
        query.stop()
        console_query.stop()
        spark.stop()
        print("✓ Job terminé")


if __name__ == "__main__":
    main()