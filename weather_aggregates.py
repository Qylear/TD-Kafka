from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, min as spark_min, max as spark_max,
    sum as spark_sum, when, current_timestamp, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)


def create_spark_session(app_name="WeatherAggregates"):
    """Créer une session Spark"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def get_transformed_schema():
    """Schéma des données transformées"""
    return StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("event_time", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("weathercode", IntegerType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("fetched_at", StringType(), True),
        StructField("wind_alert_level", StringType(), True),
        StructField("heat_alert_level", StringType(), True),
        StructField("processing_time", StringType(), True)
    ])


def calculate_aggregates(df, window_duration="5 minutes", slide_duration="1 minute"):
    """Calculer les agrégats sur fenêtres glissantes"""
    
    # Convertir event_time en timestamp
    df_with_time = df.withColumn(
        "event_timestamp",
        col("event_time").cast("timestamp")
    )
    
    # Agrégats par fenêtre de temps
    windowed_aggregates = df_with_time \
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration),
            col("latitude"),
            col("longitude")
        ) \
        .agg(
            # Statistiques de température
            avg("temperature").alias("avg_temperature"),
            spark_min("temperature").alias("min_temperature"),
            spark_max("temperature").alias("max_temperature"),
            
            # Statistiques de vent
            avg("windspeed").alias("avg_windspeed"),
            spark_min("windspeed").alias("min_windspeed"),
            spark_max("windspeed").alias("max_windspeed"),
            
            # Comptage des alertes
            spark_sum(
                when(col("wind_alert_level") == "level_1", 1).otherwise(0)
            ).alias("wind_level1_count"),
            
            spark_sum(
                when(col("wind_alert_level") == "level_2", 1).otherwise(0)
            ).alias("wind_level2_count"),
            
            spark_sum(
                when(col("heat_alert_level") == "level_1", 1).otherwise(0)
            ).alias("heat_level1_count"),
            
            spark_sum(
                when(col("heat_alert_level") == "level_2", 1).otherwise(0)
            ).alias("heat_level2_count"),
            
            # Comptage total
            count("*").alias("total_measurements"),
            
            # Autres statistiques
            avg("humidity").alias("avg_humidity"),
            count(when(col("weathercode").isNotNull(), 1)).alias("weather_reports")
        )
    
    # Formater les résultats
    result = windowed_aggregates.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("latitude"),
        col("longitude"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_windspeed"),
        col("min_windspeed"),
        col("max_windspeed"),
        col("wind_level1_count"),
        col("wind_level2_count"),
        col("heat_level1_count"),
        col("heat_level2_count"),
        (col("wind_level1_count") + col("wind_level2_count")).alias("total_wind_alerts"),
        (col("heat_level1_count") + col("heat_level2_count")).alias("total_heat_alerts"),
        col("total_measurements"),
        col("avg_humidity"),
        col("weather_reports"),
        current_timestamp().alias("computed_at")
    )
    
    return result


def main():
    # Configuration
    kafka_bootstrap_servers = "localhost:9092"
    input_topic = "weather_transformed"
    output_topic = "weather_aggregates"
    
    # Paramètres de fenêtre
    window_duration = "5 minutes"  # Taille de la fenêtre
    slide_duration = "1 minute"    # Glissement de la fenêtre
    
    print(" Démarrage du job d'agrégation Spark")
    print(f"   Input Topic: {input_topic}")
    print(f"   Output Topic: {output_topic}")
    print(f"   Fenêtre: {window_duration}")
    print(f"   Glissement: {slide_duration}")
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
    schema = get_transformed_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Calculer les agrégats
    aggregates = calculate_aggregates(parsed_df, window_duration, slide_duration)
    
    print("\n Schéma des agrégats:")
    aggregates.printSchema()
    
    # Préparer pour Kafka
    kafka_output = aggregates.select(
        to_json(struct([col(c) for c in aggregates.columns])).alias("value")
    )
    
    # Écrire vers Kafka
    kafka_query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/kafka-checkpoint-aggregates") \
        .outputMode("update") \
        .start()
    
    # Afficher sur console
    console_query = aggregates.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()
    
    print("\n✓ Agrégation démarrée - Appuyez sur Ctrl+C pour arrêter")
    print("=" * 80)
    
    try:
        kafka_query.awaitTermination()
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n Arrêt du job...")
        kafka_query.stop()
        console_query.stop()
        spark.stop()
        print("✓ Job terminé")


if __name__ == "__main__":
    main()