import json
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def create_consumer(topic, bootstrap_servers='localhost:9092', group_id='weather-consumer-group'):
    """Créer un consommateur Kafka"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        print(f"Erreur lors de la création du consommateur: {e}")
        sys.exit(1)


def consume_messages(consumer, topic):
    """Consommer les messages en temps réel"""
    print(f"\n🎧 Écoute du topic '{topic}'...")
    print("Appuyez sur Ctrl+C pour arrêter\n")
    print("-" * 80)
    
    try:
        for message in consumer:
            print(f"\n📨 Nouveau message reçu:")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Timestamp: {message.timestamp}")
            print(f"   Key: {message.key}")
            print(f"   Value: {json.dumps(message.value, indent=2, ensure_ascii=False)}")
            print("-" * 80)
            
    except KeyboardInterrupt:
        print("\n\n⛔ Arrêt du consommateur...")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
    finally:
        consumer.close()
        print("✓ Consommateur fermé proprement")


def main():
    # Vérifier les arguments
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <topic_name>")
        print("Exemple: python consumer.py weather_stream")
        sys.exit(1)
    
    topic = sys.argv[1]
    bootstrap_servers = 'localhost:9092'
    
    print(f"🚀 Démarrage du consommateur Kafka")
    print(f"   Serveur: {bootstrap_servers}")
    print(f"   Topic: {topic}")
    
    # Créer et démarrer le consommateur
    consumer = create_consumer(topic, bootstrap_servers)
    consume_messages(consumer, topic)


if __name__ == "__main__":
    main()