import json
import sys
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
        print(f"Erreur lors de la création du producteur: {e}")
        sys.exit(1)


def send_message(producer, topic, message):
    """Envoyer un message dans un topic Kafka"""
    try:
        future = producer.send(topic, value=message)
        record_metadata = future.get(timeout=10)
        print(f"Message envoyé avec succès!")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
        return True
    except KafkaError as e:
        print(f"Erreur lors de l'envoi du message: {e}")
        return False


def main():
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'weather_stream'
    
    # Message à envoyer
    message = {"msg": "Hello Kafka"}
    
    print(f"Envoi du message vers le topic '{topic}'...")
    print(f"Message: {message}")
    
    # Créer le producteur
    producer = create_producer(bootstrap_servers)
    
    # Envoyer le message
    success = send_message(producer, topic, message)
    
    # Fermer le producteur
    producer.close()
    
    if success:
        print("\n✓ Exercice 1 terminé avec succès!")
    else:
        print("\n✗ Erreur lors de l'envoi du message")
        sys.exit(1)


if __name__ == "__main__":
    main()