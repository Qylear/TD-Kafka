import json
import os
import sys
from pathlib import Path
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError


class HDFSStorageConsumer:
    def __init__(self, bootstrap_servers, topic, base_hdfs_path="/hdfs-data"):
        """Initialiser le consommateur HDFS"""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.base_hdfs_path = Path(base_hdfs_path)
        self.consumer = None
        self.message_count = 0
        self.files_created = set()
        
    def create_consumer(self):
        """Créer le consommateur Kafka"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='hdfs-storage-consumer',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"✓ Consommateur créé pour le topic '{self.topic}'")
        except Exception as e:
            print(f"❌ Erreur lors de la création du consommateur: {e}")
            sys.exit(1)
    
    def get_storage_path(self, country, city):
        """Générer le chemin de stockage HDFS"""
        # Nettoyer les noms pour éviter les problèmes de chemin
        country_clean = country.replace(' ', '_').replace('/', '_')
        city_clean = city.replace(' ', '_').replace('/', '_')
        
        # Créer le chemin
        path = self.base_hdfs_path / country_clean / city_clean
        path.mkdir(parents=True, exist_ok=True)
        
        return path
    
    def has_alert(self, data):
        """Vérifier si le message contient des alertes"""
        wind_alert = data.get('wind_alert_level', 'level_0')
        heat_alert = data.get('heat_alert_level', 'level_0')
        
        return wind_alert != 'level_0' or heat_alert != 'level_0'
    
    def save_alert(self, data):
        """Sauvegarder une alerte dans HDFS"""
        try:
            # Extraire les informations géographiques
            country = data.get('country', 'Unknown')
            city = data.get('city', 'Unknown')
            
            # Si pas d'info de ville/pays, utiliser les coordonnées
            if country == 'Unknown' or city == 'Unknown':
                lat = data.get('latitude', 0)
                lon = data.get('longitude', 0)
                country = f"coord_{lat}"
                city = f"coord_{lon}"
            
            # Obtenir le chemin de stockage
            storage_path = self.get_storage_path(country, city)
            
            # Nom du fichier avec timestamp
            filename = storage_path / "alerts.json"
            
            # Préparer l'enregistrement
            alert_record = {
                'country': country,
                'city': city,
                'latitude': data.get('latitude'),
                'longitude': data.get('longitude'),
                'event_time': data.get('event_time'),
                'temperature': data.get('temperature'),
                'windspeed': data.get('windspeed'),
                'wind_alert_level': data.get('wind_alert_level'),
                'heat_alert_level': data.get('heat_alert_level'),
                'weathercode': data.get('weathercode'),
                'humidity': data.get('humidity'),
                'saved_at': datetime.now().isoformat()
            }
            
            # Sauvegarder (append mode)
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(json.dumps(alert_record, ensure_ascii=False) + '\n')
            
            # Marquer le fichier comme créé
            file_key = f"{country}/{city}"
            if file_key not in self.files_created:
                self.files_created.add(file_key)
                print(f"📁 Nouveau fichier créé: {filename}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde: {e}")
            return False
    
    def consume_and_store(self):
        """Consommer les messages et les stocker"""
        print(f"\n🎧 Écoute du topic '{self.topic}'...")
        print(f"📂 Répertoire de base HDFS: {self.base_hdfs_path}")
        print("   Appuyez sur Ctrl+C pour arrêter\n")
        print("-" * 80)
        
        try:
            for message in self.consumer:
                self.message_count += 1
                data = message.value
                
                # Vérifier s'il y a des alertes
                if self.has_alert(data):
                    success = self.save_alert(data)
                    
                    if success:
                        wind_level = data.get('wind_alert_level', 'N/A')
                        heat_level = data.get('heat_alert_level', 'N/A')
                        city = data.get('city', f"({data.get('latitude')}, {data.get('longitude')})")
                        
                        print(f"✓ Alerte #{self.message_count} sauvegardée - "
                              f"{city} - "
                              f"Vent: {wind_level}, Chaleur: {heat_level}")
                else:
                    # Message sans alerte, on le compte mais ne le sauvegarde pas
                    if self.message_count % 10 == 0:
                        print(f"  {self.message_count} messages traités "
                              f"(pas d'alerte dans ce message)")
                
        except KeyboardInterrupt:
            print("\n\n⛔ Arrêt du consommateur...")
        except Exception as e:
            print(f"\n❌ Erreur: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.consumer.close()
            print(f"\n✓ Consommateur fermé")
            print(f"  Messages traités: {self.message_count}")
            print(f"  Fichiers créés: {len(self.files_created)}")
            self.print_summary()
    
    def print_summary(self):
        """Afficher un résumé des fichiers créés"""
        if not self.files_created:
            print("\n📊 Aucun fichier d'alertes créé")
            return
        
        print("\n📊 Résumé des fichiers créés:")
        print("-" * 80)
        for file_key in sorted(self.files_created):
            country, city = file_key.split('/')
            path = self.get_storage_path(country, city) / "alerts.json"
            if path.exists():
                # Compter les lignes
                with open(path, 'r') as f:
                    line_count = sum(1 for _ in f)
                print(f"  {country}/{city}: {line_count} alertes")


def main():
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'weather_transformed'
    base_hdfs_path = '/home/claude/kafka-weather-project/data/hdfs-data'
    
    print("🚀 Démarrage du consommateur HDFS Storage")
    print(f"   Serveur Kafka: {bootstrap_servers}")
    print(f"   Topic: {topic}")
    print(f"   Base HDFS: {base_hdfs_path}")
    
    # Créer et démarrer le consommateur
    storage_consumer = HDFSStorageConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        base_hdfs_path=base_hdfs_path
    )
    
    storage_consumer.create_consumer()
    storage_consumer.consume_and_store()


if __name__ == "__main__":
    main()