set -e

echo "🚀 Démarrage du projet Kafka Weather Streaming"
echo "=" | head -c 80; echo

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka_2.13-3.6.0}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Fonction pour vérifier si un service est en cours d'exécution
check_service() {
    if pgrep -f "$1" > /dev/null; then
        echo -e "${GREEN}✓${NC} $2 est déjà en cours d'exécution"
        return 0
    else
        echo -e "${YELLOW}⚠${NC} $2 n'est pas en cours d'exécution"
        return 1
    fi
}

# Fonction pour démarrer un service
start_service() {
    local service_name=$1
    local command=$2
    
    echo -e "\n${YELLOW}➜${NC} Démarrage de $service_name..."
    eval "$command" &
    sleep 3
    
    if pgrep -f "$service_name" > /dev/null; then
        echo -e "${GREEN}✓${NC} $service_name démarré avec succès"
    else
        echo -e "${RED}✗${NC} Échec du démarrage de $service_name"
        return 1
    fi
}

# Vérifier que Kafka est installé
if [ ! -d "$KAFKA_HOME" ]; then
    echo -e "${RED}✗${NC} Kafka n'est pas installé dans $KAFKA_HOME"
    echo "   Veuillez définir la variable KAFKA_HOME ou installer Kafka"
    exit 1
fi

echo -e "\n${GREEN}1.${NC} Vérification des services existants"
echo "-" | head -c 80; echo

check_service "zookeeper" "ZooKeeper" || NEED_ZOOKEEPER=1
check_service "kafka" "Kafka" || NEED_KAFKA=1

# Démarrer ZooKeeper si nécessaire
if [ "$NEED_ZOOKEEPER" = "1" ]; then
    start_service "zookeeper" "$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties"
fi

# Démarrer Kafka si nécessaire
if [ "$NEED_KAFKA" = "1" ]; then
    start_service "kafka" "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties"
    sleep 5  # Attendre que Kafka soit complètement démarré
fi

# Créer les topics Kafka
echo -e "\n${GREEN}2.${NC} Création des topics Kafka"
echo "-" | head -c 80; echo

TOPICS=(
    "weather_stream"
    "weather_transformed"
    "weather_aggregates"
    "weather_anomalies"
    "weather_history"
    "weather_records"
)

for topic in "${TOPICS[@]}"; do
    if $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -q "^${topic}$"; then
        echo -e "${GREEN}✓${NC} Topic '$topic' existe déjà"
    else
        echo -e "${YELLOW}➜${NC} Création du topic '$topic'..."
        $KAFKA_HOME/bin/kafka-topics.sh --create \
            --topic "$topic" \
            --bootstrap-server localhost:9092 \
            --partitions 3 \
            --replication-factor 1 2>/dev/null
        echo -e "${GREEN}✓${NC} Topic '$topic' créé"
    fi
done

# Créer les répertoires de données
echo -e "\n${GREEN}3.${NC} Création des répertoires de données"
echo "-" | head -c 80; echo

mkdir -p "$PROJECT_DIR/data/hdfs-data"
mkdir -p "$PROJECT_DIR/data/visualizations"
mkdir -p "$PROJECT_DIR/data/checkpoints"

echo -e "${GREEN}✓${NC} Répertoires créés"

# Afficher les informations de connexion
echo -e "\n${GREEN}4.${NC} Informations de connexion"
echo "-" | head -c 80; echo
echo "Kafka Bootstrap Server: localhost:9092"
echo "ZooKeeper: localhost:2181"
echo "Project Directory: $PROJECT_DIR"

# Menu interactif
echo -e "\n${GREEN}5.${NC} Que souhaitez-vous faire ?"
echo "-" | head -c 80; echo
echo "1) Démarrer un producteur simple (Ex. 1)"
echo "2) Démarrer un consommateur (Ex. 2)"
echo "3) Démarrer le streaming météo (Ex. 3)"
echo "4) Démarrer la transformation Spark (Ex. 4)"
echo "5) Démarrer le producteur enrichi (Ex. 6)"
echo "6) Démarrer le stockage HDFS (Ex. 7)"
echo "7) Générer les visualisations (Ex. 8)"
echo "8) Lancer un workflow complet"
echo "9) Quitter"
echo ""
read -p "Votre choix [1-9]: " choice

case $choice in
    1)
        echo -e "\n${YELLOW}➜${NC} Démarrage du producteur simple..."
        cd "$PROJECT_DIR"
        python3 producers/simple_producer.py
        ;;
    2)
        echo -e "\n${YELLOW}➜${NC} Démarrage du consommateur..."
        read -p "Topic à consommer [weather_stream]: " topic
        topic=${topic:-weather_stream}
        cd "$PROJECT_DIR"
        python3 consumers/consumer.py "$topic"
        ;;
    3)
        echo -e "\n${YELLOW}➜${NC} Démarrage du streaming météo..."
        read -p "Latitude [48.8566]: " lat
        read -p "Longitude [2.3522]: " lon
        read -p "Intervalle en secondes [10]: " interval
        lat=${lat:-48.8566}
        lon=${lon:-2.3522}
        interval=${interval:-10}
        cd "$PROJECT_DIR"
        python3 producers/current_weather.py "$lat" "$lon" "$interval"
        ;;
    4)
        echo -e "\n${YELLOW}➜${NC} Démarrage de la transformation Spark..."
        cd "$PROJECT_DIR"
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            spark-jobs/weather_transformation.py
        ;;
    5)
        echo -e "\n${YELLOW}➜${NC} Démarrage du producteur enrichi..."
        read -p "Ville [Paris]: " city
        read -p "Pays [France]: " country
        read -p "Intervalle en secondes [30]: " interval
        city=${city:-Paris}
        country=${country:-France}
        interval=${interval:-30}
        cd "$PROJECT_DIR"
        python3 producers/enhanced_weather_producer.py "$city" "$country" "$interval"
        ;;
    6)
        echo -e "\n${YELLOW}➜${NC} Démarrage du consommateur HDFS..."
        cd "$PROJECT_DIR"
        python3 consumers/hdfs_storage_consumer.py
        ;;
    7)
        echo -e "\n${YELLOW}➜${NC} Génération des visualisations..."
        cd "$PROJECT_DIR"
        python3 consumers/visualize_weather.py
        ;;
    8)
        echo -e "\n${YELLOW}➜${NC} Lancement du workflow complet..."
        echo "Ce workflow va lancer plusieurs processus en parallèle"
        echo ""
        
        cd "$PROJECT_DIR"
        
        # Lancer Spark transformation en arrière-plan
        echo "1. Démarrage de la transformation Spark..."
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            spark-jobs/weather_transformation.py > logs/spark_transform.log 2>&1 &
        SPARK_PID=$!
        sleep 5
        
        # Lancer le producteur enrichi en arrière-plan
        echo "2. Démarrage du producteur (Paris)..."
        python3 producers/enhanced_weather_producer.py Paris France 30 > logs/producer_paris.log 2>&1 &
        PRODUCER_PID=$!
        sleep 2
        
        # Lancer le consommateur HDFS en arrière-plan
        echo "3. Démarrage du stockage HDFS..."
        python3 consumers/hdfs_storage_consumer.py > logs/hdfs_consumer.log 2>&1 &
        HDFS_PID=$!
        
        echo -e "\n${GREEN}✓${NC} Workflow complet lancé!"
        echo "   Spark Transformation PID: $SPARK_PID"
        echo "   Producteur PID: $PRODUCER_PID"
        echo "   HDFS Consumer PID: $HDFS_PID"
        echo ""
        echo "Pour arrêter tous les processus, utilisez: ./scripts/stop_all.sh"
        echo "Logs disponibles dans le dossier 'logs/'"
        ;;
    9)
        echo -e "\n${GREEN}✓${NC} Au revoir!"
        exit 0
        ;;
    *)
        echo -e "\n${RED}✗${NC} Choix invalide"
        exit 1
        ;;
esac

echo -e "\n${GREEN}✓${NC} Terminé!"