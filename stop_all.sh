echo "🛑 Arrêt du projet Kafka Weather Streaming"
echo "=" | head -c 80; echo

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Fonction pour arrêter un processus
stop_process() {
    local process_name=$1
    local signal=${2:-TERM}
    
    pids=$(pgrep -f "$process_name")
    
    if [ -z "$pids" ]; then
        echo -e "${YELLOW}⚠${NC} Aucun processus '$process_name' en cours"
        return 0
    fi
    
    echo -e "${YELLOW}➜${NC} Arrêt de '$process_name'..."
    for pid in $pids; do
        kill -$signal "$pid" 2>/dev/null
        echo "   Arrêt du PID $pid"
    done
    
    sleep 2
    
    if pgrep -f "$process_name" > /dev/null; then
        echo -e "${RED}✗${NC} Certains processus n'ont pas pu être arrêtés"
        echo "   Essayez: kill -9 \$(pgrep -f '$process_name')"
    else
        echo -e "${GREEN}✓${NC} '$process_name' arrêté"
    fi
}

# Arrêter les processus Python
echo -e "\n${GREEN}1.${NC} Arrêt des scripts Python"
echo "-" | head -c 80; echo

stop_process "producer"
stop_process "consumer"
stop_process "visualize_weather"

# Arrêter Spark
echo -e "\n${GREEN}2.${NC} Arrêt des jobs Spark"
echo "-" | head -c 80; echo

stop_process "spark-submit"
stop_process "pyspark"

# Arrêter Kafka (optionnel)
echo -e "\n${GREEN}3.${NC} Services Kafka"
echo "-" | head -c 80; echo

read -p "Voulez-vous arrêter Kafka et ZooKeeper ? (o/N): " stop_kafka

if [[ "$stop_kafka" =~ ^[Oo]$ ]]; then
    stop_process "kafka" TERM
    sleep 2
    stop_process "zookeeper" TERM
    echo -e "${GREEN}✓${NC} Kafka et ZooKeeper arrêtés"
else
    echo -e "${YELLOW}⚠${NC} Kafka et ZooKeeper laissés en cours d'exécution"
fi

# Nettoyage des fichiers temporaires
echo -e "\n${GREEN}4.${NC} Nettoyage (optionnel)"
echo "-" | head -c 80; echo

read -p "Voulez-vous nettoyer les checkpoints Spark ? (o/N): " clean_checkpoints

if [[ "$clean_checkpoints" =~ ^[Oo]$ ]]; then
    rm -rf /tmp/kafka-checkpoint-* 2>/dev/null
    echo -e "${GREEN}✓${NC} Checkpoints nettoyés"
fi

# Afficher le statut final
echo -e "\n${GREEN}5.${NC} Statut final"
echo "-" | head -c 80; echo

if pgrep -f "producer|consumer|spark" > /dev/null; then
    echo -e "${YELLOW}⚠${NC} Certains processus sont encore en cours:"
    ps aux | grep -E "producer|consumer|spark|kafka|zookeeper" | grep -v grep
else
    echo -e "${GREEN}✓${NC} Tous les processus du projet sont arrêtés"
fi

echo -e "\n${GREEN}✓${NC} Terminé!"