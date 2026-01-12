import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


class WeatherVisualizer:
    def __init__(self, hdfs_base_path="/hdfs-data"):
        """Initialiser le visualiseur"""
        self.hdfs_base_path = Path(hdfs_base_path)
        self.data = []
        
    def load_data(self, country=None, city=None):
        """Charger les données depuis HDFS"""
        print("📂 Chargement des données...")
        
        if country and city:
            # Charger une ville spécifique
            path = self.hdfs_base_path / country / city / "alerts.json"
            if path.exists():
                self._load_file(path)
        else:
            # Charger toutes les données
            for alerts_file in self.hdfs_base_path.rglob("alerts.json"):
                self._load_file(alerts_file)
        
        print(f"✓ {len(self.data)} enregistrements chargés")
        return len(self.data)
    
    def _load_file(self, filepath):
        """Charger un fichier JSON lines"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        # Parser le timestamp
                        if 'event_time' in record:
                            try:
                                record['datetime'] = datetime.fromisoformat(
                                    record['event_time'].replace('Z', '+00:00')
                                )
                            except:
                                record['datetime'] = datetime.now()
                        self.data.append(record)
        except Exception as e:
            print(f"⚠️  Erreur lors de la lecture de {filepath}: {e}")
    
    def plot_temperature_evolution(self, output_file='temperature_evolution.png'):
        """Graphique de l'évolution de la température"""
        if not self.data:
            print("❌ Pas de données à visualiser")
            return
        
        # Organiser par ville
        cities_data = defaultdict(lambda: {'times': [], 'temps': []})
        
        for record in self.data:
            if 'datetime' in record and 'temperature' in record:
                city_key = f"{record.get('city', 'Unknown')}, {record.get('country', 'Unknown')}"
                cities_data[city_key]['times'].append(record['datetime'])
                cities_data[city_key]['temps'].append(record['temperature'])
        
        # Créer le graphique
        plt.figure(figsize=(14, 8))
        
        for city, data in cities_data.items():
            if data['times']:
                # Trier par temps
                sorted_data = sorted(zip(data['times'], data['temps']))
                times, temps = zip(*sorted_data)
                plt.plot(times, temps, marker='o', label=city, linewidth=2, markersize=4)
        
        plt.xlabel('Temps', fontsize=12)
        plt.ylabel('Température (°C)', fontsize=12)
        plt.title('Évolution de la température au fil du temps', fontsize=14, fontweight='bold')
        plt.legend(loc='best')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Graphique sauvegardé: {output_file}")
        plt.close()
    
    def plot_windspeed_evolution(self, output_file='windspeed_evolution.png'):
        """Graphique de l'évolution de la vitesse du vent"""
        if not self.data:
            print("❌ Pas de données à visualiser")
            return
        
        cities_data = defaultdict(lambda: {'times': [], 'winds': []})
        
        for record in self.data:
            if 'datetime' in record and 'windspeed' in record:
                city_key = f"{record.get('city', 'Unknown')}, {record.get('country', 'Unknown')}"
                cities_data[city_key]['times'].append(record['datetime'])
                cities_data[city_key]['winds'].append(record['windspeed'])
        
        plt.figure(figsize=(14, 8))
        
        for city, data in cities_data.items():
            if data['times']:
                sorted_data = sorted(zip(data['times'], data['winds']))
                times, winds = zip(*sorted_data)
                plt.plot(times, winds, marker='o', label=city, linewidth=2, markersize=4)
        
        plt.xlabel('Temps', fontsize=12)
        plt.ylabel('Vitesse du vent (m/s)', fontsize=12)
        plt.title('Évolution de la vitesse du vent', fontsize=14, fontweight='bold')
        plt.legend(loc='best')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Graphique sauvegardé: {output_file}")
        plt.close()
    
    def plot_alert_counts(self, output_file='alert_counts.png'):
        """Graphique du nombre d'alertes par niveau"""
        if not self.data:
            print(" Pas de données à visualiser")
            return
        
        wind_counts = Counter()
        heat_counts = Counter()
        
        for record in self.data:
            wind_level = record.get('wind_alert_level', 'level_0')
            heat_level = record.get('heat_alert_level', 'level_0')
            wind_counts[wind_level] += 1
            heat_counts[heat_level] += 1
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Alertes de vent
        levels = ['level_0', 'level_1', 'level_2']
        wind_values = [wind_counts[level] for level in levels]
        colors_wind = ['green', 'orange', 'red']
        ax1.bar(levels, wind_values, color=colors_wind, alpha=0.7)
        ax1.set_xlabel('Niveau d\'alerte', fontsize=12)
        ax1.set_ylabel('Nombre d\'alertes', fontsize=12)
        ax1.set_title('Alertes de vent par niveau', fontsize=13, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Alertes de chaleur
        heat_values = [heat_counts[level] for level in levels]
        colors_heat = ['lightblue', 'orange', 'darkred']
        ax2.bar(levels, heat_values, color=colors_heat, alpha=0.7)
        ax2.set_xlabel('Niveau d\'alerte', fontsize=12)
        ax2.set_ylabel('Nombre d\'alertes', fontsize=12)
        ax2.set_title('Alertes de chaleur par niveau', fontsize=13, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Graphique sauvegardé: {output_file}")
        plt.close()
    
    def plot_weathercode_by_country(self, output_file='weathercode_by_country.png'):
        """Graphique des codes météo les plus fréquents par pays"""
        if not self.data:
            print(" Pas de données à visualiser")
            return
        
        country_codes = defaultdict(Counter)
        
        for record in self.data:
            country = record.get('country', 'Unknown')
            weathercode = record.get('weathercode')
            if weathercode is not None:
                country_codes[country][weathercode] += 1
        
        # Préparer les données
        fig, ax = plt.subplots(figsize=(12, 6))
        
        countries = list(country_codes.keys())
        x = range(len(countries))
        
        # Obtenir le code le plus fréquent par pays
        most_common = []
        for country in countries:
            if country_codes[country]:
                code, count = country_codes[country].most_common(1)[0]
                most_common.append((code, count))
            else:
                most_common.append((0, 0))
        
        codes, counts = zip(*most_common) if most_common else ([], [])
        
        bars = ax.bar(x, counts, alpha=0.7, color='steelblue')
        ax.set_xlabel('Pays', fontsize=12)
        ax.set_ylabel('Fréquence du code météo', fontsize=12)
        ax.set_title('Code météo le plus fréquent par pays', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(countries, rotation=45, ha='right')
        ax.grid(True, alpha=0.3, axis='y')
        
        # Ajouter les codes au-dessus des barres
        for i, (bar, code) in enumerate(zip(bars, codes)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'Code {code}',
                   ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Graphique sauvegardé: {output_file}")
        plt.close()
    
    def generate_all_plots(self, output_dir='visualizations'):
        """Générer tous les graphiques"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n Génération des visualisations dans '{output_dir}'...\n")
        
        self.plot_temperature_evolution(output_path / 'temperature_evolution.png')
        self.plot_windspeed_evolution(output_path / 'windspeed_evolution.png')
        self.plot_alert_counts(output_path / 'alert_counts.png')
        self.plot_weathercode_by_country(output_path / 'weathercode_by_country.png')
        
        print(f"\n✓ Toutes les visualisations ont été générées!")
    
    def print_statistics(self):
        """Afficher des statistiques sur les données"""
        if not self.data:
            print(" Pas de données")
            return
        
        print("\n Statistiques des données:")
        print("=" * 80)
        
        # Compter par pays et ville
        countries = Counter(r.get('country', 'Unknown') for r in self.data)
        cities = Counter(r.get('city', 'Unknown') for r in self.data)
        
        print(f"\n Pays représentés: {len(countries)}")
        for country, count in countries.most_common():
            print(f"  - {country}: {count} enregistrements")
        
        print(f"\n  Villes représentées: {len(cities)}")
        for city, count in cities.most_common(10):  # Top 10
            print(f"  - {city}: {count} enregistrements")
        
        # Statistiques de température
        temps = [r['temperature'] for r in self.data if 'temperature' in r]
        if temps:
            print(f"\n  Température:")
            print(f"  - Moyenne: {sum(temps)/len(temps):.1f}°C")
            print(f"  - Min: {min(temps):.1f}°C")
            print(f"  - Max: {max(temps):.1f}°C")
        
        # Statistiques de vent
        winds = [r['windspeed'] for r in self.data if 'windspeed' in r]
        if winds:
            print(f"\n Vent:")
            print(f"  - Moyenne: {sum(winds)/len(winds):.1f} m/s")
            print(f"  - Min: {min(winds):.1f} m/s")
            print(f"  - Max: {max(winds):.1f} m/s")
        
        print("\n" + "=" * 80)


def main():
    # Configuration
    hdfs_base_path = '/home/claude/kafka-weather-project/data/hdfs-data'
    output_dir = '/home/claude/kafka-weather-project/data/visualizations'
    
    print(" Démarrage de la visualisation des données météo")
    print(f"   Base HDFS: {hdfs_base_path}")
    print(f"   Répertoire de sortie: {output_dir}\n")
    
    # Créer le visualiseur
    visualizer = WeatherVisualizer(hdfs_base_path)
    
    # Charger les données
    count = visualizer.load_data()
    
    if count == 0:
        print(" Aucune donnée trouvée dans HDFS")
        print("   Assurez-vous d'avoir exécuté le producteur et le consommateur HDFS")
        sys.exit(1)
    
    # Afficher les statistiques
    visualizer.print_statistics()
    
    # Générer les visualisations
    visualizer.generate_all_plots(output_dir)
    
    print(f"\n✓ Visualisation terminée!")
    print(f"   Consultez les graphiques dans: {output_dir}")


if __name__ == "__main__":
    main()