"""Configuration for the CDR/EDR generator."""

# Distribution des types d'événements (total doit être = 1)
EVENT_DISTRIBUTION = {
    'voice': 0.4,
    'sms': 0.3,
    'data': 0.3
}

# Paramètres de génération
BATCH_SIZE = 1000
ERROR_RATE = 0.05  # 5% d'erreurs
MISSING_DATA_RATE = 0.03  # 3% de données manquantes
DUPLICATE_RATE = 0.02  # 2% de doublons

# Plages de valeurs
DURATION_RANGE = {
    'voice': (10, 3600),  # 10 secondes à 1 heure
    'sms': (1, 1),       # toujours 1 seconde
    'data': (60, 7200)   # 1 minute à 2 heures
}

DATA_VOLUME_RANGE = {
    'data': (1024, 1024*1024*1024)  # 1KB à 1GB
}

# Technologies par type de service
TECHNOLOGIES = {
    'voice': ['2G', '3G', '4G', '5G'],
    'sms': ['2G', '3G', '4G'],
    'data': ['3G', '4G', '5G']
}

# Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9093',
    'topic': 'telecom_events'
}

# Configuration des fichiers de sortie
OUTPUT_CONFIG = {
    'json_path': '../output/events.json',
    'csv_path': '../output/events.csv'
}

# Nombre de cell_ids à générer
CELL_ID_COUNT = 1000
