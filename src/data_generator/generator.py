"""Générateur de données CDR/EDR."""
import random
import uuid
from datetime import datetime, timedelta
import json
import pandas as pd
from faker import Faker
from kafka import KafkaProducer
from typing import List, Optional
import os

from config import (
    EVENT_DISTRIBUTION, BATCH_SIZE, ERROR_RATE,
    MISSING_DATA_RATE, DUPLICATE_RATE, DURATION_RANGE,
    DATA_VOLUME_RANGE, TECHNOLOGIES, KAFKA_CONFIG,
    OUTPUT_CONFIG, CELL_ID_COUNT
)
from models import TelecomEvent

class TelecomEventGenerator:
    def __init__(self):
        """Initialise le générateur d'événements."""
        self.fake = Faker()
        self.cell_ids = [f"CELL_{i:05d}" for i in range(CELL_ID_COUNT)]
        self.user_ids = [self.fake.uuid4() for _ in range(20)]  # 20 utilisateurs uniques
        self.kafka_producer = self._setup_kafka() if KAFKA_CONFIG['bootstrap_servers'] else None

    def _setup_kafka(self) -> Optional[KafkaProducer]:
        """Configure le producteur Kafka."""
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except Exception as e:
            print(f"Erreur lors de la configuration de Kafka: {e}")
            return None

    def generate_event(self, user_id: str) -> TelecomEvent:
        """Génère un seul événement de télécommunication pour un utilisateur donné."""
        event_type = random.choices(
            list(EVENT_DISTRIBUTION.keys()),
            weights=list(EVENT_DISTRIBUTION.values())
        )[0]

        # Génération des champs de base
        event = TelecomEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            event_type=event_type,
            user_id=user_id,
            destination_id=self.fake.uuid4(),
            duration=random.uniform(*DURATION_RANGE[event_type]) if event_type != 'sms' else 1,
            volume=random.randint(*DATA_VOLUME_RANGE['data']) if event_type == 'data' else None,
            cell_id=random.choice(self.cell_ids),
            technology=random.choice(TECHNOLOGIES[event_type]),
            status='completed',
            error_code=None
        )

        # Introduction d'erreurs aléatoires
        if random.random() < ERROR_RATE:
            event.status = 'failed'
            event.error_code = f"ERR_{random.randint(100, 999)}"

        # Introduction de données manquantes
        if random.random() < MISSING_DATA_RATE:
            fields = ['destination_id', 'duration', 'volume', 'cell_id']
            field_to_nullify = random.choice(fields)
            setattr(event, field_to_nullify, None)

        return event

    def generate_batch(self, size: int = BATCH_SIZE) -> List[TelecomEvent]:
        """Génère un lot d'événements pour 20 utilisateurs avec plusieurs événements chacun."""
        events = []
        events_per_user = max(1, size // len(self.user_ids))  # Nombre minimum d'événements par utilisateur

        for user_id in self.user_ids:
            # Générer plusieurs événements par utilisateur
            user_events = [self.generate_event(user_id) for _ in range(events_per_user)]
            events.extend(user_events)

        # Ajuster pour atteindre ou approcher BATCH_SIZE si nécessaire
        remaining_events = size - len(events)
        if remaining_events > 0:
            additional_users = self.user_ids[:remaining_events]  # Réutiliser certains utilisateurs
            for user_id in additional_users:
                events.append(self.generate_event(user_id))

        # Introduction de doublons
        if random.random() < DUPLICATE_RATE:
            duplicate_count = int(len(events) * DUPLICATE_RATE)
            events.extend(random.choices(events, k=duplicate_count))

        # Mélange des événements pour des timestamps désordonnés
        random.shuffle(events)
        return events[:size]  # Tronquer à BATCH_SIZE si dépassé

    def save_to_json(self, events: List[TelecomEvent], filepath: str):
        """Sauvegarde les événements au format JSON."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump([event.to_dict() for event in events], f, indent=2)

    def save_to_csv(self, events: List[TelecomEvent], filepath: str):
        """Sauvegarde les événements au format CSV."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df = pd.DataFrame([event.to_dict() for event in events])
        df.to_csv(filepath, index=False)

    def send_to_kafka(self, event: TelecomEvent):
        """Envoie un événement à Kafka avec gestion d'erreurs."""
        if not self.kafka_producer:
            print("Kafka producer non configuré, envoi ignoré.")
            return
        try:
            self.kafka_producer.send(
                KAFKA_CONFIG['topic'],
                value=event.to_dict()
            ).get(timeout=30)  # Attendre la confirmation avec un timeout
            print(f"Événement envoyé à Kafka: {event.event_id}")
        except Exception as e:
            print(f"Erreur lors de l'envoi à Kafka: {e}")

    def close_kafka(self):
        """Ferme proprement le producteur Kafka."""
        if self.kafka_producer:
            try:
                self.kafka_producer.flush()
                self.kafka_producer.close()
                print("✅ Kafka producer fermé proprement.")
            except Exception as e:
                print(f"❌ Erreur lors de la fermeture de Kafka: {e}")

    def run(self, mode: str = 'both', batch_size: int = BATCH_SIZE):
        """
        Exécute le générateur dans le mode spécifié.
        mode: 'batch', 'stream', ou 'both'
        """
        events = self.generate_batch(batch_size)

        if mode in ['batch', 'both']:
            self.save_to_json(events, OUTPUT_CONFIG['json_path'])
            self.save_to_csv(events, OUTPUT_CONFIG['csv_path'])
            print(f"Générés {len(events)} événements en mode batch")

        if mode in ['stream', 'both'] and self.kafka_producer:
            for event in events:
                self.send_to_kafka(event)
            print(f"Envoyés {len(events)} événements à Kafka")

if __name__ == "__main__":
    generator = TelecomEventGenerator()
    generator.run(mode='both')
    generator.close_kafka()