from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_values
import os
os.environ['PGCLIENTENCODING'] = 'UTF8'
os.environ['LC_ALL'] = 'C'

# Configuration Kafka
kafka_bootstrap_servers = ['localhost:9092']
topic_name = 'factures_telecom'


# Configuration PostgreSQL
db_params = {
    'dbname': 'telecomdb',  # Utiliser 'telecomdb' au lieu de 'telecom_db'
    'user': 'postgres',
    'password': 'otmane',
    'host': 'localhost',
    'port': '5432'
}

# Initialiser le consommateur Kafka
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Connexion à PostgreSQL
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Connexion à PostgreSQL réussie !")
except psycopg2.Error as e:
    print(f"Erreur de connexion à PostgreSQL : {str(e).encode('utf-8', errors='replace').decode('utf-8')}")
    exit(1)

# Créer la table si elle n'existe pas
create_table_query = """
CREATE TABLE IF NOT EXISTS invoices (
    identifiant_client VARCHAR(36) PRIMARY KEY,
    cycle_facturation VARCHAR(7),
    nombre_total_evenements INTEGER,
    nombre_appels_vocaux INTEGER,
    nombre_sms INTEGER,
    nombre_donnees INTEGER,
    nombre_sms_excedentaires INTEGER,
    tarif_base DECIMAL(15, 2),
    tarif_sms_excedentaires DECIMAL(15, 2),
    tarif_ajuste DECIMAL(15, 2),
    montant_remise DECIMAL(15, 2),
    montant_taxable DECIMAL(15, 2),
    montant_tva DECIMAL(15, 2),
    frais_fixes DECIMAL(15, 2),
    montant_total_a_payer DECIMAL(15, 2)
);
"""
cursor.execute(create_table_query)
conn.commit()

# Fonction pour insérer une facture
def insert_invoice(data):
    insert_query = """
    INSERT INTO invoices (
        identifiant_client, cycle_facturation, nombre_total_evenements,
        nombre_appels_vocaux, nombre_sms, nombre_donnees, nombre_sms_excedentaires,
        tarif_base, tarif_sms_excedentaires, tarif_ajuste, montant_remise,
        montant_taxable, montant_tva, frais_fixes, montant_total_a_payer
    ) VALUES %s
    ON CONFLICT (identifiant_client) DO UPDATE
    SET cycle_facturation = EXCLUDED.cycle_facturation,
        nombre_total_evenements = EXCLUDED.nombre_total_evenements,
        nombre_appels_vocaux = EXCLUDED.nombre_appels_vocaux,
        nombre_sms = EXCLUDED.nombre_sms,
        nombre_donnees = EXCLUDED.nombre_donnees,
        nombre_sms_excedentaires = EXCLUDED.nombre_sms_excedentaires,
        tarif_base = EXCLUDED.tarif_base,
        tarif_sms_excedentaires = EXCLUDED.tarif_sms_excedentaires,
        tarif_ajuste = EXCLUDED.tarif_ajuste,
        montant_remise = EXCLUDED.montant_remise,
        montant_taxable = EXCLUDED.montant_taxable,
        montant_tva = EXCLUDED.montant_tva,
        frais_fixes = EXCLUDED.frais_fixes,
        montant_total_a_payer = EXCLUDED.montant_total_a_payer;
    """
    values = [
        (
            data['identifiant_client'], data['cycle_facturation'], data['nombre_total_evenements'],
            data['nombre_appels_vocaux'], data['nombre_sms'], data['nombre_donnees'],
            data['nombre_sms_excedentaires'], data['tarif_base'], data['tarif_sms_excedentaires'],
            data['tarif_ajuste'], data['montant_remise'], data['montant_taxable'],
            data['montant_tva'], data['frais_fixes'], data['montant_total_a_payer']
        )
    ]
    execute_values(cursor, insert_query, values)
    conn.commit()

# Consommer les messages
try:
    for message in consumer:
        invoice_data = message.value
        print(f"Received invoice: {invoice_data}")
        insert_invoice(invoice_data)
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    cursor.close()
    conn.close()
    consumer.close()