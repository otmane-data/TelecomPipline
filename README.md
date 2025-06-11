📡 Pipeline Télécom – Génération, Médiation, Tarification, Stockage & Visualisation

🧾 Présentation

Ce projet implémente un pipeline complet de traitement de données dans un contexte télécom, en allant de la génération de données simulées jusqu’à leur visualisation interactive.

🔄 Étapes principales :

🛠️ Génération : Création de données télécom simulées en Python.

📡 Médiation : Envoi des événements via Apache Kafka.

⚙️ Tarification : Traitement des événements et génération de factures avec Apache Spark.

🗄️ Stockage : Insertion des factures dans PostgreSQL.

📊 Visualisation : Affichage dynamique via une application Streamlit.

🧱 Architecture

 ![Architecture du projet](sets/Diagramme%20sans%20nom.drawio%20(2).png)

text
Copier
Modifier
[Data Generator] → [Kafka] → [Spark Streaming] → [PostgreSQL] → [Streamlit App]
🔧 Prérequis
Assurez-vous d’avoir les outils suivants installés :

Python ≥ 3.8

Docker & Docker Compose

Apache Kafka

Apache Spark

PostgreSQL

Streamlit

🚀 Installation
1. Cloner le dépôt
bash
Copier
Modifier
git clone <repo_url>
cd big-data
2. Lancer les services avec Docker Compose
bash
Copier
Modifier
docker-compose up --build
Cela démarre les services nécessaires (Kafka, PostgreSQL, Spark, etc.).

3. Installer les dépendances Python
bash
Copier
Modifier
pip install -r requirements.txt
⚙️ Lancement des modules
▶️ Générateur de données
Script de simulation d’événements envoyés à Kafka :

bash
Copier
Modifier
python src/data_generator/generator.py
▶️ Consommateur Kafka → PostgreSQL

Consomme les messages Kafka et les insère dans la base PostgreSQL :

bash
Copier
Modifier
python consomateur.py
▶️ Traitement Spark (Tarification)

Exécuter le traitement et la génération de factures dans :

bash
Copier
Modifier
data/otmane.ipynb
Ce notebook Spark lit depuis Kafka, applique la logique métier et produit les factures.

▶️ Visualisation avec Streamlit
Lancer l’application :

bash
Copier
Modifier
streamlit run streamlit_app.py
Accessible via http://localhost:8501

big-data/

├── src/

│   └── data_generator/         # Générateur de données simulées

├── consomateur.py              # Kafka → PostgreSQL

├── data/

│   └── otmane.ipynb            # Traitement Spark

├── streamlit_app.py            # Interface Streamlit

├── requirements.txt            # Dépendances Python

├── docker-compose.yml          # Services Docker

└── README.md                   # Ce fichier

🧪 Exemple d’utilisation

Les factures télécom sont enregistrées dans la table invoices de la base PostgreSQL.

L’interface Streamlit permet :

🔍 de filtrer les clients,

📈 de visualiser les montants par cycle,

📄 d’explorer les détails de chaque facture.

👥 Auteurs
Projet réalisé par Otmane et collaborateurs, dans le cadre d’un projet Big Data.
