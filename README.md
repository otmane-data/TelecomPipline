# Pipeline Télécom : Génération, Médiation, Tarification, Stockage et Visualisation

## Présentation
Ce projet met en œuvre un pipeline complet pour la gestion de données télécoms, de la génération à la visualisation :
- **Génération** : Création de données simulées via un générateur Python.
- **Médiation** : Transmission des données par Kafka.
- **Tarification** : Traitement et calcul des factures avec Spark.
- **Stockage** : Insertion des factures dans une base PostgreSQL.
- **Visualisation** : Affichage interactif des factures avec Streamlit.

## Architecture

![Architecture du projet](sets/Diagramme%20sans%20nom.drawio%20(2).png)

```
[Data Generator] → [Kafka] → [Spark Streaming] → [PostgreSQL] → [Streamlit App]
```

## Prérequis
- Python 3.8+
- Docker & Docker Compose
- Apache Kafka
- Apache Spark
- PostgreSQL
- Streamlit

## Installation
1. **Cloner le dépôt**
```bash
git clone <repo_url>
cd big-data
```
2. **Lancer les services avec Docker Compose**
```bash
docker-compose up --build
```
Cela démarre Kafka, PostgreSQL, Jupyter/Spark, etc.

3. **Installer les dépendances Python**
```bash
pip install -r requirements.txt
```

## Lancement des modules
- **Générateur de données** :
  - Voir `src/data_generator/generator.py` pour générer et envoyer des événements vers Kafka.
- **Consommateur Kafka & Insertion PostgreSQL** :
  - Exécuter :
    ```bash
    python consomateur.py
    ```
- **Traitement Spark** :
  - Utiliser le notebook `data/otmane.ipynb` pour lire depuis Kafka, traiter et produire les factures.
- **Visualisation Streamlit** :
  - Lancer :
    ```bash
    streamlit run streamlit_app.py
    ```
  - Accéder à l'interface sur [http://localhost:8501](http://localhost:8501)

## Exemple d'utilisation
- Les factures générées sont stockées dans la table `invoices` de PostgreSQL.
- L'application Streamlit permet de visualiser et d'explorer les factures en temps réel.

## Structure du projet
- `src/data_generator/` : Génération de données simulées
- `consomateur.py` : Consommateur Kafka et insertion en base
- `data/otmane.ipynb` : Traitement Spark
- `streamlit_app.py` : Visualisation
- `docker-compose.yml` : Orchestration des services

## Auteurs
- Projet réalisé par Otmane et collaborateurs.

## Licence
MIT