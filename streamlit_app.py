import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Paramètres de connexion à la base PostgreSQL
db_params = {
    'dbname': 'telecomdb',
    'user': 'postgres',
    'password': 'otmane',
    'host': 'localhost',
    'port': '5432'
}

def get_data():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM invoices')
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur de connexion ou de récupération des données : {e}")
        return pd.DataFrame()

st.title("Visualisation des factures télécom")

st.write("Connexion à la base de données et récupération des données...")
data = get_data()

if not data.empty:
    st.dataframe(data)
    st.write(f"Nombre total de factures : {len(data)}")
    # Optionnel : Ajout de graphiques
    if 'montant_total_a_payer' in data.columns:
        st.bar_chart(data['montant_total_a_payer'])
else:
    st.warning("Aucune donnée à afficher ou erreur de connexion.")