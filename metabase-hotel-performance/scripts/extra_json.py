import json
import pandas as pd
import pymysql
from datetime import datetime

# Fonction pour convertir ISO 8601 '2024-05-27T10:30:00Z' en format MySQL 'YYYY-MM-DD HH:MM:SS'
def convert_iso_to_mysql_datetime(iso_str):
    if iso_str is None:
        return None
    dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# champ charger le fichier JSON
with open("ca_metabase.json", "r", encoding='utf-8') as file:
    data = json.load(file)  # Charge le contenu JSON en dictionnaire Python

# champ d'extraction des enregistrements
records = []
for date in data["data"]:
    for user_id, values in data["data"][date].items():
        # Conversion du timestamp avant ajout
        if "timestamp" in values:
            values["timestamp"] = convert_iso_to_mysql_datetime(values["timestamp"])
        records.append(values)

df = pd.DataFrame(records)

# Connexion à la base MySQL 
connection = pymysql.connect(
    host='localhost',
    user='user_djeneba',
    password='Doumbia2002@',
    database='metabase',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

cursor = connection.cursor()

# Insertion des données dans la table hotel_performance
for _, row in df.iterrows():
    placeholders = ', '.join(['%s'] * len(row))
    columns = ', '.join(row.index)
    sql = f"REPLACE INTO hotel_performance ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, tuple(row))




connection.commit()
cursor.close()
connection.close()

print("Données insérées  dans MySQL.")
