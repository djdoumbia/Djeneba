import json
import pandas as pd
import pymysql
from datetime import datetime

# Fonction pour convertir les dates au format MySQL
def convert_date(date_str):
    if date_str is None:
        return None
    return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')

# Charger le fichier JSON
with open("ra.json", "r", encoding='utf-8') as file:
    data = json.load(file)

# Fonction pour extraire les données de vente
def extract_ventes_data(data, periode_type):
    ventes = data['stRA'][f'stVente{periode_type.capitalize()}']
    dates = {
        'jour': (data['stRA']['dDateDebutJour'], data['stRA']['dDateFinJour']),
        'mois': (data['stRA']['dDateDebutMois'], data['stRA']['dDateFinMois']),
        'cumul': (data['stRA']['dDateDebutCumul'], data['stRA']['dDateFinCumul'])
    }
    
    record = {
        'periode_type': periode_type,
        'date_debut': convert_date(dates[periode_type][0]),
        'date_fin': convert_date(dates[periode_type][1]),
        'nb_chambres': ventes['nNbChambre'],
        'nb_chambres_hors_to': ventes['nNbChambreHorsTO'],
        'nb_chambres_corbeilles': ventes.get('nNBChambresCorbeilles', 0), #Si la clé "nNBChambresCorbeilles" n’existe pas → on retourne 0
        'nb_nuitees': ventes['nNbNuitee'],
        'nb_pdj': ventes['nNbPDJ'],
        'taux_occupation': ventes['nTO'],
        'taux_occupation_personne': ventes['nTOPersonne'],
        'prix_moyen': ventes['nPM'],
        'prix_moyen_hbgt': ventes['nPMHBGT'],
        'revpar_brut': ventes['nRevParBrut'],
        'trevpar': ventes['nTrevPar'],
        'arpar': ventes['nARPAR'],
        'pdj_par_nuitee': ventes['nPDJParNuitee'],
        'pdj_prix_moyen': ventes['nPDJPrixMoyen'],
        'couvert_midi': ventes['nCouvertMidi'],
        'couvert_soir': ventes['nCouvertSoir'],
        'ca_midi': ventes['moCAMidi'],
        'ca_soir': ventes['moCASoir']
    }
    
    # # Ajouter les types de nuitee comme une colonne JSON
    # record['types_nuitee'] = json.dumps(ventes['taTypeNuitee'])
    
    return record

def extract_typeNuitee_data(taTypeNuitee_dico, vente_id):
    return [
        {
            "vente_id": vente_id,
            "type_nuitee": nom,
            "quantite": quantite
        }
        for nom, quantite in taTypeNuitee_dico.items()
    ]


# Fonction pour extraire les données CA
def extract_ca_data(data, periode_type):
    ca = data['stRA'][f'stCA{periode_type.capitalize()}']
    dates = {
        'jour': (data['stRA']['dDateDebutJour'], data['stRA']['dDateFinJour']),
        'mois': (data['stRA']['dDateDebutMois'], data['stRA']['dDateFinMois']),
        'cumul': (data['stRA']['dDateDebutCumul'], data['stRA']['dDateFinCumul'])
    }
    
    record = {
        'periode_type': periode_type,
        'date_debut': convert_date(dates[periode_type][0]),
        'date_fin': convert_date(dates[periode_type][1]),
        'ca_chambre': ca['stCAHbgt']['moCAChambre'],
        'ca_chambre_forfait': ca['stCAHbgt']['moCAChambreForfait'],
        'no_show': ca['stCAHbgt'].get('moNoShow', 0),
        'ca_pdj': ca['stCAPdj']['moCAPdj'],
        'ca_pdj_forfait': ca['stCAPdj']['moCAPdjForfait'],
        'ca_carte': ca['stCAResto']['moCACarte'],
        'ca_menu': ca['stCAResto']['moCAMenu'],
        'ca_resto_forfait': ca['stCAResto'].get('moCARestoForfait', 0),
        'ca_traiteur': ca['stCAResto'].get('moCATraiteur', 0),
        'ca_vin': ca['stCABar']['moCAVin'],
        'ca_spa': ca.get('moCASPA', 0),
        'taxe_sejour': ca.get('moTaxeDeSejour', 0),
        'ca_ht': ca['rCAHT'],
        'ca_ttc': ca['rCATTC'],
        'remise_hotel': ca.get('moRemiseHotel', 0),
        'remise_tpv': ca.get('moRemiseTPV', 0),
        'montant_gratuit': ca.get('moMontantGratuit', 0),
        'montant_offert': ca.get('moMontantOffert', 0)
    }
    
    # # Ajouter les données TVA
    # if 'tablTVA' in ca:
    #     record['tva_details'] = json.dumps(ca['tablTVA'])
    
    return record

# Fonction pour extraire les données TPV
def extract_tpv_data(tpv_data):
    records = []
    for periode_type in ['Jour', 'Mois', 'Cumul']:
        if f'stVente{periode_type}' in tpv_data['stRA']:
            vente = tpv_data['stRA'][f'stVente{periode_type}']
            dates = {
                'Jour': (tpv_data['stRA']['dDateDebutJour'], tpv_data['stRA']['dDateFinJour']),
                'Mois': (tpv_data['stRA']['dDateDebutMois'], tpv_data['stRA']['dDateFinMois']),
                'Cumul': (tpv_data['stRA']['dDateDebutCumul'], tpv_data['stRA']['dDateFinCumul'])
            }
            
            record = {
                'tpv_id': tpv_data['nIDTPV'],
                'tpv_nom': tpv_data['sLibelleTPV'],
                'periode_type': periode_type.lower(),
                'date_debut': convert_date(dates[periode_type][0]),
                'date_fin': convert_date(dates[periode_type][1]),
                'nb_chambres': vente.get('nNbChambre', 0),
                'nb_chambres_hors_to': vente.get('nNbChambreHorsTO', 0),
                'nb_chambres_corbeilles': vente.get('nNBChambresCorbeilles', 0),
                'nb_nuitees': vente.get('nNbNuitee', 0),
                'nb_pdj': vente.get('nNbPDJ', 0),
                'couvert_midi': vente.get('nCouvertMidi', 0),
                'couvert_soir': vente.get('nCouvertSoir', 0),
                'ca_midi': vente.get('moCAMidi', 0),
                'ca_soir': vente.get('moCASoir', 0)
            }
            records.append(record)
    return records


# Étape 1 : Extraction des données séparées
ventes_records = []
ca_records = []
ventes_tpv_records = []


for periode_type in ['jour', 'mois', 'cumul']:
    ventes_records.append(extract_ventes_data(data, periode_type))

    ca_records.append(extract_ca_data(data, periode_type))
if 'tablRATPV' in data:
    for tpv_data in data['tablRATPV']:
        ventes_tpv_records.extend(extract_tpv_data(tpv_data))

# Étape 2 : Création des DataFrames
df_ventes = pd.DataFrame(ventes_records)
# df_ca = pd.DataFrame(ca_records)
# df_ventes_tpv = pd.DataFrame(ventes_tpv_records)

# Étape 3 : Connexion à MySQL
connection = pymysql.connect(
    host='localhost',
    user='user_djeneba',
    password='Doumbia2002@',
    database='metabase',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def insert_dataframe(df, table_name, connection):
    with connection.cursor() as cursor:
        for _, row in df.iterrows():
            row = row.where(pd.notnull(row), None)
            columns = ', '.join(row.index)
            placeholders = ', '.join(['%s'] * len(row))
            sql = f"""
                INSERT INTO {table_name}
                ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
            """
            updates = ', '.join([f"{col}=VALUES({col})" for col in row.index])
            sql += updates
            cursor.execute(sql, tuple(row.values))
            vente_id = cursor.lastrowid   # la cle de la ligne vente inserer 

            # Insertion des types_nuitee liés
            periode_type = row['periode_type']
            ta_type = data['stRA'][f'stVente{periode_type.capitalize()}'].get('taTypeNuitee', {})
            for nom, quantite in ta_type.items():
                cursor.execute(
                    "INSERT INTO types_nuitee (vente_id, type_nuitee, quantite) VALUES (%s, %s, %s)",
                    (vente_id, nom, quantite)
                )

            

        connection.commit()
        print(f"{len(df)} enregistrements insérés dans {table_name}")

# Étape 4 : Insertion dans les bonnes tables
insert_dataframe(df_ventes, "ventes", connection)
# # insert_dataframe(df_ca, "chiffre_affaire", connection)
# insert_dataframe(df_ventes_tpv, "ventes_tpv", connection)

connection.close()
