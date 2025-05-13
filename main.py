from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
import list_columns as lc
import numpy as np



def main(request):

    #Parameters
    uri = 'insert mongo_uri'
    client = MongoClient(uri)

    db = client['kimono']

    collection = db['real_estate_ads']

    dataset_id = 'raw_mongodb'
    table_id = 'contactRequests'

    def get_max_date_from_bigquery(client, dataset_id, table_id):
        query = f"SELECT MAX(cast(date as datetime)) AS max_date FROM `{dataset_id}.{table_id}`"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row['max_date']

    client_bq = bigquery.Client()

    max_date = get_max_date_from_bigquery(client_bq, dataset_id, table_id)


    # Construire la requête d'agrégation
    requete_aggregation = [

    {
            "$match": {
                "contactRequests.date": {
                    "$gt": max_date

                }
            }
        },

        {
            "$project" : {
                    "_id": 1,
                    "contactRequests": 1
                },

        },

        {
            "$unwind": "$contactRequests"  # Décompose les tableaux contactRequests en documents individuels
        },

        {
            "$match": {
                "contactRequests.date": {
                    "$gt": max_date

                }
            }
        },
    ]


    # Exécuter la requête d'agrégation
    resultats_aggregation = collection.aggregate(requete_aggregation)

    # Initialiser une liste pour stocker les données
    data = []

    # Parcourir les résultats et les ajouter à la liste
    # for resultat in resultats_aggregation:
    #     data.append(resultat)


    for resultat in resultats_aggregation:
        # Extraire l'ID et le reste des données du document
        document_data = {"_id": str(resultat["_id"])}
        document_data.update(resultat["contactRequests"])
        document_data.update(resultat["contactRequests"]["sender"])

        data.append(document_data)


    # Créer un DataFrame Pandas
    df = pd.DataFrame(data)

    # Convertir toutes les colonnes du DataFrame en chaînes de caractères
    df = df.astype(str)

    df.rename(columns=
            {
                'column1': 'columnRename1',
                'column2': 'columnRename2',
                'column3': 'columnRename3'
            }, inplace=True)


    #Ne garder que les colonnes voulus
    #liste des colonnes du df
    df_columns_list = df.columns.values
    #liste des colonnes que l'on veut garder
    columns_to_keep = lc.contactRequests


    list_to_drop = []
    for i in df_columns_list:
        if i not in columns_to_keep:
            list_to_drop.append(i)
        else:
            pass


    df_final = df.drop(list_to_drop, axis=1)

    df_final = df_final.replace('nan',np.nan)


    # Exporter le DataFrame vers BigQuery
    # Créer une connexion au client BigQuery
    client_bq = bigquery.Client()

    # Définir les informations de destination dans BigQuery

    # Créer un objet de référence de table BigQuery
    table_ref = client_bq.dataset(dataset_id).table(table_id)

    # Créer un job d'insertion
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

    # Charger les données dans BigQuery
    job = client_bq.load_table_from_dataframe(df_final, table_ref, job_config=job_config)

    job.result()

    print(f"Données insérées avec succès dans {table_id} dans le dataset {dataset_id} de BigQuery.")
    return 'Done'
