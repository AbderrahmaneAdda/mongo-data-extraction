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

    db = client['databaseName']
    collection = db['collectionName']
    dataset_id = 'datasetName'
    table_id = 'tableName'

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

    for resultat in resultats_aggregation:
        # Extraction de l'id et le reste des données du document
        document_data = {"_id": str(resultat["_id"])}
        document_data.update(resultat["contactRequests"])
        document_data.update(resultat["contactRequests"]["sender"])

        data.append(document_data)

    #création Dataframe
    df = pd.DataFrame(data)
    df = df.astype(str)

    df.rename(columns=
            {
                'column1': 'columnRename1',
                'column2': 'columnRename2',
                'column3': 'columnRename3'
            }, inplace=True)


    #Ne garder que les colonnes voulus + cleaning
    columns_to_keep = [col for col in df.columns if col in lc.contactRequests]
    df_final = df[columns_to_keep].replace('nan', np.nan)


    # Exporter le DataFrame vers BigQuery
    # Créer une connexion au client BigQuery
    client_bq = bigquery.Client()

    #Table de destination BQ
    table_ref = client_bq.dataset(dataset_id).table(table_id)

    # Créer job d'insertion
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

    # Charger données dans BigQuery
    job = client_bq.load_table_from_dataframe(df_final, table_ref, job_config=job_config)

    job.result()

    print(f"Données insérées avec succès dans {table_id} dans le dataset {dataset_id} de BigQuery.")
    return 'Done'