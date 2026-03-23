from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta  
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
import requests
import pandas as pd


def llamar_api_vuelos(**kwargs):
    params = {
        'access_key': 'TU_API_KEY_AQUI',
        'limit': 100 # Máximo del plan free
    }
    api_result = requests.get('http://api.aviationstack.com/v1/flights', params)
    api_response = api_result.json()
    
    # Extraemos solo lo que nos interesa del JSON
    vuelos_lista = []
    for flight in api_response['data']:
        vuelos_lista.append({
            'fecha': flight['flight_date'],
            'aerolinea': flight['airline']['name'],
            'origen': flight['departure']['iata'],
            'destino': flight['arrival']['iata'],
            'retraso_salida': flight['departure']['delay']
        })
    
    # Lo convertimos a DataFrame y lo subimos a una tabla "Bronze_API"
    df = pd.DataFrame(vuelos_lista)
    # Aquí usaríamos el método to_gbq para mandarlo a BigQuery
    df.to_gbq('vuelos_ds.vuelos_api_raw', project_id='project-7d60b9cf-b155-44d5-990', if_exists='replace')


default_args = {
    'owner': 'Ivan_2026',
    'start_date': datetime(2026, 3, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'carga_vuelos_gcs_a_bigquery',
    default_args=default_args,
    description='Mueve datos de vuelos de GCS a BQ con vigilancia',
    schedule='@daily',
    catchup=False,
    tags=['vuelos', 'edreams'],
) as dag:
    

    tarea_api = PythonOperator(
        task_id='obtener_datos_api_vuelos',
        python_callable=llamar_api_vuelos, # <-- Llama a la función de arriba
    )

    # TAREA 1: El Vigilante (Sensor)
    # Nota: Aquí usamos google_cloud_conn_id
    esperar_archivo_vuelos = GCSObjectExistenceSensor(
        task_id='esperar_archivo_csv',
        bucket='edreams-vuelos-data',
        object='flight_data_2024.csv',
        google_cloud_conn_id='google_cloud_default',
        timeout=600,
        poke_interval=30,
    )

    # TAREA 2: La Carga (Lo que ya te funcionaba)
    # Nota: Aquí usamos gcp_conn_id
    tarea_cargar_csv = GCSToBigQueryOperator(
        task_id='cargar_csv_vuelos',
        bucket='edreams-vuelos-data',
        source_objects=['flight_data_2024.csv'],
        destination_project_dataset_table='project-7d60b9cf-b155-44d5-990.vuelos_ds.performance_2024',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    # TAREA 3: De Bronze a Silver (Limpieza y Lógica)
    
    tarea_crear_silver = BigQueryInsertJobOperator(
            task_id='transformar_vuelos_a_silver',
            configuration={
                "query": {
                    "query": """
                        CREATE OR REPLACE TABLE `project-7d60b9cf-b155-44d5-990.vuelos_ds.vuelos_silver` AS
                        
                        -- PARTE 1: Los datos del CSV (Histórico)
                        SELECT DISTINCT
                            CAST(FL_DATE AS DATE) as fecha,
                            OP_UNIQUE_CARRIER as aerolinea,
                            ORIGIN as origen,
                            DEST as destino,
                            CAST(DEP_DELAY AS FLOAT64) as retraso_salida,
                            CAST(ARR_DELAY AS FLOAT64) as retraso_llegada,
                            IF(CAST(ARR_DELAY AS FLOAT64) > 15, 1, 0) as is_delayed,
                            'KAGGLE_CSV' as fuente  -- <--- IDENTIFICADOR
                        FROM 
                            `project-7d60b9cf-b155-44d5-990.vuelos_ds.performance_2024`
                        WHERE 
                            OP_UNIQUE_CARRIER IS NOT NULL AND FL_DATE IS NOT NULL
                        
                        UNION ALL

                        -- PARTE 2: Los datos de la API (Tiempo Real)
                        SELECT 
                            CAST(fecha AS DATE) as fecha,
                            aerolinea,
                            origen,
                            destino,
                            CAST(retraso_salida AS FLOAT64) as retraso_salida,
                            NULL as retraso_llegada,
                            IF(CAST(retraso_salida AS FLOAT64) > 15, 1, 0) as is_delayed,
                            'AVIATION_API' as fuente -- <--- IDENTIFICADOR
                        FROM 
                            `project-7d60b9cf-b155-44d5-990.vuelos_ds.vuelos_api_raw`
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default',
        )

    # Ahora son 3 cajas conectadas en serie
    esperar_archivo_vuelos >> [tarea_cargar_csv, tarea_api] >> tarea_crear_silver

    # Nota: Este codigo es de dessarrollo
    # En la tabla silver se aplicaria merge para juntar con datos anteriores y mantener un historico, pero para el ejemplo lo dejamos asi
    # se agregar columna fuente para saber de donde viene cada dato (csv o api) y asi poder hacer analisis posteriores de calidad de datos, retrasos, etc.