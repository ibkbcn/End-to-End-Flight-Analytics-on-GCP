# Usamos la versión estable de 2026
FROM apache/airflow:3.1.8

# Instalamos los proveedores de Google Cloud (esenciales para BigQuery)
RUN pip install --no-cache-dir apache-airflow-providers-google