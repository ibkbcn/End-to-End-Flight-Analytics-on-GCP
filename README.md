# ✈️ End-to-End Flight Analytics on GCP

Este proyecto es un pipeline de ingeniería de datos robusto que integra datos históricos y en tiempo real de vuelos utilizando una arquitectura **Medallion** (Bronze/Silver). 

## Arquitectura del Proyecto
El pipeline orquestado por **Apache Airflow** realiza las siguientes etapas:

1.  **Ingestión Híbrida (Bronze):**
    * **Batch:** Carga de datos históricos (CSV) desde Google Cloud Storage a BigQuery.
    * **Real-time:** Consumo de la API de *Aviationstack* para capturar vuelos actuales.
2.  **Transformación (Silver):** Limpieza, tipado y unificación de fuentes mediante SQL en BigQuery.
3.  **Visualización:** Dashboard interactivo en Looker Studio.

## Tecnologías Utilizadas
* **Orquestación:** Apache Airflow (Dockerized)
* **Cloud:** Google Cloud Platform (BigQuery & GCS)
* **Lenguajes:** Python (Pandas, Requests) & SQL
* **Contenedores:** Docker & Docker Compose
* **Seguridad:** GitIgnore para protección de Service Accounts y API Keys.

## Configuración y Ejecución

### Requisitos Previos
* Docker & Docker Compose instalado.
* Cuenta en Google Cloud Platform con una Service Account (`.json`).
* API Key de Aviationstack.

### Instalación
1. Clonar el repositorio:
   ```bash
   git clone [https://github.com/ibkbcn/End-to-End-Flight-Analytics-on-GCP.git](https://github.com/ibkbcn/End-to-End-Flight-Analytics-on-GCP.git)
   cd End-to-End-Flight-Analytics-on-GCP
