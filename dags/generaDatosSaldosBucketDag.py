from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import os
from pathlib import Path
from airflow.sdk import Variable
from include.dominio_generaDatosSaldosBucket.task.obtener_metadatos_y_calendario import (
    obtener_metadatos_y_calendario,
)


def get_dag_docs():
    """
    Lee la documentación del DAG desde un archivo externo .md.
    """
    path_docs = Path(__file__).parent / "docs" / "genera_Datos_Saldos_Bucket_Dag.md"
    try:
        return path_docs.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "# Documentación no encontrada"


DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))

# Variables de entorno/Airflow
PG_CERO_CONN_ID = Variable.get("PG_CERO_CONN_ID")
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_TABLE_PART1 = Variable.get("BQ_TABLE_PART1")


@dag(
    dag_id="genera_Datos_Saldos_Bucket_Dag",
    schedule="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    doc_md=get_dag_docs(),
    tags=["area:datos", "proceso:planchas", "target:bigquery", "frecuencia:diaria"],
)
def generaDatosSaldosBucketDag():
    """
    ### DAG de Generación de Datos de Saldos y Planchas

    Este proceso orquesta la ejecución de múltiples DAGs secundarios y tareas de transformación
    para consolidar la información de saldos en Google BigQuery.

    **Flujo de ejecución:**
    1. Obtención de metadatos y calendarios operativos.
    2. Trigger del proceso de generación de datos clave (`GeneracionDatosClave_Dag`).
    3. Preparación de configuración para la plancha 'Saldos Cero'.
    4. Trigger de la generación de plancha de saldos cero (`GeneracionPlanchaSaldosCero_Dag`).
    5. Paso de finalización y log de cierre.

    **Conexiones utilizadas:**
    - `PG_CERO_CONN_ID`: Conexión a base de datos PostgreSQL Cero.
    - `GCP_CONN_ID`: Credenciales para servicios de Google Cloud.
    """

    # 1. Obtener Metadatos
    metadatos = obtener_metadatos_y_calendario()

    # 2. Trigger Datos Clave
    trigger_generacionDatosClaveDag = TriggerDagRunOperator(
        task_id="trigger_GeneracionDatosClave_Dag",
        trigger_dag_id="GeneracionDatosClave_Dag",
        conf=metadatos,
        wait_for_completion=True,
        poke_interval=60,
    )

    # 3. Task de Configuración
    @task
    def preparar_datos_plancha_saldos_cero():
        """
        Genera el diccionario de configuración necesario para el trigger de la plancha Cero.
        Extrae rutas locales y variables globales.
        """
        return {
            "ruta_absoluta": DIRECTORIO_BASE,
            "archivo_sql": "consulta_cero.sql",
            "pg_conn_id": PG_CERO_CONN_ID,
            "gcp_conn_id": GCP_CONN_ID,
            "bq_project_id": GCP_PROJECT_ID,
            "bq_table_part1": BQ_TABLE_PART1,
        }

    preparar_config = preparar_datos_plancha_saldos_cero()

    # 4. Trigger Plancha Saldos Cero
    trigger_GeneracionPlanchaSaldosCeroDag = TriggerDagRunOperator(
        task_id="trigger_GeneracionPlanchaSaldosCero_Dag",
        trigger_dag_id="GeneracionPlanchaSaldosCero_Dag",
        conf=preparar_config,
        wait_for_completion=True,
        poke_interval=60,
    )

    # 5. Finalización
    @task
    def final_step():
        """Log de confirmación de fin de flujo."""
        print("Proceso de orquestación completado exitosamente.")

    # Definición de dependencias
    (
        metadatos
        >> trigger_generacionDatosClaveDag
        >> preparar_config
        >> trigger_GeneracionPlanchaSaldosCeroDag
        >> final_step()
    )


# Instanciación del DAG
dag_instancia = generaDatosSaldosBucketDag()
