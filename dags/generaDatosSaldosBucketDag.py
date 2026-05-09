from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from include.dominio_generaDatosSaldosBucket.task.obtener_metadatos_y_calendario import (
    obtener_metadatos_y_calendario,
)
import os
from airflow.sdk import Variable

DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))

PG_CERO_CONN_ID = Variable.get("PG_CERO_CONN_ID")
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_TABLE_PART1 = Variable.get("BQ_TABLE_PART1")


@dag(
    dag_id="genera_Datos_Saldos_Bucket_Dag",
    schedule="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def generaDatosSaldosBucketDag():
    """
    trigger_IngestaMasterUpsertBQDag = TriggerDagRunOperator(
        task_id="trigger_IngestaMasterUpsertBQ_Dag",
        trigger_dag_id="IngestaMasterUpsertBQ_Dag",
        wait_for_completion=True,
        poke_interval=60,
    )

    """
    metadatos = obtener_metadatos_y_calendario()

    trigger_generacionDatosClaveDag = TriggerDagRunOperator(
        task_id="trigger_GeneracionDatosClave_Dag",
        trigger_dag_id="GeneracionDatosClave_Dag",
        conf=metadatos,
        wait_for_completion=True,
        poke_interval=60,
    )

    @task
    def preparar_datos_plancha_saldos_cero():

        config = {
            "ruta_absoluta": DIRECTORIO_BASE,
            "archivo_sql": "consulta_cero.sql",
            "pg_conn_id": PG_CERO_CONN_ID,
            "gcp_conn_id": GCP_CONN_ID,
            "bq_project_id": GCP_PROJECT_ID,
            "bq_table_part1": BQ_TABLE_PART1,  # "dwh_cero.planchaSaldosCero_P1",
        }

        return config

    preparar_config = preparar_datos_plancha_saldos_cero()

    trigger_GeneracionPlanchaSaldosCeroDag = TriggerDagRunOperator(
        task_id="trigger_GeneracionPlanchaSaldosCero_Dag",
        trigger_dag_id="GeneracionPlanchaSaldosCero_Dag",
        conf=preparar_config,
        wait_for_completion=True,
        poke_interval=60,
    )

    @task
    def final_step():
        print("Todos los dags terminaron")

    (
        # trigger_IngestaMasterUpsertBQDag
        # >>
        metadatos
        >> trigger_generacionDatosClaveDag
        >> preparar_config
        >> trigger_GeneracionPlanchaSaldosCeroDag
        >> final_step()
    )


# Instanciación
dag_instancia = generaDatosSaldosBucketDag()
