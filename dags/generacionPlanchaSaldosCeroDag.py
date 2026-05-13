import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from include.common.bigQuery import inferir_y_crear_esquema_bq, sanitize_df
from pathlib import Path

# Configuración de rutas para SQL y Documentación
BASE_PATH = Path(__file__).parent
SQL_PATH = BASE_PATH / "include" / "dominio_generacionDatosClave" / "sql"


def get_dag_docs():
    """Lee la documentación del DAG desde un archivo externo .md."""
    path_docs = BASE_PATH / "docs" / "GeneracionPlanchaSaldosCero_Dag.md"
    try:
        return path_docs.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "# Documentación .md no encontrada"


@dag(
    dag_id="GeneracionPlanchaSaldosCero_Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    doc_md=get_dag_docs(),  # Función asumiendo estructura anterior
    tags=["ingesta", "custom", "pandas", "bigquery"],
    default_args={"retries": 1},
)
def ingesta_custom_pandas_dag():
    """
    ### DAG de Ingesta Customizada (Postgres -> BigQuery)
    Este proceso gestiona la extracción de datos desde el sistema Cero hacia BigQuery.
    Utiliza Pandas para la manipulación intermedia y sanitización de tipos de datos,
    garantizando la integridad del esquema en el Data Warehouse.
    """

    @task
    def validar_y_crear_esquema(**context):
        """
        ### Task: Validar y Crear Esquema
        1. Localiza el archivo SQL de consulta en el sistema de archivos.
        2. Analiza el nombre de la tabla destino (Dataset + Tabla).
        3. Invoca la utilidad `inferir_y_crear_esquema_bq` para sincronizar
           el esquema de Postgres con BigQuery antes de la carga.
        """

        config = context["dag_run"].conf

        RUTA_SQL_DIR = os.path.join(
            config["ruta_absoluta"], "include", "dominio_generacionPlanchaSaldosCero"
        )

        config["ruta_archivo"] = os.path.join(RUTA_SQL_DIR, config["archivo_sql"])

        with open(config["ruta_archivo"], "r", encoding="utf-8") as f:
            query_cruda = f.read()

        bq_dataset, bq_table_name = config["bq_table_part1"].split(".")

        inferir_y_crear_esquema_bq(
            pg_conn_id=config["pg_conn_id"],
            gcp_conn_id=config["gcp_conn_id"],
            query_personalizada=query_cruda,
            bq_project=config["bq_project_id"],
            bq_dataset=bq_dataset,
            bq_table_name=bq_table_name,
            cero=True,
        )

        return config

    @task(
        execution_timeout=timedelta(minutes=45),
        retries=0,
        retry_delay=timedelta(minutes=1),
    )
    def extraccion_y_carga_manual(config: dict, ds: str):
        """
        ### Task: Extracción y Carga Manual
        Realiza el movimiento de datos entre nubes:
        - **Lectura**: Ejecuta SQL en Postgres y carga el resultado en un DataFrame.
        - **Sanitización**: Limpia el DataFrame contra el esquema oficial de BigQuery.
        - **Carga**: Sube los datos optimizados mediante el formato Parquet en memoria.
        """

        print("Iniciando proceso de extracción manual...")

        # 1. Leer y formatear el archivo SQL
        with open(config["ruta_archivo"], "r", encoding="utf-8") as f:
            query_base = f.read()

        try:
            query_final = query_base.format(fecha_filtro=ds)
        except KeyError:
            query_final = query_base

        # 2. Extracción desde Postgres
        pg_hook = PostgresHook(postgres_conn_id=config["pg_conn_id"])
        print("Ejecutando query en Postgres")
        df = pg_hook.get_pandas_df(sql=query_final)

        cantidad_registros = len(df)
        print(
            f"Query finalizada exitosamente. Registros extraídos: {cantidad_registros}"
        )

        if cantidad_registros == 0:
            print("No hay datos para cargar. Finalizando tarea.")
            return "Sin datos"

        # 3. Preparar cliente BQ y obtener esquema estricto
        print("Obteniendo esquema de BigQuery y sanitizando datos")
        bq_hook = BigQueryHook(gcp_conn_id=config["gcp_conn_id"], use_legacy_sql=False)
        client = bq_hook.get_client()

        target_str = f"{config['bq_project_id']}.{config['bq_table_part1']}"
        target_obj = client.get_table(target_str)

        # 4. Sanitizar y limpiar el DataFrame
        df = sanitize_df(df, target_obj.schema)
        # Convertir Pandas NaT/NaN a None de Python para evitar fallos de serialización
        df = df.replace({pd.NA: None})

        # 5. Carga a BigQuery (Vía Parquet en memoria)
        print("Iniciando carga a BigQuery vía Parquet")
        job_config = bigquery.LoadJobConfig(
            schema=target_obj.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(df, target_str, job_config=job_config)
        job.result()  # Esperamos confirmación de BQ

        print(
            f"Carga finalizada. {job.output_rows} registros insertados en {target_str}."
        )
        return config

    @task
    def ejecutar_procedure(config: dict):
        """
        ### Task: Ejecutar Procedimiento Almacenado
        Invoca el Stored Procedure `generar_planchassaldostmp` en BigQuery.
        Este SP es el encargado de procesar la tabla cruda P1 y convertirla
        en la plancha final de Saldos Cero.
        """
        bq_hook = BigQueryHook(gcp_conn_id=config["gcp_conn_id"], use_legacy_sql=False)
        client = bq_hook.get_client()

        query = "CALL `data-warehouse-412715.dwh_cero.generar_planchassaldostmp`();"

        job = client.query(query)
        job.result()

        return

    # Crear esquema para subir la query en dags\include\dominio_generacionPlanchaSaldosCero\consulta_cero.sql en la tabla llamada dwh_cero.planchaSaldosCero_P1
    tarea_esquema = validar_y_crear_esquema()

    # Subir el resultado de la query en dags\include\dominio_generacionPlanchaSaldosCero\consulta_cero.sql a la tabla llamada dwh_cero.planchaSaldosCero_P1
    tarea_ingesta = extraccion_y_carga_manual(tarea_esquema)

    # Ejecutar procedimiento almacenado dwh_cero.generar_planchassaldostmp para generar la plancha tabla planchasaldostmp que es la planchaSaldosCero
    tarea_procedure = ejecutar_procedure(tarea_ingesta)

    tarea_esquema >> tarea_ingesta >> tarea_procedure


dag_instancia = ingesta_custom_pandas_dag()
