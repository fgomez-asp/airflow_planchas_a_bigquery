import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from include.common.bigQuery import inferir_y_crear_esquema_bq, sanitize_df


@dag(
    dag_id="GeneracionPlanchaSaldosCero_Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingesta", "custom", "pandas", "bigquery"],
    default_args={"retries": 1},
)
def ingesta_custom_pandas_dag():

    @task
    def validar_y_crear_esquema(**context):

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

        bq_hook = BigQueryHook(gcp_conn_id=config["gcp_conn_id"], use_legacy_sql=False)
        client = bq_hook.get_client()

        query = "CALL `data-warehouse-412715.dwh_cero.generar_planchassaldostmp`();"

        job = client.query(query)
        job.result()

        return

    @task
    def ejecutar_procedure_fl(config: dict):

        bq_hook = BigQueryHook(gcp_conn_id=config["gcp_conn_id"], use_legacy_sql=False)
        client = bq_hook.get_client()

        query = "CALL `data-warehouse-412715.ds_procrea.congela_datos_saldos_bucket_p1_v1`( ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51'], '2026-04-01',  '2026-04-30', '2026-04-29', '2026-04-27', '2026-05-03', 501, true);"

        job = client.query(query)
        job.result()

        return

    # Crear esquema para subir la query en dags\include\dominio_generacionPlanchaSaldosCero\consulta_cero.sql en la tabla llamada dwh_cero.planchaSaldosCero_P1
    tarea_esquema = validar_y_crear_esquema()

    # Subir el resultado de la query en dags\include\dominio_generacionPlanchaSaldosCero\consulta_cero.sql a la tabla llamada dwh_cero.planchaSaldosCero_P1
    tarea_ingesta = extraccion_y_carga_manual(tarea_esquema)

    # Ejecutar procedimiento almacenado dwh_cero.generar_planchassaldostmp para generar la plancha tabla planchasaldostmp que es la planchaSaldosCero
    tarea_procedure = ejecutar_procedure(tarea_ingesta)

    tarea_procedure_fl = ejecutar_procedure_fl(tarea_ingesta)

    tarea_esquema >> tarea_ingesta >> tarea_procedure >> tarea_procedure_fl


dag_instancia = ingesta_custom_pandas_dag()
