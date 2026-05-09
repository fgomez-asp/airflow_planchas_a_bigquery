from datetime import datetime
import time
from airflow.sdk import dag, task, task_group
from include.common.bigQuery import (
    generar_query_extraccion,
    inferir_y_crear_esquema_bq,
    ejecutar_merge_upsert_efimero,
)
from datetime import datetime, timedelta
import time
from airflow.decorators import dag, task, task_group
from airflow.sdk import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.empty import EmptyOperator

CONFIGURACION_DWH = Variable.get("CONFIGURACION_DWH", deserialize_json=True)


@dag(
    dag_id="IngestaMasterUpsertBQ_Dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingesta", "upsert", "bigquery", "estricto"],
    default_args={"retries": 1},
    max_active_runs=1,
)
def orquestador_ingesta_upsert():

    @task
    def preparar_infraestructura(
        conn_name: str, conf: dict, esquema: str, tabla: str, cfg: dict
    ):
        """Genera la query base y asegura que la tabla exista en BQ con el esquema correcto."""

        query_base = cfg.get("query", f"SELECT * FROM {esquema}.{tabla}")

        inferir_y_crear_esquema_bq(
            pg_conn_id=conf["pg_conn_id"],
            gcp_conn_id=conf["gcp_conn_id"],
            query_personalizada=query_base,
            bq_project=conf["bq_project_id"],
            bq_dataset=conf["bq_dataset_id"],
            bq_table_name=tabla,
        )
        return "Esquema validado"

    @task(
        retries=3, retry_delay=timedelta(minutes=2)
    )  # Añadimos resiliencia a la carga
    def ejecutar_carga_estricta(
        conn_name: str,
        conf: dict,
        esquema: str,
        tabla: str,
        cfg: dict,
        ruta_gcs_base: str,
    ):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery

        hook = BigQueryHook(gcp_conn_id=conf["gcp_conn_id"], use_legacy_sql=False)
        client = hook.get_client()

        target_str = f"{conf['bq_project_id']}.{conf['bq_dataset_id']}.{tabla}"
        target_obj = client.get_table(target_str)

        modo = cfg.get("modo", "truncar")

        gcs_hook = GCSHook(gcp_conn_id=conf["gcp_conn_id"])

        files = gcs_hook.list(bucket_name=conf["gcs_bucket"], prefix=ruta_gcs_base)

        json_files = [f for f in files if f.endswith(".json")]
        if not files:
            raise ValueError(f"No se encontraron archivos para: {ruta_gcs_base}")

        elif len(files) == 1:
            uri = f"gs://{conf['gcs_bucket']}/{files[0]}"
            print(f"Cargando archivo único: {uri}")

        else:
            uri = f"gs://{conf['gcs_bucket']}/{ruta_gcs_base}_*.json"
            print(f"Cargando múltiples archivos con patrón: {uri}")

        job_config = bigquery.LoadJobConfig(
            schema=target_obj.schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_TRUNCATE",
            # Opcional pero recomendado para ignorar pequeños errores de formato en JSONs masivos
            ignore_unknown_values=True,
        )

        if modo in ["actualizar", "actualizar_query"]:
            staging_str = f"{conf['bq_project_id']}.{conf['bq_dataset_id']}.stg_temp_{tabla}_{int(time.time())}"
            print(f"Cargando JSONs a staging efímera: {staging_str}")
            client.load_table_from_uri(uri, staging_str, job_config=job_config).result(
                timeout=1800
            )

            columnas = [f.name for f in target_obj.schema]
            print(f"Ejecutando MERGE para {tabla}...")
            ejecutar_merge_upsert_efimero(
                client, target_str, staging_str, cfg["pk"], columnas
            )

            print("Limpiando tabla temporal...")
            client.delete_table(staging_str, not_found_ok=True)
        else:
            print(f"Sobreescribiendo tabla final: {target_str}")
            client.load_table_from_uri(uri, target_str, job_config=job_config).result(
                timeout=1800
            )

    @task_group
    def tg_tabla(conn_name, conf, esquema, tabla, cfg):

        from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
            PostgresToGCSOperator,
        )

        preparacion = preparar_infraestructura(conn_name, conf, esquema, tabla, cfg)
        query_sql = generar_query_extraccion(esquema, tabla, cfg)

        # MODIFICACIÓN CRÍTICA: La ruta base sin la extensión, para que el operador ponga los índices
        ruta_gcs_base = (
            f"cargar_tablas_Planchas/{conn_name}/{esquema}/{tabla}/{{{{ ds }}}}"
        )

        ruta_gcs_operador = ruta_gcs_base + "_{}.json"

        extraer = PostgresToGCSOperator(
            task_id=f"pg_a_gcs_{tabla}",
            postgres_conn_id=conf["pg_conn_id"],
            sql=query_sql,
            bucket=conf["gcs_bucket"],
            filename=ruta_gcs_operador,
            export_format="json",
            use_server_side_cursor=True,
            cursor_itersize=10000,  # Aumentado para mayor eficiencia en memoria RAM de la BD
            approx_max_file_size_bytes=5_000_000,  # ~5MB por archivo. GCS maneja esto sin inmutarse.
            retries=3,
            retry_delay=timedelta(minutes=2),
        )

        carga = ejecutar_carga_estricta(
            conn_name, conf, esquema, tabla, cfg, ruta_gcs_base
        )
        final = EmptyOperator(task_id="final", trigger_rule="all_done")

        preparacion >> extraer >> carga >> final

    @task_group
    def tg_esquema(conn_name, conf, esquema, tablas):
        for tabla, cfg in tablas.items():

            if not cfg.get("subir", True):
                continue

            tg_tabla.override(group_id=f"tabla_{conn_name}_{esquema}_{tabla}")(
                conn_name=conn_name, conf=conf, esquema=esquema, tabla=tabla, cfg=cfg
            )

    @task_group
    def tg_conexion(conn_name, conf):
        for esquema, tablas in conf["esquemas"].items():
            tg_esquema.override(group_id=f"esquema_{conn_name}_{esquema}")(
                conn_name=conn_name, conf=conf, esquema=esquema, tablas=tablas
            )

    # --- GENERACIÓN DE FLUJOS ---
    for conn_name, conf in CONFIGURACION_DWH.items():
        tg_conexion.override(group_id=f"conexion_{conn_name}")(
            conn_name=conn_name, conf=conf
        )


dag_instancia = orquestador_ingesta_upsert()
