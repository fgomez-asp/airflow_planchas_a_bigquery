import re
from typing import Optional, List, Dict, Any
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import pandas as pd
import decimal

PG_TO_BQ_SIMPLE_ESQUEMA = {
    "smallint": "INT64",
    "integer": "INT64",
    "int": "INT64",
    "bigint": "INT64",
    "serial": "INT64",
    "bigserial": "INT64",
    "numeric": "NUMERIC",
    "decimal": "NUMERIC",
    "real": "FLOAT64",
    "double precision": "FLOAT64",
    "money": "NUMERIC",
    "text": "STRING",
    "character varying": "STRING",
    "varchar": "STRING",
    "character": "STRING",
    "char": "STRING",
    "citext": "STRING",
    "boolean": "BOOL",
    "date": "DATE",
    "timestamp with time zone": "TIMESTAMP",
    "timestamp without time zone": "TIMESTAMP",
    "time with time zone": "TIME",
    "time without time zone": "TIME",
    "bytea": "BYTES",
    "json": "JSON",
    "jsonb": "JSON",
    "uuid": "STRING",
    "inet": "STRING",
    "cidr": "STRING",
    "macaddr": "STRING",
}


def sanitize_df(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Fuerza los tipos de datos de un DataFrame de Pandas para que coincidan
    exactamente con el esquema estricto de BigQuery.
    """
    for field in schema:
        col = field.name
        bq_type = field.field_type

        # Si la columna que espera BQ no viene en el DF, la creamos vacía
        if col not in df.columns:
            df[col] = None

        if bq_type == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif bq_type == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        elif bq_type == "DATETIME":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
        elif bq_type in ["INT64", "INTEGER"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif bq_type in ["FLOAT64", "FLOAT"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif bq_type in ["NUMERIC", "BIGNUMERIC"]:

            df[col] = (
                df[col].astype(str).replace(["nan", "NaN", "<NA>", "None", ""], None)
            )
            df[col] = df[col].apply(
                lambda x: decimal.Decimal(x) if x is not None else None
            )
        elif bq_type == "BOOL":
            df[col] = df[col].astype("boolean")
        elif bq_type == "STRING":
            df[col] = df[col].astype(str).replace(["nan", "NaN", "<NA>", "None"], None)

    # Filtrar el DataFrame para que SOLO contenga las columnas del esquema BQ
    columnas_bq = [field.name for field in schema]
    df = df[[col for col in columnas_bq if col in df.columns]]

    return df


def bq_sanitize_table_id(name: str) -> str:
    """BigQuery table IDs: [a-zA-Z_][a-zA-Z0-9_]*; reemplaza inválidos y evita empezar con número."""
    if not name:
        raise ValueError("Nombre de tabla vacío.")
    name2 = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if re.match(r"^[0-9]", name2):
        name2 = "_" + name2
    return name2[:256]


def generar_query_extraccion(esquema: str, tabla: str, config: dict) -> str:
    """
    Construye la query de Postgres basada en los 4 modos:
    1. truncar: SELECT *
    2. truncar_query: Usa la query del JSON
    3. actualizar: SELECT * WHERE (creacion >= delta OR modificacion >= delta)
    4. actualizar_query: Usa la query del JSON
    """
    modo = config.get("modo", "truncar")

    if modo in ["truncar_query", "actualizar_query"]:
        if "query" not in config:
            raise ValueError(
                f"El modo {modo} requiere la clave 'query' en la configuración."
            )
        return config["query"]

    if modo == "actualizar":
        col_c = config.get("columna_creacion", "fecha_creacion")
        col_m = config.get("columna_modificacion", "fecha_modificacion")
        dias = config.get("dias_delta", 1)

        filtro_fechas = (
            f"({col_c} >= '{{{{ macros.ds_add(ds, -{dias}) }}}}' "
            f"OR {col_m} >= '{{{{ macros.ds_add(ds, -{dias}) }}}}')"
        )
        return f"SELECT * FROM {esquema}.{tabla} WHERE {filtro_fechas};"

    return f"SELECT * FROM {esquema}.{tabla};"


def inferir_y_crear_esquema_bq(
    pg_conn_id: str,
    gcp_conn_id: str,
    query_personalizada: str,
    bq_project: str,
    bq_dataset: str,
    bq_table_name: str,
    cero: bool = False,
):
    """
    Ejecuta una query en Postgres con LIMIT 0 para extraer la metadata de las columnas,
    traduce los OIDs a tipos de BigQuery y crea la tabla si no existe.
    """
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    query_limpia = query_personalizada.strip().rstrip(";")

    sql_limit_0 = f"SELECT * FROM ({query_limpia}) AS subq LIMIT 0;"

    if cero:
        sql_limit_0 = f" {query_limpia} LIMIT 0;"

    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(sql_limit_0)
        metadata_columnas = cur.description

        if not metadata_columnas:
            raise ValueError(
                f"La query para {bq_table_name} no devolvió columnas válidas."
            )

        oids = tuple(set([desc[1] for desc in metadata_columnas]))
        cur.execute("SELECT oid, typname FROM pg_type WHERE oid IN %s", (oids,))
        mapa_oids = dict(cur.fetchall())

        esquema_bq = []
        for desc in metadata_columnas:
            col_name = desc[0]
            type_oid = desc[1]
            pg_type_name = str(mapa_oids.get(type_oid, "text")).strip().lower()

            bq_type = "STRING"
            for key, val in PG_TO_BQ_SIMPLE_ESQUEMA.items():
                if pg_type_name.startswith(key) or (
                    "int" in pg_type_name and key == "int"
                ):
                    bq_type = val
                    break

            esquema_bq.append(
                bigquery.SchemaField(name=col_name, field_type=bq_type, mode="NULLABLE")
            )

    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = bq_hook.get_client()
    bq_table_id = bq_sanitize_table_id(bq_table_name)
    tabla_ref = bigquery.Table(
        f"{bq_project}.{bq_dataset}.{bq_table_id}", schema=esquema_bq
    )

    try:
        client.create_table(tabla_ref)
        print(f"Éxito: Tabla '{bq_table_id}' creada con esquema inferido.")
    except Conflict:
        print(f"Aviso: La tabla '{bq_table_id}' ya existe.")


def ejecutar_merge_upsert_efimero(
    client, target_table: str, staging_table: str, pk: str, columnas: List[str]
):
    """Ejecuta el MERGE asumiendo que los esquemas son idénticos."""
    update_set = ", ".join([f"T.{col} = S.{col}" for col in columnas if col != pk])
    insert_cols = ", ".join(columnas)
    insert_values = ", ".join([f"S.{col}" for col in columnas])

    query = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.{pk} = S.{pk}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values});
    """
    client.query(query).result()


def ejecutar_query_bq(
    query: str, params: Optional[Dict[str, Any]] = None, fetch: bool = False
) -> Optional[List[Dict[str, Any]]]:

    print(f"Ejecutando en BigQuery:\n{query}")

    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    client = hook.get_client()

    job_config = None

    if params:
        query_parameters = []

        for key, value in params.items():
            if isinstance(value, list):
                # Detectar tipo del array (básico)
                if all(isinstance(v, int) for v in value):
                    param_type = "INT64"
                elif all(isinstance(v, float) for v in value):
                    param_type = "FLOAT64"
                else:
                    param_type = "STRING"

                query_parameters.append(
                    bigquery.ArrayQueryParameter(key, param_type, value)
                )
            else:
                # Parámetros escalares
                if isinstance(value, int):
                    param_type = "INT64"
                elif isinstance(value, float):
                    param_type = "FLOAT64"
                else:
                    param_type = "STRING"

                query_parameters.append(
                    bigquery.ScalarQueryParameter(key, param_type, value)
                )

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    if fetch:
        rows = [dict(row) for row in results]
        print(f"Filas obtenidas: {len(rows)}")
        return rows

    return None
