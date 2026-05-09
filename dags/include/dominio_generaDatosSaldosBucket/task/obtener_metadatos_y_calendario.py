from datetime import timedelta
import calendar
from airflow.sdk import task
from airflow.sdk import Variable
from include.common.bigQuery import ejecutar_query_bq
from include.common.parameters_converter import build_dag_parameters
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


@task
def obtener_metadatos_y_calendario(logical_date):
    """
    Obtiene fechas, regiones y buckets.
    """
    fecha_ejecucion = logical_date.date()

    tablas, params, flags = build_dag_parameters(
        Variable.get("generaDatosSaldosBucket_Variables", deserialize_json=True)
    )
    print(params)
    print(flags)

    inicio_semana = fecha_ejecucion - timedelta(days=fecha_ejecucion.weekday())
    fin_semana = inicio_semana + timedelta(days=6)
    num_semana = fecha_ejecucion.isocalendar()[1]

    inicio_mes = fecha_ejecucion.replace(day=1)
    ultimo_dia_mes = calendar.monthrange(fecha_ejecucion.year, fecha_ejecucion.month)[1]
    fin_mes = fecha_ejecucion.replace(day=ultimo_dia_mes)

    print(tablas)
    sql_regiones = f"""
        SELECT r.clave as region_id 
        FROM {tablas['regiones']} r 
        ORDER BY r.clave
    """

    lista_regiones = ejecutar_query_bq(query=sql_regiones)

    def get_buckets(responsable):
        sql = f"""
        SELECT escala 
        FROM {tablas['cat_buckets']} 
        WHERE responsable = '{responsable}'
        """

        result = ejecutar_query_bq(query=sql)

        if not result:
            return []

        return [str(row["escala"]) for row in result if "escala" in row]

    buckets_asesor = get_buckets("ASESOR")
    buckets_gestor = get_buckets("GESTOR")

    return {
        "fecha_ejecucion": fecha_ejecucion.strftime("%Y-%m-%d"),
        "inicio_semana": inicio_semana.strftime("%Y-%m-%d"),
        "fin_semana": fin_semana.strftime("%Y-%m-%d"),
        "semana": num_semana,
        "inicio_mes": inicio_mes.strftime("%Y-%m-%d"),
        "fin_mes": fin_mes.strftime("%Y-%m-%d"),
        "mes": fecha_ejecucion.month,
        "anio": fecha_ejecucion.year,
        "sucursales": lista_regiones,
        "bucket_asesor": buckets_asesor,
        "bucket_gestor": buckets_gestor,
    }
