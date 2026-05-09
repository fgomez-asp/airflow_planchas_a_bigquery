from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import calendar
from airflow.sdk import task
from airflow.sdk import Variable
from types import SimpleNamespace

@task
def obtener_metadatos_y_calendario(logical_date):
    """
    Obtiene fechas, regiones y buckets.
    """
    fecha_ejecucion = logical_date.date()

    p_id=Variable.get('bq_project_id')        
    tablas=Variable.get("tablas_dag_generaDatosSaldosBucket", deserialize_json=True)

    print(f"id {p_id}")

    tablas_completas={clave: f"{p_id}.{valor}" for clave, valor in tablas.items()}

    tablas=SimpleNamespace(**tablas_completas)
    
    inicio_semana = fecha_ejecucion - timedelta(days=fecha_ejecucion.weekday())
    fin_semana = inicio_semana + timedelta(days=6)
    num_semana = fecha_ejecucion.isocalendar()[1]
    
    inicio_mes = fecha_ejecucion.replace(day=1)
    ultimo_dia_mes = calendar.monthrange(fecha_ejecucion.year, fecha_ejecucion.month)[1]
    fin_mes = fecha_ejecucion.replace(day=ultimo_dia_mes)
    
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    
    sql_regiones = f"""
        SELECT r.clave as region_id 
        FROM `{tablas.regiones}` r 
        ORDER BY r.clave
    """

    df_regiones = bq_hook.get_pandas_df(sql=sql_regiones)
    lista_regiones = df_regiones['region_id'].astype(str).tolist()

    """
    # 3. Obtención de Buckets (Asesor y Gestor)
    def get_buckets(responsable):
        sql = f"SELECT escala FROM `{tablas.cat_buckets}` WHERE responsable = '{responsable}'"
        df = bq_hook.get_pandas_df(sql=sql)
        return df['escala'].astype(str).tolist()

    buckets_asesor = get_buckets('ASESOR')
    buckets_gestor = get_buckets('GESTOR')
    """
    
    return {
        'fecha_ejecucion': fecha_ejecucion.strftime('%Y-%m-%d'),
        'inicio_semana': inicio_semana.strftime('%Y-%m-%d'),
        'fin_semana': fin_semana.strftime('%Y-%m-%d'),
        'semana': num_semana,
        'inicio_mes': inicio_mes.strftime('%Y-%m-%d'),
        'fin_mes': fin_mes.strftime('%Y-%m-%d'),
        'mes': fecha_ejecucion.month,
        'anio': fecha_ejecucion.year,
        'sucursales': lista_regiones,
        #'bucket_asesor': buckets_asesor,
        #'bucket_gestor': buckets_gestor,
        "id_proyecto_bigquery": p_id,
        "tablas_BQ": tablas_completas
    }