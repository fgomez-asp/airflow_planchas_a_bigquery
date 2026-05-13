from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path
import os
from airflow.sdk import Variable
from include.common.bigQuery import ejecutar_query_bq
from include.common.parameters_converter import build_dag_parameters

# Configuración de rutas para SQL y Documentación
BASE_PATH = Path(__file__).parent
SQL_PATH = BASE_PATH / "include" / "dominio_generacionDatosClave" / "sql"


def get_dag_docs():
    """Lee la documentación del DAG desde un archivo externo .md."""
    path_docs = BASE_PATH / "docs" / "GeneracionDatosClave_Dag.md"
    try:
        return path_docs.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "# Documentación .md no encontrada"


@dag(
    dag_id="GeneracionDatosClave_Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    doc_md=get_dag_docs(),
    tags=["clave", "historico", "empleados"],
)
def pipeline_generacion_clave():
    """
    ### Pipeline de Generación de Datos Clave

    Este proceso realiza la preparación técnica de los datos. Se encarga de:
    1. **Versionamiento**: Mueve datos a históricos al inicio de mes.
    2. **Sincronización**: Actualiza buckets de empleados mediante procedimientos almacenados.
    3. **Cálculo de Bolsa**: Aplica lógica de negocio para determinar montos semanales.

    Este DAG es **pasivo** (schedule=None) y depende de una configuración enviada por un Orquestador.
    """

    @task()
    def recibir_y_preparar_config(**context) -> dict:
        """
        ### Task: Preparar Configuración
        Une la configuración recibida por el trigger con las variables globales del sistema.
        """
        conf_maestra = context["dag_run"].conf

        # Obtener variables específicas de este DAG
        tablas, params, flags = build_dag_parameters(
            Variable.get("generaDatosClave_Variables", deserialize_json=True)
        )

        if not conf_maestra:
            raise ValueError(
                "No se recibió configuración del Orquestador (conf es None)"
            )

        return {
            "tablas": tablas,
            "flags": flags,
            "params": params | conf_maestra,  # Prioriza parámetros del orquestador
        }

    @task.branch
    def check_dia_mes():
        """Evalúa si es el primer día del mes para ejecutar el backup histórico."""
        return "insertar_historico" if datetime.now().day == 1 else "join_path_dia"

    @task()
    def insertar_historico(config: dict):
        """Respalda la información de bolsa semanal en la tabla histórica."""
        query_file = SQL_PATH / "insertar_historico.sql"
        query = query_file.read_text(encoding="utf-8").format(**config["tablas"])
        ejecutar_query_bq(query=query)
        return config

    @task()
    def truncar_origen(config: dict):
        """Limpia la tabla operativa después del respaldo histórico."""
        query = f"TRUNCATE TABLE {config['tablas']['ifbolsa_semanal']}"
        ejecutar_query_bq(query=query)
        return config

    join_path_dia = EmptyOperator(
        task_id="join_path_dia", trigger_rule="none_failed_min_one_success"
    )

    # --- FLUJO EMPLEADOS ---

    @task.branch
    def check_flag_empleados(config: dict):
        """Verifica si el flag para actualizar empleados está activo."""
        if config["flags"].get("executeActualizaEmpleadosBucket", False):
            return "borrar_tablas_auxiliares"
        return "join_empleados"

    @task()
    def borrar_tablas_auxiliares(config: dict):
        """Limpia tablas temporales de empleados Procrea."""
        t = config["tablas"]
        query = f"TRUNCATE TABLE {t['cat_buckets_empleados_PROCREA']}; TRUNCATE TABLE {t['cat_buckets_empleados_PROCREA_EXTRA']};"
        ejecutar_query_bq(query=query)
        return config

    @task()
    def invocar_sp_actualizacion(config: dict):
        """Ejecuta Stored Procedure en BigQuery para actualizar buckets."""
        p = config["params"]
        t = config["tablas"]
        query = f"CALL {t['sp_upsert_buckets']}({p['puestoAsesor']});"
        ejecutar_query_bq(query=query)
        return config

    @task()
    def insertar_extrajudicial(config: dict):
        """Inserta datos en la tabla de empleados extrajudiciales."""
        t = config["tablas"]
        # En una versión final, este SQL también podría ser un archivo externo
        query = f"INSERT INTO {t['cat_buckets_empleados_PROCREA_EXTRA']} SELECT * FROM {t['cat_buckets_empleados_PROCREA']}"
        ejecutar_query_bq(query=query)
        return config

    join_empleados = EmptyOperator(
        task_id="join_empleados", trigger_rule="none_failed_min_one_success"
    )

    # --- FLUJO BOLSA SEMANAL ---

    @task.branch
    def check_flag_Bolsa_Semanal(config: dict):
        """Verifica si se debe calcular la bolsa semanal."""
        if config["flags"].get("executeBolsaSemanal", False):
            return "obtener_empleados_activos_por_puesto"
        return "fin_proceso"

    @task()
    def obtener_empleados_activos_por_puesto(config: dict):
        """Calcula la bolsa semanal mediante la lógica SQL externa."""
        query_file = SQL_PATH / "obtener_empleados_activos.sql"
        query = query_file.read_text(encoding="utf-8").format(**config["tablas"])

        params = {"puestos": config["params"].get("puestos", [])}
        return ejecutar_query_bq(query=query, params=params, fetch=True)

    fin_proceso = EmptyOperator(
        task_id="fin_proceso", trigger_rule="none_failed_min_one_success"
    )

    # --- ORQUESTACIÓN DE DEPENDENCIAS ---

    conf_preparada = recibir_y_preparar_config()

    # Rama Histórico
    b_dia = check_dia_mes()
    t_historico = insertar_historico(conf_preparada)
    t_truncar = truncar_origen(t_historico)

    conf_preparada >> b_dia
    b_dia >> t_historico >> t_truncar >> join_path_dia
    b_dia >> join_path_dia

    # Rama Empleados
    b_emp = check_flag_empleados(conf_preparada)
    t_borrar = borrar_tablas_auxiliares(conf_preparada)
    t_sp = invocar_sp_actualizacion(t_borrar)
    t_extra = insertar_extrajudicial(t_sp)

    join_path_dia >> b_emp
    b_emp >> t_borrar >> t_sp >> t_extra >> join_empleados
    b_emp >> join_empleados

    # Rama Bolsa Semanal
    b_bolsa = check_flag_Bolsa_Semanal(conf_preparada)
    t_calc_bolsa = obtener_empleados_activos_por_puesto(conf_preparada)

    join_empleados >> b_bolsa
    b_bolsa >> t_calc_bolsa >> fin_proceso
    b_bolsa >> fin_proceso


# Instanciación
dag_instancia = pipeline_generacion_clave()
