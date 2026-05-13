from datetime import datetime
from airflow.sdk import dag, task, Variable
from include.common.bigQuery import inferir_y_crear_esquema_bq, sanitize_df
from include.common.bigQuery import ejecutar_query_bq
from airflow.operators import EmptyOperator
from include.common.parameters_converter import build_dag_parameters

dag_docs = """
"""


@dag(
    dag_id="CongelaDatosSaldos_Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # tags=[],
    # default_args={"retries": 1},
)
def congela_datos_saldos_dag():

    # 1. Tarea de inicio y validación
    @task()
    def recibir_y_preparar_config(**context) -> dict:
        """
        Recibe el diccionario exacto que el Orquestador envió vía TriggerDagRunOperator
        y prepara la configuración base para el resto de tareas.
        """
        conf_maestra = context["dag_run"].conf

        print(f"Variables de la conf_maestra: {conf_maestra}")

        tablas, params, flags = build_dag_parameters(
            Variable.get("generaDatosClave_Variables", deserialize_json=True)
        )

        if not conf_maestra:
            raise ValueError("No se recibió configuración del Orquestador")

        return {
            "tablas": tablas,
            "flags": flags,
            "params": params | conf_maestra,
        }

    # 2. Branch principal: Evalúa modoContingencia
    @task.branch
    def evaluar_modo_contingencia(config: dict) -> str:
        if config.get("flags", {}).get("modoContingencia", False):
            return "contar_fl_saldos_pago"
        return "recrear_tabla_temp_desglosado"

    # ==========================================
    # RAMA TRUE (modoContingencia == True)
    # ==========================================
    @task
    def contar_fl_saldos_pago(config: dict) -> int:
        tabla = config["tablas"]["fl_saldos_pago"]
        fecha = config["params"]["fecha_ejecucion"]

        query = f"""
            SELECT COUNT(1) AS total_registos 
            FROM `{tabla}` 
            WHERE fecha_ejecucion = '{fecha}'
        """
        resultado = ejecutar_query_bq(query, fetch=True)
        # Asumiendo que la función retorna [{"total_registos": X}]
        return resultado[0]["total_registos"] if resultado else 0

    @task.branch
    def evaluar_conteo_pago(conteo: int) -> str:
        if conteo > 0:
            return "truncar_e_insertar_saldos_pago"
        return "fin_proceso"

    @task
    def truncar_e_insertar_saldos_pago(config: dict) -> dict:
        tabla_origen = config["tablas"]["fl_saldos_pago"]
        tabla_destino = config["tablas"]["fl_saldos_pago_actual"]
        fecha = config["params"]["fecha_ejecucion"]

        query = f"""
            TRUNCATE TABLE `{tabla_destino}`;
            INSERT INTO `{tabla_destino}`
            SELECT * FROM `{tabla_origen}` 
            WHERE fecha_ejecucion = '{fecha}';
        """
        ejecutar_query_bq(query, fetch=False)
        return config  # Retornamos config para pasarlo a la siguiente tarea secuencial

    @task
    def contar_fl_saldos_pagare(config: dict) -> int:
        tabla = config["tablas"]["fl_saldos_pagare"]
        fecha = config["params"]["fecha_ejecucion"]

        query = f"""
            SELECT COUNT(1) AS totalPagares 
            FROM `{tabla}` 
            WHERE fecha_ejecucion = '{fecha}'
        """
        resultado = ejecutar_query_bq(query, fetch=True)
        return resultado[0]["totalPagares"] if resultado else 0

    @task.branch
    def evaluar_conteo_pagare(totalPagares: int) -> str:
        if totalPagares > 0:
            return "truncar_e_insertar_saldos_pagares"
        return "fin_proceso"

    @task
    def truncar_e_insertar_saldos_pagares(config: dict):
        tabla_origen = config["tablas"]["fl_saldos_pagare"]
        tabla_destino = config["tablas"]["fl_saldos_pagares_actual"]
        fecha = config["params"]["fecha_ejecucion"]

        query = f"""
            TRUNCATE TABLE `{tabla_destino}`;
            INSERT INTO `{tabla_destino}`
            SELECT * FROM `{tabla_origen}` 
            WHERE fecha_creacion_proceso = '{fecha}';
        """
        ejecutar_query_bq(query, fetch=False)

    # ==========================================
    # RAMA FALSE (modoContingencia == False)
    # ==========================================
    @task
    def recrear_tabla_temp_desglosado(config: dict) -> dict:
        tabla = config["tablas"]["fl_reporte_saldos_desglosado_temp"]
        # NOTA: Debes agregar la definición de columnas real en el CREATE TABLE
        query = f"""
            DROP TABLE IF EXISTS `{tabla}`;
            CREATE TABLE `{tabla}` (
                dummy_col STRING
            );
        """
        ejecutar_query_bq(query, fetch=False)
        return config

    @task
    def recrear_tabla_temp_mensual(config: dict) -> dict:
        tabla = config["tablas"]["fl_reporte_saldos_desglosado_mensual_temp"]
        # NOTA: Debes agregar la definición de columnas real
        query = f"""
            DROP TABLE IF EXISTS `{tabla}`;
            CREATE TABLE `{tabla}` (
                dummy_col STRING
            );
        """
        ejecutar_query_bq(query, fetch=False)
        return config

    @task
    def ejecutar_procedure(config: dict):
        sp = config["tablas"]["sp_procedure"]
        query = f"CALL `{sp}`();"
        ejecutar_query_bq(query, fetch=False)

    # ==========================================
    # NODO FINAL
    # ==========================================
    # trigger_rule="none_failed_min_one_success" asegura que el proceso termine
    # correctamente sin importar de qué rama venga.
    fin_proceso = EmptyOperator(
        task_id="fin_proceso", trigger_rule="none_failed_min_one_success"
    )

    # -------------------------------------------------------------------
    # Orquestación y dependencias
    # -------------------------------------------------------------------
    config_inicial = recibir_y_preparar_config()
    branch_principal = evaluar_modo_contingencia(config_inicial)

    # Nodos Rama True
    conteo_pago = contar_fl_saldos_pago(config_inicial)
    branch_pago = evaluar_conteo_pago(conteo_pago)
    insert_pago = truncar_e_insertar_saldos_pago(config_inicial)

    conteo_pagare = contar_fl_saldos_pagare(insert_pago)
    branch_pagare = evaluar_conteo_pagare(conteo_pagare)
    insert_pagare = truncar_e_insertar_saldos_pagares(config_inicial)

    # Flujo de dependencias de la Rama True
    branch_principal >> conteo_pago >> branch_pago
    branch_pago >> insert_pago >> conteo_pagare >> branch_pagare
    branch_pagare >> insert_pagare >> fin_proceso

    # Rutas de salida temprana (Si conteo == 0)
    branch_pago >> fin_proceso
    branch_pagare >> fin_proceso

    # Nodos Rama False
    temp_desglosado = recrear_tabla_temp_desglosado(config_inicial)
    temp_mensual = recrear_tabla_temp_mensual(temp_desglosado)
    call_sp = ejecutar_procedure(temp_mensual)

    # Flujo de dependencias de la Rama False
    branch_principal >> temp_desglosado >> temp_mensual >> call_sp >> fin_proceso


dag_instancia = congela_datos_saldos_dag()
