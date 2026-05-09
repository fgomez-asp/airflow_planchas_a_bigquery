from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from airflow.sdk import Variable
from include.common.bigQuery import ejecutar_query_bq
from include.common.parameters_converter import build_dag_parameters


@dag(
    dag_id="GeneracionDatosClave_Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def pipeline_generacion_clave():

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

    @task.branch
    def check_dia_mes():
        if datetime.now().day == 1:
            return "insertar_historico"
        return "join_path_dia"

    @task()
    def insertar_historico(config: dict):
        t = config["tablas"]
        query = f"""
            INSERT INTO {t['ifbolsa_semanal_historico']} (
                id,
                no_empleado,
                nombre_empleado,
                bolsa_semanal,
                usuario_creacion,
                fecha_creacion,
                usuario_modificacion,
                fecha_modificacion
            )
            SELECT 
                id,
                no_empleado,
                nombre_empleado,
                bolsa_semanal,
                usuario_creacion,
                fecha_creacion,
                usuario_modificacion,
                fecha_modificacion
            FROM {t['ifbolsa_semanal']}
            """
        ejecutar_query_bq(query=query)

        return config

    @task()
    def truncar_origen(config: dict):
        t = config["tablas"]
        query = f"TRUNCATE TABLE {t['ifbolsa_semanal']}"
        ejecutar_query_bq(query=query)

        return config

    join_path_dia = EmptyOperator(
        task_id="join_path_dia", trigger_rule="none_failed_min_one_success"
    )

    # ==========================================
    # FLUJO 2: ACTUALIZACIÓN DE EMPLEADOS
    # ==========================================
    @task.branch
    def check_flag_empleados(config: dict):
        if config["flags"].get("executeActualizaEmpleadosBucket", False):
            return "borrar_tablas_auxiliares"
        return "fin_proceso"

    @task()
    def borrar_tablas_auxiliares(config: dict):
        t = config["tablas"]
        query = f"TRUNCATE TABLE {t['cat_buckets_empleados_PROCREA']}; TRUNCATE TABLE {t['cat_buckets_empleados_PROCREA_EXTRA']};"
        ejecutar_query_bq(query=query)
        return config

    @task()
    def invocar_sp_actualizacion(config: dict):
        p = config["params"]
        t = config["tablas"]

        query = f"""
            CALL {t['sp_upsert_buckets']}(
            {p['puestoAsesor']}
            );
        """
        ejecutar_query_bq(query=query)
        return config

    @task()
    def insertar_extrajudicial(config: dict):
        t = config["tablas"]
        query = f"""
            INSERT INTO {t['cat_buckets_empleados_PROCREA_EXTRA']} (
                id,
                cat_bucket_id,
                num_empleado,
                sucursal_id,
                habilitado,
                usuario_creacion,
                fecha_creacion,
                usuario_modificacion,
                fecha_modificacion
            )
            SELECT 
                id,
                cat_bucket_id,
                num_empleado,
                sucursal_id,
                habilitado,
                usuario_creacion,
                fecha_creacion,
                usuario_modificacion,
                fecha_modificacion
            FROM {t['cat_buckets_empleados_PROCREA']}
            """
        ejecutar_query_bq(query=query)
        return config

    # ==========================================
    # FLUJO 3: BOLSA SEMANAL
    # ==========================================

    join_empleados = EmptyOperator(
        task_id="join_empleados", trigger_rule="none_failed_min_one_success"
    )

    @task.branch
    def check_flag_Bolsa_Semanal(config: dict):
        if config["flags"].get("executeBolsaSemanal", False):
            return "obtener_empleados_activos_por_puesto"
        return "fin_proceso"

    @task()
    def obtener_empleados_activos_por_puesto(config: dict):
        t = config["tablas"]

        query = f"""
           INSERT INTO `data-warehouse-412715.dwh_cero.ifbolsa_semanal` (
                no_empleado, 
                nombre_empleado, 
                bolsa_semanal, 
                usuario_creacion, 
                fecha_creacion
                )
                WITH EmpleadosBase AS (
                -- Consulta 1: Obtención de empleados activos por puesto
                SELECT 
                    e.num_empleado AS no_empleado,
                    ARRAY_TO_STRING(
                    ARRAY[
                        TRIM(e.nombres),
                        TRIM(e.apellido_pat),
                        TRIM(e.apellido_mat)
                    ], 
                    ' '
                    ) AS nombre_empleado,
                    CAST(p.id AS INT64) AS puesto_id
                FROM `data-warehouse-412715.ds_procrea.empleado` e
                JOIN `data-warehouse-412715.ds_procrea.puestos` p 
                    ON e.puesto_asig = p.id
                WHERE e.fecha_baja IS NULL
                    AND CAST(p.id AS INT64) IN UNNEST(@puestos)
                ),

                FactoresContencion AS (
                -- Consulta 2: Lógica del tJava (Mantenimiento del BUG original)
                -- En el Java, porc_factor2 siempre se queda en 0.0 porque el 'else' 
                -- nunca entra a evaluar "B1" de nuevo.
                SELECT 
                    no_empleado,
                    MAX(CASE WHEN bucket = 'B1' THEN con_factor_traspaso ELSE 0.0 END) AS porc_factor,
                    0.0 AS porc_factor2 -- Réplica exacta del bug: nunca recibe valor
                FROM `data-warehouse-412715.dwh_reportefl.ifhistorico_ingresos_contencion`
                WHERE CAST(fecha_creacion AS DATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
                GROUP BY no_empleado
                ),

                LogicaBolsa AS (
                -- Aplicación de la condición: (porc_factor >= 80) AND (0.0 >= 80) -> Siempre será FALSE
                SELECT 
                    eb.no_empleado,
                    eb.nombre_empleado,
                    eb.puesto_id,
                    COALESCE(fc.porc_factor, 0.0) AS porc_factor_final,
                    CASE 
                    WHEN COALESCE(fc.porc_factor, 0.0) >= 80.0 AND 0.0 >= 80.0 THEN 1500 
                    ELSE 1250 
                    END AS bolsa_calculada
                FROM EmpleadosBase eb
                LEFT JOIN FactoresContencion fc ON eb.no_empleado = fc.no_empleado
                )

                -- Inserción final con cruce a configuración de bolsa
                SELECT 
                lb.no_empleado,
                lb.nombre_empleado,
                lb.bolsa_calculada,
                9 AS usuario_creacion,
                CURRENT_TIMESTAMP() AS fecha_creacion
                FROM LogicaBolsa lb
                INNER JOIN `data-warehouse-412715.dwh_cero.ifconf_bolsa_semanal` ix 
                ON lb.puesto_id = CAST(ix.puesto_id AS INT64)
                AND lb.porc_factor_final BETWEEN ix.rango_inicio AND ix.rango_fin
                WHERE lb.no_empleado NOT IN (
                -- Réplica del filtro final del tJava
                SELECT no_empleado FROM `data-warehouse-412715.dwh_cero.ifbolsa_semanal`
                );
        """

        params = {"puestos": config.get("puestos", [])}

        return ejecutar_query_bq(query=query, params=params, fetch=True)

    # ==========================================
    # FIN PROCESO
    # ==========================================
    fin_proceso = EmptyOperator(
        task_id="fin_proceso", trigger_rule="none_failed_min_one_success"
    )

    # ==========================================
    # ORQUESTACIÓN (DEPENDENCIAS)
    # ==========================================
    # 1. Obtenemos la configuración
    conf = recibir_y_preparar_config()

    # 2. Enlazamos la rama del Día 1
    rama_dia = check_dia_mes()
    tarea_historico = insertar_historico(conf)
    tarea_truncar = truncar_origen(tarea_historico)

    conf >> rama_dia >> tarea_historico >> tarea_truncar >> join_path_dia
    rama_dia >> join_path_dia

    # 3. Enlazamos la rama de Empleados
    rama_flag = check_flag_empleados(conf)
    tarea_borrar = borrar_tablas_auxiliares(conf)
    tarea_sp = invocar_sp_actualizacion(tarea_borrar)
    tarea_extrajudicial = insertar_extrajudicial(tarea_sp)

    join_path_dia >> rama_flag

    rama_flag >> tarea_borrar >> tarea_sp >> tarea_extrajudicial >> join_empleados
    rama_flag >> join_empleados

    flag_Bolsa_Semanal = check_flag_Bolsa_Semanal(conf)
    task_obtener_empleados_activos_por_puesto = obtener_empleados_activos_por_puesto(
        conf
    )

    join_empleados >> flag_Bolsa_Semanal

    flag_Bolsa_Semanal >> task_obtener_empleados_activos_por_puesto >> fin_proceso
    flag_Bolsa_Semanal >> fin_proceso


# Instanciamos el DAG
dag_instancia = pipeline_generacion_clave()
