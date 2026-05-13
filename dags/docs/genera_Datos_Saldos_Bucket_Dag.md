# Orquestación del proceso de generación de planchas

## Propósito
Este DAG gestiona la creación de planchas mediante la obtención y transformación de datos clave.  
El flujo contempla la generación de las siguientes tablas:

- `ifbolsa_semanal`
- `fl_reporte_saldos_desglosado`
- `fl_reporte_saldos_desglosado_mensual`

Además, realiza la construcción de:

- `Plancha **Saldos Cero**`
- `Plancha **Indicadores Cartera Mensual**`

---

## Arquitectura

El proceso opera principalmente sobre **Google BigQuery**, utilizando consultas SQL y procedimientos almacenados (*Stored Procedures*) para la transformación y actualización de información.

### Componentes principales

- Ejecución de consultas SQL en BigQuery.
- Sobrescritura y actualización de tablas en el dataset operativo.
- Uso de Stored Procedures para procesos de transformación complejos.
- Integración con bases de datos externas para cargas pesadas o información no disponible en BigQuery.

### Detalle de Tareas Críticas

#### `obtener_metadatos_y_calendario`
Esta tarea actúa como el cerebro logístico del DAG. Su función es calcular las ventanas temporales y extraer parámetros de configuración dinámicos para las tareas posteriores.


*   **Cálculo de Fechas**: Determina automáticamente el inicio/fin de semana, número de semana ISO, inicio/fin de mes y año basándose en la `logical_date` de Airflow.
*   **Gestión de Variables**: Recupera el ID del proyecto de BigQuery (`bq_project_id`) y el mapa de tablas operativas desde Airflow Variables.
*   **Consulta de Regiones**: Utiliza `BigQueryHook` para obtener la lista actualizada de regiones desde la tabla de configuración.
*   **Salida (Output)**: Retorna un diccionario serializable que se pasa como configuración (`conf`) a los DAGs hijos a través de los `TriggerDagRunOperator`.

### Fuentes externas utilizadas

El DAG establece conexiones hacia las siguientes bases de datos productivas:

- Cero
- Procrea
- ReportesFL

Estas conexiones son utilizadas principalmente en procesos de alta carga o para consultar tablas que no se encuentran replicadas en BigQuery.

---

## Destino

**Google BigQuery** Dataset: `ds_operacion_comercial`

---

## SLA

La ejecución del DAG debe completarse diariamente antes de las **07:00 AM**.

---

## Contacto

**Área de Datos**