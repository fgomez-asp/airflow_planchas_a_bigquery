# Pipeline: Generación Plancha Saldos Cero

## Propósito
Este DAG realiza la ingesta de datos desde la base de datos productiva **Cero (PostgreSQL)** hacia **Google BigQuery**. Es un proceso crítico que consolida la información base para la "Plancha de Saldos Cero" mediante una técnica de extracción por DataFrame y carga vía Parquet.

---

## Flujo de Trabajo

### 1. Validación de Infraestructura
La tarea `validar_y_crear_esquema` asegura que el destino en BigQuery exista y sea compatible con la consulta SQL. Utiliza una utilidad de inferencia para crear la tabla `dwh_cero.planchaSaldosCero_P1` de forma dinámica si es necesario.

### 2. Extracción y Carga (Pandas / Parquet)
Es el núcleo técnico del DAG:
- **Origen**: Ejecuta el archivo `consulta_cero.sql` en PostgreSQL.
- **Sanitización**: Aplica reglas de limpieza (limites de strings, nulos de Pandas a Python `None`) para evitar fallos de serialización en BigQuery.
- **Carga**: Utiliza el método `load_table_from_dataframe` con disposición `WRITE_TRUNCATE`, asegurando que la tabla operativa siempre tenga la versión más fresca de los datos.

### 3. Generación de Plancha Final
Una vez cargados los datos crudos, se invoca el procedimiento almacenado `generar_planchassaldostmp()` en BigQuery. Este SP realiza las transformaciones finales y cálculos de negocio para materializar la plancha definitiva.

---

## Especificaciones Técnicas
- **Timeout de Extracción**: 45 minutos (diseñado para cargas pesadas).
- **Tags**: `ingesta`, `custom`, `pandas`, `bigquery`.
- **Dispositivo de Escritura**: Overwrite (Truncar y Cargar).

---

## Contacto
**Área de Datos**