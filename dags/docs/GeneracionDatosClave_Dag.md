# Pipeline: Generación de Datos Clave

## Propósito
Este DAG actúa como el motor de procesamiento intermedio para la generación de planchas. Su objetivo es preparar las tablas base de empleados, bolsas semanales e históricos antes de la construcción de las planchas finales.

---

## Flujos de Trabajo

### 1. Gestión de Históricos (Solo Día 1)
Si la fecha de ejecución es el primer día del mes, el DAG realiza un respaldo de la bolsa semanal actual hacia la tabla histórica antes de truncar la tabla operativa.

### 2. Actualización de Empleados y Buckets
Controlado por el flag `executeActualizaEmpleadosBucket`
- Limpia tablas auxiliares de empleados Procrea.
- Ejecuta el Stored Procedure `sp_upsert_buckets` para sincronizar la estructura de puestos y buckets.
- Replica la información hacia la tabla de extrajudiciales.

### 3. Cálculo de Bolsa Semanal
Controlado por el flag `executeBolsaSemanal`
- Calcula la bolsa de empleados basada en factores de contención y rangos configurados.
- **Nota técnica**: Replica la lógica de un sistema legado (tJava), incluyendo condiciones específicas de factores de traspaso.

---

## Arquitectura SQL
Para mejorar la mantenibilidad, las consultas complejas se almacenan en archivos externos:
- `insertar_historico.sql`
- `obtener_empleados_activos.sql`

---

## SLA y Tags
- **Sincronización**: Debe ser disparado por el DAG Orquestador.