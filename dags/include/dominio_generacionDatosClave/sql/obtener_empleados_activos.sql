INSERT INTO `{ifbolsa_semanal}` (
    no_empleado, nombre_empleado, bolsa_semanal, 
    usuario_creacion, fecha_creacion
)
WITH EmpleadosBase AS (
    SELECT 
        e.num_empleado AS no_empleado,
        ARRAY_TO_STRING([TRIM(e.nombres), TRIM(e.apellido_pat), TRIM(e.apellido_mat)], ' ') AS nombre_empleado,
        CAST(p.id AS INT64) AS puesto_id
    FROM `data-warehouse-412715.ds_procrea.empleado` e
    JOIN `data-warehouse-412715.ds_procrea.puestos` p ON e.puesto_asig = p.id
    WHERE e.fecha_baja IS NULL
      AND CAST(p.id AS INT64) IN UNNEST(@puestos)
),
FactoresContencion AS (
    SELECT 
        no_empleado,
        MAX(CASE WHEN bucket = 'B1' THEN con_factor_traspaso ELSE 0.0 END) AS porc_factor,
        0.0 AS porc_factor2 
    FROM `data-warehouse-412715.dwh_reportefl.ifhistorico_ingresos_contencion`
    WHERE CAST(fecha_creacion AS DATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
    GROUP BY no_empleado
),
LogicaBolsa AS (
    SELECT 
        eb.no_empleado, eb.nombre_empleado, eb.puesto_id,
        COALESCE(fc.porc_factor, 0.0) AS porc_factor_final,
        CASE 
            WHEN COALESCE(fc.porc_factor, 0.0) >= 80.0 AND 0.0 >= 80.0 THEN 1500 
            ELSE 1250 
        END AS bolsa_calculada
    FROM EmpleadosBase eb
    LEFT JOIN FactoresContencion fc ON eb.no_empleado = fc.no_empleado
)
SELECT 
    lb.no_empleado, lb.nombre_empleado, lb.bolsa_calculada,
    9 AS usuario_creacion, CURRENT_TIMESTAMP() AS fecha_creacion
FROM LogicaBolsa lb
INNER JOIN `data-warehouse-412715.dwh_cero.ifconf_bolsa_semanal` ix 
    ON lb.puesto_id = CAST(ix.puesto_id AS INT64)
    AND lb.porc_factor_final BETWEEN ix.rango_inicio AND ix.rango_fin
WHERE lb.no_empleado NOT IN (SELECT no_empleado FROM `{ifbolsa_semanal}`)