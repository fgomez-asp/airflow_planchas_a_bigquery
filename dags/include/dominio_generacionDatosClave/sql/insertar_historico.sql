INSERT INTO `{ifbolsa_semanal_historico}` (
    id, no_empleado, nombre_empleado, bolsa_semanal, 
    usuario_creacion, fecha_creacion, usuario_modificacion, fecha_modificacion
)
SELECT 
    id, no_empleado, nombre_empleado, bolsa_semanal, 
    usuario_creacion, fecha_creacion, usuario_modificacion, fecha_modificacion
FROM `{ifbolsa_semanal}`