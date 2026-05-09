SELECT 
    sol.sucursal_id,
    (SELECT descripcion 
     FROM nucleocentral.ncfrecuencias_pago 
     WHERE clave = (SELECT valor 
                    FROM credito.crdatos_credito 
                    WHERE credito_id = cre.id 
                    AND conceptos_id = (SELECT id FROM nucleocentral.ncconceptos WHERE clave = 'CRE_CLV_FRE_PAG'))) AS periodo_pago, 
    saldos.venc,
    TO_DATE((CASE WHEN ncb.observaciones IS NULL THEN TO_CHAR(cre.fecha_contrato, 'DD/MM/YYYY') ELSE ncb.observaciones END), 'DD/MM/YYYY') AS fecha_contrato, 
    cre.monto,  
    COALESCE(saldos.saldo_capital, 0) AS saldo_capital, 
    COALESCE(saldos.saldo_interes, 0) AS saldo_interes,
    COALESCE(saldos.capital_vigente, 0) AS capital_vigente,
    COALESCE(saldos.interes_vigente, 0) AS interes_vigente,
    COALESCE(saldos.capital_vencido, 0) AS capital_vencido,
    COALESCE(saldos.interes_vencido, 0) AS interes_vencido,
    COALESCE(saldos.mora_info, 0) AS mora_info,
    COALESCE(saldos.amort_venc, 0) AS amort_venc,
    CASE WHEN salcaract.dias_venc < 0 THEN 0 ELSE salcaract.dias_venc END AS dias_venc,
    salcaract.calificacion,
    COALESCE(pagos_info.pagos, 0) AS pagos, 
    pagos_info.ultimo_pago AS ultimo_pago, 
    (SELECT valor 
     FROM credito.crdatos_credito 
     WHERE credito_id = cre.id 
     AND conceptos_id = (SELECT id FROM nucleocentral.ncconceptos WHERE clave = 'CRE_FTE_FDO')) AS fuentes,
    salcaract.tipo_cliente_id,
    (SELECT tipo FROM nucleocentral.nctipo_cliente WHERE id = salcaract.tipo_cliente_id) AS tipo_cliente,
    cre.cuenta, 
    (SELECT count(*) FROM credito.crdisposiciones WHERE credito_id = cre.id AND estatus <> 0) AS num_disposiciones,
    sol.persona_id, 
    salcaract.gestor_id, 
    salcaract.asesor_id,
    pro.descripcion AS producto,
    COALESCE(saldos.moratorios, 0) AS moratorios,
    COALESCE(saldos.falta_pago, 0) AS falta_pago,
    COALESCE(saldos.pago_tardio, 0) AS pago_tardio,
    (SELECT MAX(cdd.valor) 
     FROM credito.crcreditos cr 
     JOIN credito.crdisposiciones cd ON cr.id = cd.credito_id 
     JOIN credito.crestatus_disposiciones ce ON cd.estatus = ce.id 
     JOIN credito.crdatos_disposiciones cdd ON cd.no_disposicion = cdd.no_disposicion AND cdd.credito_id = cd.credito_id 
     JOIN nucleocentral.ncconceptos nc ON cdd.conceptos_id = nc.id 
     WHERE cr.cuenta = cre.cuenta AND nc.clave IN ('ETIQ_CRED') AND ce.clave = 'ACT') AS etiqueta_credito,
    CASE WHEN cre.esquema_cobro_id = 3 THEN 'PROV' ELSE '' END AS proveedor, 
    COALESCE(ncbn.observaciones, '') AS REVOLVENCIA,
    (SELECT MAX(cast(b.fecha_creacion as date)) FROM nucleocentral.ncbitacora b WHERE b.tipo_bitacora_id = 16 AND b.dato = cre.cuenta) AS ultimo_aumento,   
    (SELECT MAX(coalesce(cast(b.observaciones as double precision), 0.0)) FROM nucleocentral.ncbitacora b WHERE b.tipo_bitacora_id = 16 AND b.dato = cre.cuenta) AS monto_aumentado,
    (SELECT COUNT(b.id) FROM nucleocentral.ncbitacora b WHERE b.tipo_bitacora_id = 16 AND b.dato = cre.cuenta) AS numero_de_aumentos, 
    (SELECT EXTRACT(DAY FROM NOW() - d.fecha_creacion::date) FROM credito.crdisposiciones d WHERE d.credito_id = cre.id ORDER BY d.no_disposicion DESC LIMIT 1) AS Dias_sin_uso, 
    cec.etiqueta AS cve_esquema_cobro 
FROM credito.crcreditos cre 
JOIN credito.crcat_esquemas_cobro cec ON cre.esquema_cobro_id = cec.id 
JOIN solicitud.sosolicitudes sol ON cre.solicitud_id = sol.id  
JOIN credito.crsaldo_cartera_actual salcaract ON cre.cuenta = salcaract.cuenta  
JOIN productos.prproductos pro ON pro.id = sol.producto_id 
LEFT JOIN nucleocentral.ncbitacora ncbn ON cre.cuenta = ncbn.dato AND ncbn.tipo_bitacora_id = (SELECT id FROM nucleocentral.nctipos_bitacora WHERE clave = 'CRE_FECHA_NUEVA')                                 
LEFT JOIN nucleocentral.ncbitacora ncb ON cre.cuenta = ncb.dato AND ncb.tipo_bitacora_id = (SELECT id FROM nucleocentral.nctipos_bitacora WHERE clave = 'CRE_FECHA_ANTERIOR')
LEFT JOIN (
    SELECT 
        dis.credito_id,
        MIN(CASE WHEN cro.fecha_ven >= CURRENT_DATE THEN cro.fecha_ven END) AS venc,
        SUM(CASE WHEN crodet.ncconceptos_id = 47 THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS saldo_capital,
        SUM(CASE WHEN crodet.ncconceptos_id IN (48,49) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS saldo_interes,
        SUM(CASE WHEN cro.fecha_ven >= CURRENT_DATE AND crodet.ncconceptos_id = 47 THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS capital_vigente,
        SUM(CASE WHEN cro.fecha_ven >= CURRENT_DATE AND crodet.ncconceptos_id IN (48,49) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS interes_vigente,
        SUM(CASE WHEN cro.fecha_ven < CURRENT_DATE AND crodet.ncconceptos_id = 47 THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS capital_vencido,
        SUM(CASE WHEN cro.fecha_ven < CURRENT_DATE AND crodet.ncconceptos_id IN (48,49) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS interes_vencido,
        SUM(CASE WHEN crodet.ncconceptos_id IN (141,142) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS mora_info,
        SUM(CASE WHEN crodet.ncconceptos_id IN (50,51) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS moratorios,
        SUM(CASE WHEN cro.fecha_ven < CURRENT_DATE AND crodet.ncconceptos_id IN (206,207) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS falta_pago,
        SUM(CASE WHEN cro.fecha_ven < CURRENT_DATE AND crodet.ncconceptos_id IN (204,205) THEN crodet.monto - crodet.monto_pag ELSE 0 END) AS pago_tardio,
        COUNT(DISTINCT CASE WHEN cro.fecha_ven < CURRENT_DATE AND cro.estatus = 'PEND' THEN cro.id END) AS amort_venc
    FROM credito.crdisposiciones dis
    JOIN credito.crcronograma cro ON cro.credito_id = dis.credito_id AND cro.no_disposicion = dis.no_disposicion
    JOIN credito.crcronograma_det crodet ON cro.id = crodet.cronograma_id
    WHERE dis.estatus <> 0
    GROUP BY dis.credito_id
) saldos ON saldos.credito_id = cre.id
LEFT JOIN (
    SELECT 
        credito_id,
        COUNT(*) AS pagos,
        MAX(fecha_pago) AS ultimo_pago
    FROM credito.crcronograma
    WHERE estatus = 'PAG'
    GROUP BY credito_id
) pagos_info ON pagos_info.credito_id = cre.id
WHERE COALESCE(cre.validado, 0) = 3
AND COALESCE(cre.revisado, 0) = 3 
AND salcaract.no_disposicion = -1 
ORDER BY cre.cuenta;