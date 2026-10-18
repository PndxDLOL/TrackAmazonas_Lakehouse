-- Q6. El día corresponde a la captura, aunque se recibiera después.
SELECT t.fecha,count(*) AS muestras,sum(f.late_sample) AS muestras_tardias,
 round(100.0*sum(f.late_sample)/count(*),3) AS tardias_pct,
 count(DISTINCT v.vessel_id) AS embarcaciones_observadas
FROM fact_telemetria f JOIN dim_tiempo t ON f.date_sk=t.date_sk
JOIN dim_embarcacion v ON f.vessel_sk=v.vessel_sk
GROUP BY t.fecha ORDER BY t.fecha;
