-- Q4. Lista para revisión; no atribuye infracciones ni cuenta incidentes.
SELECT v.vessel_id,count(*) AS muestras,sum(f.anomaly_sample) AS muestras_anomalas,
 round(100.0*sum(f.anomaly_sample)/count(*),3) AS anomalia_pct,
 round(quantile_cont(f.ingestion_delay_s,.95),3) AS retraso_p95_s
FROM fact_telemetria f JOIN dim_embarcacion v ON f.vessel_sk=v.vessel_sk
GROUP BY v.vessel_id HAVING count(*)>=20
ORDER BY anomalia_pct DESC,muestras DESC,v.vessel_id LIMIT 10;
