-- Q1. Volumen y tasas sobre muestras, no número de accidentes.
SELECT r.river_name AS rio,t.anio,t.mes,
  count(*) AS muestras,sum(f.anomaly_sample) AS muestras_anomalas,
  round(100.0*sum(f.anomaly_sample)/count(*),3) AS tasa_anomalia_pct,
  round(avg(f.speed_kmh),3) AS velocidad_media_kmh
FROM fact_telemetria f
JOIN dim_tiempo t ON f.date_sk=t.date_sk
JOIN dim_rio r ON f.river_sk=r.river_sk
GROUP BY r.river_name,t.anio,t.mes ORDER BY t.anio,t.mes,r.river_name;
