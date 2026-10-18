-- Q2. Retraso captura-recepción, incluido backlog. No es solo latencia de red.
SELECT r.river_name AS rio,count(*) AS muestras,
  round(avg(f.ingestion_delay_s),3) AS retraso_medio_s,
  round(quantile_cont(f.ingestion_delay_s,.95),3) AS retraso_p95_s,
  round(quantile_cont(f.ingestion_delay_s,.99),3) AS retraso_p99_s,
  round(100.0*sum(f.late_sample)/count(*),3) AS muestras_mayores_60s_pct
FROM fact_telemetria f JOIN dim_rio r ON f.river_sk=r.river_sk
GROUP BY r.river_name ORDER BY retraso_p95_s DESC;
