-- Priorizar revisión por ruta con un denominador explícito.
SELECT rt.route_id,r.river_name AS rio,count(*) AS muestras,
  round(100.0*sum(f.late_sample)/count(*),3) AS tardias_pct,
  round(quantile_cont(f.ingestion_delay_s,.5),3) AS retraso_mediana_s,
  round(quantile_cont(f.ingestion_delay_s,.95),3) AS retraso_p95_s,
  round(quantile_cont(f.ingestion_delay_s,.99),3) AS retraso_p99_s
FROM fact_telemetria f JOIN dim_ruta rt USING(route_sk)
JOIN dim_rio r USING(river_sk)
GROUP BY rt.route_id,r.river_name
ORDER BY tardias_pct DESC,rt.route_id;
