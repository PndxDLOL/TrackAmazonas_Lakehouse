-- Q3. Rendimiento de rutas; AVG excluye nulos y se muestra su cobertura.
SELECT rt.route_id,r.river_name AS rio,count(*) AS muestras,
 count(f.speed_kmh) AS velocidades_validas,
 round(avg(f.speed_kmh),3) AS velocidad_media_kmh,
 round(100.0*sum(f.anomaly_sample)/count(*),3) AS anomalia_pct
FROM fact_telemetria f JOIN dim_ruta rt ON f.route_sk=rt.route_sk
JOIN dim_rio r ON f.river_sk=r.river_sk
GROUP BY rt.route_id,r.river_name ORDER BY anomalia_pct DESC;
