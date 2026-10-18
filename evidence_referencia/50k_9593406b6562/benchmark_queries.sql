-- SQLite fuente

SELECT e.river AS rio,cast(substr(e.event_time,1,4) AS INTEGER) AS anio,
 cast(substr(e.event_time,6,2) AS INTEGER) AS mes,count(*) AS muestras,
 sum(CASE WHEN abs(cast(e.latitude AS REAL)-cast(e.route_reference_lat AS REAL))*111320>500 THEN 1 ELSE 0 END) AS anomalas
FROM position_events e JOIN accepted_ids a ON e.record_id=a.record_id
GROUP BY e.river,anio,mes ORDER BY anio,mes,rio

-- DuckDB Gold

SELECT r.river_name AS rio,t.anio,t.mes,count(*) AS muestras,sum(f.anomaly_sample) AS anomalas
FROM fact_telemetria f JOIN dim_tiempo t ON f.date_sk=t.date_sk
JOIN dim_rio r ON f.river_sk=r.river_sk
GROUP BY r.river_name,t.anio,t.mes ORDER BY t.anio,t.mes,rio

-- Opt 1 sin partición
SELECT count(*),sum(anomaly_sample),sum(late_sample) FROM fact_unpartitioned WHERE year=2026 AND month=2;
-- Opt 1 con partición
SELECT count(*),sum(anomaly_sample),sum(late_sample) FROM fact_telemetria WHERE year=2026 AND month=2;
-- Opt 2 agregado

SELECT r.river_name AS rio,a.year AS anio,a.month AS mes,
 sum(a.event_count)::BIGINT AS muestras,sum(a.anomaly_samples)::BIGINT AS anomalas
FROM agg_rio_mes a JOIN dim_rio r ON a.river_sk=r.river_sk
GROUP BY r.river_name,a.year,a.month ORDER BY anio,mes,rio
