-- Sumar numeradores y denominadores; no promediar medias de grupos desiguales.
SELECT r.river_name AS rio,
  count(DISTINCT (v.vessel_id,h.hour_utc)) AS horas_embarcacion_observadas,
  count(*) AS filas_del_mart,
  sum(h.event_count) AS muestras,
  round(sum(h.speed_sum)/nullif(sum(h.speed_count),0),3) AS velocidad_media_ponderada,
  round(100.0*sum(h.late_samples)/sum(h.event_count),3) AS tardias_pct
FROM agg_embarcacion_hora h
JOIN dim_rio r USING(river_sk)
JOIN dim_embarcacion v USING(vessel_sk)
GROUP BY r.river_name ORDER BY r.river_name;
