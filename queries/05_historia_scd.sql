-- Q5. Cambio real en el fixture académico, lookup según tiempo del evento.
SELECT v.vessel_id,v.operator_name,v.service_class,v.valid_from,v.valid_to,
 count(f.event_id) AS muestras,
 round(avg(f.speed_kmh),3) AS velocidad_media_kmh
FROM dim_embarcacion v LEFT JOIN fact_telemetria f ON f.vessel_sk=v.vessel_sk
WHERE v.vessel_id='TA-001'
GROUP BY v.vessel_id,v.operator_name,v.service_class,v.valid_from,v.valid_to
ORDER BY v.valid_from;
