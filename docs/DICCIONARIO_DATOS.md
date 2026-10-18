# Diccionario y catálogo del Lakehouse TrackAmazonas

## Identidad y granularidad

FACT_TELEMETRIA tiene una fila por event_id válido de una medición GPS. event_id es dimensión degenerada; el identificador se conserva para auditoría. source_record_id apunta a record_id en Bronze y en la fuente SQLite. En una exportación V3, raw_extended_json conserva el documento BSON completo.

## Hecho atómico

| Campo | Tipo lógico | Significado y agregación |
|---|---|---|
| event_id | VARCHAR único | Identidad; no contar repetidamente reintentos |
| source_record_id | BIGINT | Linaje a la fila raw de este snapshot |
| date_sk | INTEGER FK | Fecha de captura UTC, YYYYMMDD |
| vessel_sk | BIGINT FK | Versión SCD2 vigente al capturar |
| river_sk | INTEGER FK | Río de la muestra |
| route_sk | INTEGER FK | Ruta de la muestra |
| event_time | TIMESTAMP UTC | Captura; se normaliza a UTC |
| ingestion_time | TIMESTAMP UTC | Recepción central; no ordena el estado histórico del catálogo |
| device_epoch | BIGINT | Época del dispositivo |
| sequence_number | BIGINT | Secuencia de captura; no confundir con row count global |
| longitude y latitude | DOUBLE | Coordenadas; no sumar |
| speed_kmh | DOUBLE nullable | Velocidad medida; AVG excluye ausentes |
| speed_missing | BOOLEAN | Cobertura de medición de velocidad |
| ingestion_delay_s | DOUBLE | Recepción menos captura; AVG/percentiles |
| deviation_m | DOUBLE | Aproximación de separación latitudinal del corredor |
| anomaly_sample | INTEGER 0/1 | Desviación mayor a 500 m; sumar muestras, no incidentes |
| late_sample | INTEGER 0/1 | Demora mayor a 60 s; sumar muestras |
| event_count | INTEGER 1 | Conteo aditivo |
| year y month | INTEGER | Partición física derivada de la captura UTC |

## Dimensiones

DIM_TIEMPO: date_sk PK, fecha, anio, mes, dia, trimestre, dia_semana ISO y es_fin_semana. Un día por fila y atributos inmutables.

DIM_EMBARCACION: vessel_sk PK, vessel_id natural, operator_name, service_class, valid_from, valid_to, is_current. Una versión por intervalo de barco. Vigencia [desde,hasta); fin NULL abierto. Los datos V3 no tienen catálogo histórico y se marcan NO_DOCUMENTADO. Los cambios académicos son sintéticos.

DIM_RIO: river_sk PK y river_name. Amazonas, Putumayo y Caqueta. La ortografía Caqueta conserva la clave técnica de la V3; en narrativa se usa Caquetá.

DIM_RUTA: route_sk PK, route_id y river_name. La pareja ruta/río evita un lookup ambiguo. La referencia de corredor específica de la muestra permanece en Silver.

## Agregado independiente

AGG_RIO_MES: año, mes y river_sk. Métricas event_count, anomaly_samples, late_samples, speed_sum, speed_count, delay_sum. Los conteos y numeradores son aditivos al cambiar agrupación, si cada muestra se incluye una sola vez. Media = speed_sum/speed_count. No se pueden recomponer p95 o p99 exactos sin distribución.

## Agregado horario V2

AGG_EMBARCACION_HORA: hour_utc (TIMESTAMP), vessel_sk, river_sk y route_sk identifican una fila. Métricas: event_count, anomaly_samples, late_samples, speed_sum, speed_count, speed_max y delay_sum. El conteo total reconcilia con FACT_TELEMETRIA. Media ponderada = SUM(speed_sum) / SUM(speed_count); no usar AVG de medias. speed_max se combina con MAX. Los percentiles exactos siguen consultándose sobre el detalle.

El grano incluye la versión SCD y la ruta: un barco puede tener varias filas en la misma hora. Q7 cuenta DISTINCT de vessel_id natural y hour_utc al informar horas-embarcación. Q8 consulta el detalle por ruta. No se suman filas del mart para inferir horas de navegación ni distancias recorridas.

El perfil académico V2 usa hasta cinco ventanas por barco, con GPS cada 30 segundos dentro de cada ráfaga. Hay intervalos sin muestreo entre ventanas; no demuestran fallos, disponibilidad ni cobertura continua del trimestre. En el ensayo de 500.000 originales resultaron 8.000 filas horarias y 496.989 hechos válidos.

## Gobierno mínimo y reporte de ejecución

Dueño: Equipo 4 TrackAmazonas. Propósito: laboratorio de analítica de telemetría. Clasificación: sintética en el perfil académico; ubicaciones sensibles si se importa una fuente operacional real. Bronze restringido en una implantación productiva, Gold y agregados según audiencia.

Linaje: source -> Bronze -> Silver -> Gold -> consulta. Cada lote contiene hashes y cada ejecución conserva catálogo, calidad, validación y benchmark. current.json identifica el snapshot activo. No existe eliminación automática ni un SLA de actualización productiva.

evidence/<execution_id>/reporte.html presenta cinco pestañas de una corrida validada, con filtro de filas y enlaces a CSV completos. Es un archivo local sin consultas al servidor ni refresco automático; no equivale al panel operativo V3. No se deben mezclar CSV ni snapshots de distintas corridas.

Retención, permisos de archivo, acceso por fila/columna, recuperación ante pérdida física y catálogo corporativo son decisiones de producción aún no implementadas en el laboratorio.

## Distinciones para exponer

Muestra anómala no es incidente. Retraso de llegada no es latencia pura de red. Datos observados no son un denominador de disponibilidad. Tres particiones Parquet no son réplicas. Un snapshot publicado por manifiesto no constituye un commit transaccional multi-tabla Delta/Iceberg.
