# Contraste crítico de las propuestas del Taller 3

La referencia de entrega es la guía práctica y la rúbrica aportadas por el equipo. El resumen del módulo sirve de teoría. Se distinguen diseños propuestos, archivos presentes y mecanismos probados.

## Decisiones de consolidación

| Propuesta | Aporte | Tratamiento |
|---|---|---|
| Informe DOCX | Objetivos, supuestos, esquema y estado de implementación | Incorporado a la documentación |
| Informe DOCX | Cambios SCD con fechas efectivas y atributos reales | Se conserva el lookup temporal y el fixture explícito |
| ZIP/PDF Copilot | Resumen embarcación-hora separado del detalle | Implementado con versión SCD, río y ruta |
| ZIP/PDF Copilot | Delta como profundización | Preparado aparte; no validado por error local de escritura |
| Diagrama PNG | Calidad, gobierno y versionamiento transversales | Diagrama ajustado al código |
| Diagrama PNG | Visualización de resultados | Reporte HTML local con cinco pestañas |

## Hallazgos verificables en el ZIP

Se leyó el código y los Parquet sin ejecutar su main.py.

Bronze contiene 20.200 filas y Gold 19.964. DIM_VESSEL tiene 800 filas para 800 barcos: cero cambios históricos observables. transform_gold.py hace merge por vessel_id, sin intervalo temporal. Los campos SCD están preparados, pero no demuestran una transición; añadir dos versiones a ese merge podría multiplicar hechos.

El benchmark de analyze.py lee la SQLite cruda de 20.200 filas y la compara con 19.964 hechos depurados. Por ejemplo, Amazonas tiene 8.990 filas raw y 8.881 hechos. No se comprueba igualdad. El speedup incluido 2,55 no se adopta como comparación válida entre resultados equivalentes.

Silver lee todos los position_events_*.parquet y todos los vessels_*.parquet. Repetir extracciones aumenta entradas y puede mezclar fuentes. La V2 usa un batch y snapshot exactos; no pide borrar Bronze para cambiar de perfil.

La distancia speed_kmh * 30/3600 supone intervalos de 30 s por barco. El generador avanza 30 s globalmente y elige un barco al azar. Los intervalos por embarcación pueden ser mucho mayores. Esa aproximación no acredita distancia recorrida y no se incorpora.

Q6 promedia medias horarias. Con tamaños desiguales produce sesgo. La V2 guarda sumas y cantidades y utiliza media ponderada.

Los archivos Delta incluyen la versión 0. Eso acredita el archivo suministrado; no prueba time travel entre dos commits ni una transacción entre todas las tablas.

validate.py comprueba unicidad, barco no nulo, coordenadas y existencia de reportes. Su PASS no prueba igualdad de benchmark, transición SCD ni incrementalidad. Los conteos de auditoría están en auditoria_propuestas.json.

## Esquema del proyecto y diagrama

La V3 utiliza trackamazonas_v3_<run_id> y position_events, vessel_state, anomaly_alerts, message_receipts y quarantine. No implementa el catálogo vessels ni clientes, órdenes o paquetes. El adaptador consolidado preserva los documentos y marca como desconocidos los atributos de catálogo.

heading y signal_quality son extensiones sintéticas del ZIP, no campos de la V3 conocida. Su extracción Mongo no garantiza que Silver/Gold funcione si exige esas columnas. El perfil consolidado usa los campos existentes y no inventa indicadores de señal.

El diagrama ajustado representa las transformaciones en su lugar: raw, limpieza, modelo y consumo. No muestra dbt, Great Expectations o Metabase como ejecutados cuando no forman parte del código.

## Requisitos y herramientas opcionales

La guía práctica, página 2, permite Python puro sin dbt ni Airflow y presenta dbt y visualización como opciones. El DOCX usa más el stack del resumen del módulo: dbt/GX son un diseño pendiente, no funciones implementadas en el ZIP.

Mongo es continuidad del caso y SQLite el perfil académico medido. PostgreSQL recomendado no se interpreta como obligación de sustituir la V3; la guía contempla varias fuentes por proyecto.

Metabase con DuckDB requiere un driver de terceros y versiones/rutas compatibles. El reporte HTML cubre la visualización local sin añadir ese servicio. Los índices de tablas DuckDB no se atribuyen a Parquet externo: se demuestran proyección, filtros, pruning y agregación.

## Evidencias por criterio

| Criterio | Mecanismo | Evidencia |
|---|---|---|
| Modelo | 4 dimensiones, SCD temporal, grano explícito | validation.json, scd_demo.csv, diagrama |
| Pipeline | Lote raw inmutable, causas excluyentes, cuarentena | quality.json, reporte_calidad.txt |
| Analítica | 8 consultas, resumen horario, retrasos por ruta | SQL y CSV |
| Benchmark | Población y resultados iguales, 9 repeticiones | benchmark.json y planes |
| Reproducción | main.py, lock, hashes y publicación al finalizar | pruebas y logs |
| Sustentación | Muestra por ráfagas declarada y límites | informe y guía |

## Límites de la versión consolidada

El núcleo pasó el ensayo de 500.000 originales. Las siete pruebas base aprobaron; la prueba Delta queda omitida por defecto. El ensayo Delta local encontró Unable to rename file / Acceso denegado (os error 5). No se afirma que esté validado y no condiciona main.py.

Docker/Mongo requiere una instancia activa y su propia medición. GitHub debe publicarlo el equipo. La duración definitiva debe confirmarse: guía práctica 20 minutos y resumen del módulo 15. Las diferencias de pesos se documentan sin calcular una nota artificial.

## Fuentes

Informe_Actualizado_TrackAmazonas_Mini_Lakehouse_Modulo3.docx; Mina-Lakehouse Copiloto.pdf, 12 páginas; ZIP y diagrama PNG aportados por el equipo; guía y rúbrica del Taller 3; código V3 y solución anterior.

Documentación primaria: DuckDB, delta-io/delta-rs y motherduckdb/metabase_duckdb_driver.
