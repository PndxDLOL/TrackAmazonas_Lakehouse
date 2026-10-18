# Matriz de correspondencia con la rúbrica

| Criterio | Indicador sobresaliente solicitado | Evidencia local |
|---|---|---|
| Modelo dimensional | Estrella 3+ dimensiones, grano atómico, SCD implementado y justificado | modelo_dimensional.png, transform_gold.py, Q5, scd_demo.csv |
| Pipeline | Tres zonas Parquet, calidad explícita, nulos/duplicados | main.py, reporte_calidad.txt, quality.json, quarantine.parquet |
| Consultas y benchmark | 5+ consultas significativas y comparación medida con análisis | 8 SQL, CSV, benchmark.json/csv, planes y capítulos analíticos |
| Reproducibilidad | main.py y README ejecutables desde cero | requirements.txt, README, comandos y pruebas |
| Creatividad y sustentación | Dominio propio y explicación por todos | Telemetría, retrasos, SCD embarcación, V3, guía de exposición |

Los artefactos identifican decisiones y resultados; no representan una nota otorgada.

Discrepancias a confirmar: DOCX indica Peso 20% en todos, mientras sus encabezados y guía listan 25/20/20/10/25. Guía de exposición 20 minutos, resumen módulo 15. La solución cubre todos los indicadores y ofrece dos tiempos.

Pendientes del equipo antes de entregar: publicar GitHub, ejecutar desde un clon limpio, confirmar duración y ensayar main.py más dos consultas. Si se desea acreditar Mongo real, encender V3 y conservar benchmark_mongo_real.json. La medición SQLite no es esa prueba.

Complementos del módulo incluidos aunque la guía base simplifica el stack: dos optimizaciones medidas, EXPLAIN ANALYZE, partición Parquet, catálogo mínimo, linaje y discusión de formato transaccional.
