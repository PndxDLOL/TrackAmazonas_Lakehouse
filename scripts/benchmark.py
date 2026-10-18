"""Mismos resultados antes de medir; caché caliente; se publican muestras individuales."""
from pathlib import Path
import platform
import statistics
import sqlite3
import time
import math
import duckdb
import pandas as pd
import pyarrow
from .common import open_gold, parquet_view, dump_json, literal

SOURCE_SQL="""
SELECT e.river AS rio,cast(substr(e.event_time,1,4) AS INTEGER) AS anio,
 cast(substr(e.event_time,6,2) AS INTEGER) AS mes,count(*) AS muestras,
 sum(CASE WHEN abs(cast(e.latitude AS REAL)-cast(e.route_reference_lat AS REAL))*111320>500 THEN 1 ELSE 0 END) AS anomalas
FROM position_events e JOIN accepted_ids a ON e.record_id=a.record_id
GROUP BY e.river,anio,mes ORDER BY anio,mes,rio
"""
GOLD_SQL="""
SELECT r.river_name AS rio,t.anio,t.mes,count(*) AS muestras,sum(f.anomaly_sample) AS anomalas
FROM fact_telemetria f JOIN dim_tiempo t ON f.date_sk=t.date_sk
JOIN dim_rio r ON f.river_sk=r.river_sk
GROUP BY r.river_name,t.anio,t.mes ORDER BY t.anio,t.mes,rio
"""
AGG_SQL="""
SELECT r.river_name AS rio,a.year AS anio,a.month AS mes,
 sum(a.event_count)::BIGINT AS muestras,sum(a.anomaly_samples)::BIGINT AS anomalas
FROM agg_rio_mes a JOIN dim_rio r ON a.river_sk=r.river_sk
GROUP BY r.river_name,a.year,a.month ORDER BY anio,mes,rio
"""

def equivalent(left,right):
    if len(left)!=len(right): return False
    for a,b in zip(left,right):
        if len(a)!=len(b): return False
        for x,y in zip(a,b):
            if isinstance(x,(int,float)) and isinstance(y,(int,float)):
                if not math.isclose(float(x),float(y),rel_tol=1e-9,abs_tol=1e-6):return False
            elif x!=y:return False
    return True

def measure_pair(label,left,right,repeats):
    first=left();second=right()
    if not equivalent(first,second):raise ValueError("Benchmark NO comparable: "+label)
    left();right()  # Un calentamiento por alternativa además de la comprobación.
    l=[];r=[]
    def timed(fn):
        start=time.perf_counter();out=fn();return (time.perf_counter()-start)*1000,out
    for iteration in range(repeats):
        order=[("left",left),("right",right)] if iteration%2==0 else [("right",right),("left",left)]
        for key,fn in order:
            elapsed,out=timed(fn)
            if not equivalent(first,out):raise ValueError("El resultado cambió mientras se medía")
            (l if key=="left" else r).append(elapsed)
    lm=statistics.median(l);rm=statistics.median(r)
    return dict(label=label,equal_results=True,left_ms=l,right_ms=r,
        left_median_ms=lm,right_median_ms=rm,left_min_ms=min(l),right_min_ms=min(r),
        ratio_left_over_right=lm/rm,result_rows=len(first),repeats=repeats,cache="warm",
        order="alternado; conexiones abiertas; incluye fetchall; excluye ETL")

def run(root,snapshot,evidence,repeats=7):
    root=Path(root);evidence=Path(evidence)
    con,gold,_=open_gold(root,snapshot)
    parquet_view(con,"fact_unpartitioned",gold/"fact_unpartitioned.parquet")
    source=sqlite3.connect(f"file:{(root/snapshot['source']).resolve().as_posix()}?mode=ro",uri=True)
    source.execute("CREATE TEMP TABLE accepted_ids(record_id INTEGER PRIMARY KEY)")
    ids=con.execute("SELECT source_record_id FROM fact_telemetria").fetchall()
    source.executemany("INSERT INTO accepted_ids VALUES (?)",ids)
    source.commit()  # Finalizar la escritura TEMP antes del snapshot de lectura.
    source.execute("BEGIN")  # La fuente del benchmark queda estable durante todos los ensayos.
    b1=measure_pair("SQLite fuente vs DuckDB Gold",lambda:source.execute(SOURCE_SQL).fetchall(),
                   lambda:con.execute(GOLD_SQL).fetchall(),repeats)
    months=con.execute("SELECT DISTINCT year,month FROM fact_telemetria ORDER BY 1,2").fetchall()
    year,month=months[len(months)//2]
    filterquery=lambda table: f"SELECT count(*),sum(anomaly_sample),sum(late_sample) FROM {table} WHERE year={year} AND month={month}"
    unfiltered=filterquery("fact_unpartitioned");partitioned=filterquery("fact_telemetria")
    b2=measure_pair("Opt 1 Parquet monolítico vs particionado",
                   lambda:con.execute(unfiltered).fetchall(),lambda:con.execute(partitioned).fetchall(),repeats)
    b3=measure_pair("Opt 2 detalle estrella vs agregado mensual",
                   lambda:con.execute(GOLD_SQL).fetchall(),lambda:con.execute(AGG_SQL).fetchall(),repeats)
    plans=evidence/"plans";plans.mkdir(parents=True,exist_ok=True)
    (plans/"sqlite_query_plan.txt").write_text("\n".join(map(str,source.execute("EXPLAIN QUERY PLAN "+SOURCE_SQL).fetchall())),encoding="utf-8")
    for name,sql in [("gold_monthly",GOLD_SQL),("opt1_unpartitioned",unfiltered),("opt1_partitioned",partitioned),("opt2_aggregate",AGG_SQL)]:
        plan="\n".join(str(x[-1]) for x in con.execute("EXPLAIN ANALYZE "+sql).fetchall())
        (plans/(name+".txt")).write_text(plan,encoding="utf-8")
    result=dict(comparisons=[b1,b2,b3],
       environment=dict(os=platform.system()+" "+platform.release(),python=platform.python_version(),
          machine=platform.machine(),duckdb=duckdb.__version__,pandas=pd.__version__,pyarrow=pyarrow.__version__,
          sqlite=sqlite3.sqlite_version,duckdb_threads=1),
       source_profile=snapshot["source_metadata"]["profile"],
       fact_rows=snapshot["validation"]["fact_rows"],month_filter=dict(year=int(year),month=int(month)),
       methodology="Comparación académica local: tipos convertidos y validación por IDs en SQLite; métricas preparadas en Gold. No aísla solo el formato columnar.",
       mongo_measured=False)
    dump_json(evidence/"benchmark.json",result)
    flat=[dict(comparison=b["label"],left_median_ms=b["left_median_ms"],
          right_median_ms=b["right_median_ms"],ratio=b["ratio_left_over_right"],equal_results=b["equal_results"]) for b in [b1,b2,b3]]
    pd.DataFrame(flat).to_csv(evidence/"benchmark.csv",index=False)
    (evidence/"benchmark_queries.sql").write_text("-- SQLite fuente\n"+SOURCE_SQL+
        "\n-- DuckDB Gold\n"+GOLD_SQL+"\n-- Opt 1 sin partición\n"+unfiltered+
        ";\n-- Opt 1 con partición\n"+partitioned+";\n-- Opt 2 agregado\n"+AGG_SQL,encoding="utf-8")
    for b in [b1,b2,b3]:
        print(f"  {b['label']}: {b['left_median_ms']:.3f} vs {b['right_median_ms']:.3f} ms | ratio {b['ratio_left_over_right']:.2f}")
    source.close();con.close()
    return result
