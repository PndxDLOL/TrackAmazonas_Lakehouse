"""Modelo estrella atómico, lookup temporal SCD2 y agregado en grano separado."""
from pathlib import Path
import pandas as pd
from .common import connect, parquet_view, export_query, stable_key, dump_json

def dimension_vessels(history):
    h=history.copy()
    h["effective_from"]=pd.to_datetime(h["effective_from"],utc=True,errors="raise").dt.tz_localize(None)
    for col in ("vessel_id","operator_name","service_class"):
        if h[col].isna().any() or (h[col].str.strip()=="").any():
            raise ValueError("Catálogo histórico con atributo obligatorio ausente")
    if h.duplicated(["vessel_id","effective_from"]).any():
        raise ValueError("Dos versiones del barco comienzan al mismo tiempo")
    h=h.sort_values(["vessel_id","effective_from"])
    previous=h.groupby("vessel_id")[["operator_name","service_class"]].shift(1)
    changed=(h[["operator_name","service_class"]]!=previous).any(axis=1)
    h=h.loc[changed].copy()
    h["valid_to"]=h.groupby("vessel_id")["effective_from"].shift(-1)
    h["is_current"]=h["valid_to"].isna()
    h["vessel_sk"]=[stable_key(v,t.isoformat()) for v,t in zip(h.vessel_id,h.effective_from)]
    if h.vessel_sk.duplicated().any(): raise ValueError("Colisión de claves SCD")
    if not (h.groupby("vessel_id").is_current.sum()==1).all():
        raise ValueError("Debe existir una sola versión actual por barco")
    return h.rename(columns={"effective_from":"valid_from"})[
        ["vessel_sk","vessel_id","operator_name","service_class","valid_from","valid_to","is_current"]]

def run(root, bronze_info, silver, gold):
    root=Path(root);gold=Path(gold);gold.mkdir(parents=True,exist_ok=True)
    h=pd.read_parquet(root/bronze_info["bronze"]/"vessel_history.parquet")
    dim=dimension_vessels(h)
    dim.to_parquet(gold/"dim_embarcacion.parquet",index=False,compression="zstd")
    con=connect()
    parquet_view(con,"s",Path(silver)/"position_events.parquet")
    parquet_view(con,"dim_embarcacion",gold/"dim_embarcacion.parquet")
    con.execute("""CREATE TEMP TABLE dim_tiempo AS
        SELECT cast(strftime(fecha,'%Y%m%d') AS INTEGER) AS date_sk,fecha,
        year(fecha)::INTEGER AS anio,month(fecha)::INTEGER AS mes,day(fecha)::INTEGER AS dia,
        quarter(fecha)::INTEGER AS trimestre,isodow(fecha)::INTEGER AS dia_semana,
        isodow(fecha)>=6 AS es_fin_semana FROM
        (SELECT cast(d AS DATE) AS fecha FROM generate_series(
            (SELECT min(event_date) FROM s),(SELECT max(event_date) FROM s),INTERVAL 1 DAY) t(d))""")
    con.execute("""CREATE TEMP TABLE dim_rio AS SELECT row_number() OVER(ORDER BY river)::INTEGER
        AS river_sk,river AS river_name FROM (SELECT DISTINCT river FROM s)""")
    con.execute("""CREATE TEMP TABLE dim_ruta AS SELECT row_number() OVER(ORDER BY route_id,river)::INTEGER
        AS route_sk,route_id,river AS river_name FROM (SELECT DISTINCT route_id,river FROM s)""")
    con.execute("""
    CREATE TEMP TABLE fact AS SELECT s.event_id,s.source_record_id,s.event_time,s.ingestion_time,
       t.date_sk,v.vessel_sk,r.river_sk,rt.route_sk,
       s.device_epoch,s.sequence_number,s.longitude,s.latitude,s.speed_kmh,s.speed_missing,
       epoch(s.ingestion_time-s.event_time) AS ingestion_delay_s,
       abs(s.latitude-s.route_reference_lat)*111320 AS deviation_m,
       (abs(s.latitude-s.route_reference_lat)*111320>500)::INTEGER AS anomaly_sample,
       (epoch(s.ingestion_time-s.event_time)>60)::INTEGER AS late_sample,
       1::INTEGER AS event_count,t.anio AS year,t.mes AS month
    FROM s JOIN dim_tiempo t ON s.event_date=t.fecha
      JOIN dim_embarcacion v ON s.vessel_id=v.vessel_id AND s.event_time>=v.valid_from
        AND (v.valid_to IS NULL OR s.event_time<v.valid_to)
      JOIN dim_rio r ON s.river=r.river_name
      JOIN dim_ruta rt ON s.route_id=rt.route_id AND s.river=rt.river_name
    """)
    source_count=con.execute("SELECT count(*) FROM s").fetchone()[0]
    fact_count=con.execute("SELECT count(*) FROM fact").fetchone()[0]
    if source_count!=fact_count:
        raise ValueError("El lookup de dimensiones perdió o multiplicó hechos")
    if con.execute("SELECT count(*) FROM (SELECT event_id FROM fact GROUP BY 1 HAVING count(*)>1)").fetchone()[0]:
        raise ValueError("Hechos duplicados")
    # Una versión publicada no se sobreescribe: los archivos pertenecen a un build único.
    order="SELECT * FROM fact ORDER BY date_sk,river_sk,event_time,event_id"
    export_query(con,order,gold/"fact_telemetria",["year","month"])
    export_query(con,order,gold/"fact_unpartitioned.parquet")
    for name in ("dim_tiempo","dim_rio","dim_ruta"):
        export_query(con,"SELECT * FROM "+name,gold/(name+".parquet"))
    con.execute("""CREATE TEMP TABLE agg_rio_mes AS SELECT year,month,river_sk,
        sum(event_count)::BIGINT AS event_count,
        sum(anomaly_sample)::BIGINT AS anomaly_samples,
        sum(late_sample)::BIGINT AS late_samples,
        sum(speed_kmh) AS speed_sum,count(speed_kmh)::BIGINT AS speed_count,
        sum(ingestion_delay_s) AS delay_sum
        FROM fact GROUP BY year,month,river_sk""")
    export_query(con,"SELECT * FROM agg_rio_mes ORDER BY year,month,river_sk",gold/"agg_rio_mes.parquet")
    con.execute("""CREATE TEMP TABLE agg_embarcacion_hora AS SELECT
        date_trunc('hour',event_time) AS hour_utc,vessel_sk,river_sk,route_sk,
        count(*)::BIGINT AS event_count,sum(anomaly_sample)::BIGINT AS anomaly_samples,
        sum(late_sample)::BIGINT AS late_samples,sum(speed_kmh) AS speed_sum,
        count(speed_kmh)::BIGINT AS speed_count,max(speed_kmh) AS speed_max,
        sum(ingestion_delay_s) AS delay_sum
        FROM fact GROUP BY hour_utc,vessel_sk,river_sk,route_sk""")
    hourly_rows=con.execute("SELECT count(*) FROM agg_embarcacion_hora").fetchone()[0]
    hourly_sum=con.execute("SELECT sum(event_count) FROM agg_embarcacion_hora").fetchone()[0]
    if hourly_sum!=fact_count: raise ValueError("El agregado horario no reconcilia con el hecho")
    export_query(con,"SELECT * FROM agg_embarcacion_hora ORDER BY hour_utc,vessel_sk,river_sk,route_sk",
                 gold/"agg_embarcacion_hora.parquet")
    evidence=con.execute("""SELECT v.vessel_id,v.operator_name,v.service_class,v.valid_from,v.valid_to,
          count(f.event_id) AS samples
        FROM dim_embarcacion v LEFT JOIN fact f USING(vessel_sk)
        WHERE v.vessel_id='TA-001'
        GROUP BY ALL ORDER BY v.valid_from""").fetchdf()
    evidence.to_csv(gold/"scd_demo.csv",index=False)
    validation=dict(fact_rows=fact_count,unique_events=fact_count,
       vessel_versions=len(dim),vessels=int(dim.vessel_id.nunique()),
       scd_changed_vessels=int((dim.groupby("vessel_id").size()>1).sum()),
       lookup_exactly_one=True,scd_current_unique=True,foreign_keys_pass=True,
       scd_interval_policy="[valid_from, valid_to); NULL representa fin abierto",
       fact_grain="Una fila por event_id válido de telemetría",
       aggregate_grain="Una fila por año, mes y río",hourly_rows=hourly_rows,
       hourly_count_reconciled=True,
       hourly_grain="Una fila por hora UTC, versión de embarcación, río y ruta")
    dump_json(gold/"validation.json",validation)
    con.close()
    print("  Gold:",fact_count,"hechos;",len(dim),"versiones de embarcación")
    return validation
