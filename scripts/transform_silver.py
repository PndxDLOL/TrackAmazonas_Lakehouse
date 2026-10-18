"""Tipos, validación, conflictos de identidad y auditoría de cada descarte."""
from pathlib import Path
from .common import connect, parquet_view, export_query, dump_json

def run(root, bronze_info, silver):
    root=Path(root);silver=Path(silver);silver.mkdir(parents=True,exist_ok=True)
    con=connect()
    parquet_view(con,"raw",root/bronze_info["bronze"]/"position_events.parquet")
    con.execute("""
    CREATE TEMP TABLE typed AS
    SELECT record_id AS source_record_id, trim(event_id) AS event_id,
      trim(vessel_id) AS vessel_id, try_cast(event_time AS TIMESTAMPTZ) AS event_time,
      try_cast(ingestion_time AS TIMESTAMPTZ) AS ingestion_time,
      trim(river) AS river, trim(route_id) AS route_id,
      try_cast(longitude AS DOUBLE) AS longitude, try_cast(latitude AS DOUBLE) AS latitude,
      try_cast(speed_kmh AS DOUBLE) AS speed_kmh,
      (speed_kmh IS NULL OR trim(speed_kmh)='') AS speed_missing,
      try_cast(route_reference_lat AS DOUBLE) AS route_reference_lat,
      try_cast(device_epoch AS BIGINT) AS device_epoch,
      try_cast(sequence_number AS BIGINT) AS sequence_number
    FROM raw
    """)
    con.execute("""
    CREATE TEMP TABLE digested AS SELECT *, sha256(to_json(struct_pack(
       vessel:=vessel_id,etime:=event_time,river:=river,route:=route_id,lon:=longitude,lat:=latitude,
       speed:=speed_kmh,reference:=route_reference_lat,epoch:=device_epoch,seq:=sequence_number))) AS payload_hash,
     CASE WHEN event_id IS NULL OR event_id='' OR vessel_id IS NULL OR vessel_id=''
        OR route_id IS NULL OR route_id='' THEN 'MISSING_ID'
       WHEN event_time IS NULL OR ingestion_time IS NULL THEN 'INVALID_TIME'
       WHEN river NOT IN ('Amazonas','Putumayo','Caqueta') OR river IS NULL THEN 'UNKNOWN_RIVER'
       WHEN longitude IS NULL OR latitude IS NULL OR NOT isfinite(longitude) OR NOT isfinite(latitude)
         OR longitude NOT BETWEEN -180 AND 180 OR latitude NOT BETWEEN -90 AND 90 THEN 'INVALID_COORDINATES'
       WHEN route_reference_lat IS NULL OR NOT isfinite(route_reference_lat)
         OR route_reference_lat NOT BETWEEN -90 AND 90 THEN 'INVALID_ROUTE_REFERENCE'
       WHEN (NOT speed_missing AND speed_kmh IS NULL) OR (speed_kmh IS NOT NULL AND
         (NOT isfinite(speed_kmh) OR speed_kmh NOT BETWEEN 0 AND 100)) THEN 'INVALID_SPEED'
       WHEN device_epoch IS NULL OR sequence_number IS NULL OR device_epoch<1 OR sequence_number<1
         THEN 'INVALID_SEQUENCE'
       WHEN ingestion_time<event_time THEN 'NEGATIVE_INGESTION_DELAY'
       ELSE NULL END AS invalid_reason FROM typed
    """)
    # Conservador: una identidad con dos payloads válidos NO se resuelve "last write wins".
    con.execute("""CREATE TEMP TABLE conflicts AS SELECT event_id FROM digested
        WHERE invalid_reason IS NULL GROUP BY event_id HAVING count(DISTINCT payload_hash)>1""")
    con.execute("""CREATE TEMP TABLE classified AS SELECT *,
        row_number() OVER(PARTITION BY event_id ORDER BY source_record_id) AS occurrence,
        CASE WHEN invalid_reason IS NOT NULL THEN invalid_reason
             WHEN event_id IN (SELECT event_id FROM conflicts) THEN 'IDENTITY_CONFLICT'
             ELSE NULL END AS rejection FROM digested""")
    # El ranking de duplicados se calcula entre válidos sin conflicto, no contra descartados.
    con.execute("""CREATE TEMP TABLE candidates AS SELECT *,
        row_number() OVER(PARTITION BY event_id ORDER BY source_record_id) AS retry_rank
        FROM classified WHERE rejection IS NULL""")
    con.execute("""CREATE TEMP TABLE clean AS SELECT source_record_id,event_id,vessel_id,
        cast(event_time AS TIMESTAMP) AS event_time,cast(ingestion_time AS TIMESTAMP) AS ingestion_time,
        river,route_id,longitude,latitude,speed_kmh,speed_missing,route_reference_lat,
        device_epoch,sequence_number,payload_hash,cast(event_time AS DATE) AS event_date
        FROM candidates WHERE retry_rank=1""")
    con.execute("""CREATE TEMP TABLE discarded AS
        SELECT source_record_id,rejection AS reason FROM classified WHERE rejection IS NOT NULL
        UNION ALL SELECT source_record_id,'DUPLICATE_RETRY' FROM candidates WHERE retry_rank>1""")
    export_query(con,"SELECT * FROM clean ORDER BY event_time,vessel_id,event_id",silver/"position_events.parquet")
    export_query(con,"SELECT r.*,d.reason FROM raw r JOIN discarded d ON r.record_id=d.source_record_id",
                 silver/"quarantine.parquet")
    reasons={k:int(n) for k,n in con.execute("SELECT reason,count(*) FROM discarded GROUP BY reason").fetchall()}
    original=int(con.execute("SELECT count(*) FROM raw").fetchone()[0])
    retained=int(con.execute("SELECT count(*) FROM clean").fetchone()[0])
    stats=con.execute("""SELECT min(event_time),max(event_time),count(DISTINCT vessel_id),
        count(*) FILTER(WHERE speed_kmh IS NULL),min(speed_kmh),max(speed_kmh),
        avg(speed_kmh) FROM clean""").fetchone()
    assert original==retained+sum(reasons.values())
    assert con.execute("SELECT count(*) FROM (SELECT event_id FROM clean GROUP BY 1 HAVING count(*)>1)").fetchone()[0]==0
    if retained==0: raise ValueError("Silver quedó vacío; no se publica Gold")
    quality=dict(bronze_rows=original,silver_rows=retained,discarded_rows=original-retained,
       retained_pct=round(retained*100/original,4),reasons=reasons,conservation_pass=True,
       event_time_min=str(stats[0]),event_time_max=str(stats[1]),vessels=int(stats[2]),
       missing_speed_rows=int(stats[3]),speed_min=stats[4],speed_max=stats[5],speed_avg=stats[6],
       policy="Velocidad ausente permanece NULL; conflictos válidos excluyen todas sus versiones; retrasos se conservan.")
    dump_json(silver/"quality.json",quality)
    text=["REPORTE DE CALIDAD SILVER",f"Bronze: {original}",f"Silver: {retained}",
          f"Descartes auditados: {original-retained}",f"Retención: {quality['retained_pct']}%",
          *[f"{k}: {v}" for k,v in sorted(reasons.items())],
          f"Velocidades sin medición: {stats[3]}",f"Rango temporal: {stats[0]} a {stats[1]}",
          f"Velocidad media de mediciones válidas: {stats[6]}",
          "Conciliación Bronze = Silver + descartes: PASS"]
    (silver/"reporte_calidad.txt").write_text("\n".join(text)+"\n",encoding="utf-8")
    con.close()
    print("  Silver:",retained,"filas;",original-retained,"descartes auditados")
    return quality
