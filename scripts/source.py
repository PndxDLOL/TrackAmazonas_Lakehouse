"""Fuente row-store académica y espejo fiel de una exportación de Mongo V3."""
from pathlib import Path
import json
import sqlite3
from contextlib import closing
import hashlib
import numpy as np
import pandas as pd
from .common import dump_json, file_hash

FIELDS = ["event_id","vessel_id","event_time","ingestion_time","river","route_id",
          "longitude","latitude","speed_kmh","route_reference_lat","device_epoch",
          "sequence_number","raw_extended_json"]

def create_db(path, rows, history, metadata):
    path = Path(path)
    if path.exists():
        raise FileExistsError("La fuente existe; se preserva: "+str(path))
    con = sqlite3.connect(path)
    try:
        con.execute("CREATE TABLE position_events(record_id INTEGER PRIMARY KEY," +
                    ",".join(f"{x} TEXT" for x in FIELDS)+")")
        con.execute("CREATE TABLE vessel_history(vessel_id TEXT,effective_from TEXT,operator_name TEXT,service_class TEXT)")
        con.execute("CREATE TABLE source_metadata(payload TEXT)")
        con.execute("INSERT INTO source_metadata VALUES (?)",(json.dumps(metadata, ensure_ascii=False),))
        sql="INSERT INTO position_events VALUES ("+",".join("?" for _ in range(len(FIELDS)+1))+")"
        for start in range(0,len(rows),10000):
            chunk=rows[start:start+10000]
            con.executemany(sql, [(i+start+1,*[r.get(k) for k in FIELDS]) for i,r in enumerate(chunk)])
        con.executemany("INSERT INTO vessel_history VALUES (?,?,?,?)", history)
        con.execute("CREATE INDEX idx_vessel_event_time ON position_events(vessel_id,event_time)")
        con.commit()
    finally:
        con.close()
    return path

def seed_academic(root, count=50000, seed=42):
    if count < 800:
        raise ValueError("Usar al menos 800 filas para cubrir las embarcaciones")
    directory=Path(root)/"source"
    directory.mkdir(parents=True,exist_ok=True)
    path=directory/f"academic_bursts_v2_{count}_{seed}.sqlite"
    if path.exists():
        with closing(sqlite3.connect(path)) as c:
            config=json.loads(c.execute("SELECT payload FROM source_metadata").fetchone()[0])
        if config.get("rows_requested")!=count or config.get("seed")!=seed:
            raise ValueError("Fuente existente incompatible")
        return path
    rng=np.random.default_rng(seed)
    n=np.arange(count); v=n%800+1; seq=n//800+1
    ticks=int(seq.max())
    # Cinco ventanas distribuidas en el trimestre: GPS cada 30 s dentro de cada
    # ráfaga. No se afirma captura continua durante los intervalos no muestreados.
    bursts=min(5,ticks)
    samples_per_burst=int(np.ceil(ticks/bursts))
    block=(seq-1)//samples_per_burst
    last_block=int(block.max())
    offsets=np.floor(block*89*86400/max(1,last_block)).astype("int64")
    offsets+=((seq-1)%samples_per_burst)*30+v%30
    times=pd.to_datetime(pd.Timestamp("2026-01-01").value+offsets*10**9,utc=True)
    river=np.array(["Amazonas","Putumayo","Caqueta"])[(v-1)%3]
    reference=np.array([-4.2153,-2.57,-1.61])[(v-1)%3]+v*.00002
    probs=np.where(river=="Amazonas",.09,np.where(river=="Putumayo",.05,.03))
    anomaly=rng.random(count)<probs
    lat=reference+np.where(anomaly,.006,rng.normal(0,.0004,count))
    lon=np.array([-69.9406,-70.01,-72.69])[(v-1)%3]+rng.normal(0,.01,count)
    speed=rng.uniform(8,24,count); speed[rng.random(count)<.05]=0
    lag=np.where(v<=40,rng.uniform(3600,21600,count),rng.uniform(.8,3,count))
    ing=times+pd.to_timedelta(lag,unit="s")
    frame=pd.DataFrame(dict(
        event_id=[f"LAB:{x:07d}" for x in n],
        vessel_id=[f"TA-{x:03d}" for x in v],
        event_time=times.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        ingestion_time=ing.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        river=river,route_id=["R-"+r+"-sintetica" for r in river],
        longitude=np.round(lon,6).astype(str),latitude=np.round(lat,6).astype(str),
        speed_kmh=np.round(speed,3).astype(str),
        route_reference_lat=np.round(reference,6).astype(str),
        device_epoch="1",sequence_number=seq.astype(str),raw_extended_json=None))
    frame.loc[np.arange(17,count,333),"speed_kmh"]=None  # opcional: no se inventa velocidad.
    bad=np.arange(0,count,199)
    frame.loc[bad[0::3],"event_time"]="FECHA_INVALIDA"
    frame.loc[bad[1::3],"latitude"]="99.0"
    frame.loc[bad[2::3],"vessel_id"]=None
    duplicate=frame.iloc[np.arange(51,count,100)].copy()
    conflict=frame.iloc[np.arange(101,count,1000)].copy()
    conflict["longitude"]=(conflict["longitude"].astype(float)+.002).astype(str)
    rows=pd.concat([frame,duplicate,conflict],ignore_index=True).to_dict("records")
    history=[]
    for vessel in range(1,801):
        vessel_id=f"TA-{vessel:03d}"
        history.append((vessel_id,"2026-01-01T00:00:00Z","Operador A","REGULAR"))
        if vessel<=80:
            history.append((vessel_id,"2026-02-15T00:00:00Z","Operador B","PRIORITARIA"))
    return create_db(path,rows,history,dict(profile="academic_sqlite",rows_requested=count,seed=seed,
        synthetic=True,dataset_version="2.0.0",
        description="Ráfagas GPS cada 30 s en cinco ventanas de 800 barcos dentro de 90 días; no es captura continua."))

def import_v3(root, input_path):
    from bson.json_util import loads
    directory=Path(root)/"source";directory.mkdir(parents=True,exist_ok=True)
    digest=file_hash(input_path)
    path=directory/("v3_mirror_"+digest[:16]+".sqlite")
    if path.exists():
        return path
    rows=[]; earliest={}
    for line in Path(input_path).read_text(encoding="utf-8-sig").splitlines():
        if not line.strip(): continue
        doc=loads(line)
        def iso(val):
            return val.isoformat() if hasattr(val,"isoformat") else val
        coordinates=doc.get("location",{}).get("coordinates",[None,None])
        e=dict(event_id=doc.get("event_id",doc.get("_id")),vessel_id=doc.get("vessel_id"),
               event_time=iso(doc.get("event_time")),ingestion_time=iso(doc.get("ingestion_time")),
               river=doc.get("river"),route_id=doc.get("route_id"),longitude=coordinates[0],
               latitude=coordinates[1],speed_kmh=doc.get("speed_kmh"),
               route_reference_lat=doc.get("route_reference_lat"),device_epoch=doc.get("device_epoch"),
               sequence_number=doc.get("sequence_number"),raw_extended_json=line)
        rows.append({k:None if x is None else str(x) for k,x in e.items()})
        if e["vessel_id"] and e["event_time"]:
            earliest[e["vessel_id"]]=min(earliest.get(e["vessel_id"],e["event_time"]),e["event_time"])
    if not rows: raise ValueError("La exportación no contiene eventos")
    # La V3 no implementa catálogo histórico: no se inventa un operador ni un cambio.
    history=[(v,when,"NO_DOCUMENTADO","NO_DOCUMENTADA") for v,when in sorted(earliest.items())]
    return create_db(path,rows,history,dict(profile="v3_json_mirror",synthetic=False,
         export_hash=digest,rows_exported=len(rows),catalog_origin="inferido sin atributos de negocio",
         benchmark_source="SQLite espejo del snapshot; NO equivale a medir MongoDB"))
