"""Benchmark opcional del motor Mongo real; rechaza corridas o poblaciones distintas."""
import argparse
from pathlib import Path
import subprocess
import json
from .common import open_gold,dump_json
from .benchmark import measure_pair,GOLD_SQL,equivalent

def run(root,compose_dir,run_id,repeats):
    if not run_id.isalnum() or len(run_id)>40:raise ValueError("run_id inválido")
    con,_,manifest=open_gold(root)
    if manifest["source_metadata"]["profile"]!="v3_json_mirror":
        raise ValueError("Publicar primero el snapshot exportado de esa V3")
    if manifest["quality"]["discarded_rows"]:
        raise ValueError("Comparación Mongo requiere población limpia idéntica, sin descartes")
    ids=con.execute("SELECT event_id FROM fact_telemetria ORDER BY event_id").fetchall()
    if not all(str(e[0]).startswith(run_id+":") for e in ids):
        raise ValueError("Gold pertenece a otra corrida")
    remote="""
import os,json,time,statistics
from pymongo import MongoClient
c=MongoClient(os.environ['MONGO_URI'],tz_aware=True,serverSelectionTimeoutMS=6000)
db=c['trackamazonas_v3_'+RUN_ID]
pipe=[{'$group':{'_id':{'rio':'$river','anio':{'$year':'$event_time'},'mes':{'$month':'$event_time'}},
 'muestras':{'$sum':1},'anomalas':{'$sum':{'$cond':[{'$gt':[{'$multiply':[{'$abs':{'$subtract':[{'$arrayElemAt':['$location.coordinates',1]},'$route_reference_lat']}},111320]},500]},1,0]}}}},
 {'$sort':{'_id.anio':1,'_id.mes':1,'_id.rio':1}}]
def query(session):
 return [[x['_id']['rio'],x['_id']['anio'],x['_id']['mes'],x['muestras'],x['anomalas']] for x in db.position_events.aggregate(pipe,session=session)]
with c.start_session(snapshot=True) as s:
 ids=sorted(x['_id'] for x in db.position_events.find({}, {'_id':1},session=s))
 out=query(s);query(s);times=[]
 for i in range(REPEATS):
  t=time.perf_counter();query(s);times.append((time.perf_counter()-t)*1000)
print(json.dumps({'ids':ids,'result':out,'times_ms':times}))
c.close()
""".replace("RUN_ID",repr(run_id)).replace("REPEATS",str(repeats))
    result=subprocess.run(["docker","compose","exec","-T","app","python","-c",remote],
                           cwd=compose_dir,capture_output=True,text=True,check=True)
    mongo=json.loads(result.stdout)
    if mongo["ids"]!=[x[0] for x in ids]:raise ValueError("Las identidades de Mongo y Gold difieren")
    gold=con.execute(GOLD_SQL).fetchall()
    if not equivalent(mongo["result"],gold):raise ValueError("El resultado agregado no coincide")
    import time,statistics
    con.execute(GOLD_SQL).fetchall();measure=[]
    for _ in range(repeats):
        t=time.perf_counter();con.execute(GOLD_SQL).fetchall();measure.append((time.perf_counter()-t)*1000)
    evidence=dict(mongo_median_ms=statistics.median(mongo["times_ms"]),
       duckdb_median_ms=statistics.median(measure),mongo_samples=mongo["times_ms"],duckdb_samples=measure,
       equal_ids=True,equal_results=True,run_id=run_id,
       caveat="Mongo incluye transporte cliente-servidor dentro de Docker; DuckDB es embebido. Ensayos secuenciales, caché caliente; no aísla coste de columna.")
    out=Path(root)/manifest["evidence"]/"benchmark_mongo_real.json"
    dump_json(out,evidence);con.close();print(json.dumps(evidence,indent=2))

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--compose-dir",type=Path,required=True);p.add_argument("--run-id",required=True)
    p.add_argument("--root",type=Path,default=Path(__file__).resolve().parents[1])
    p.add_argument("--repeats",type=int,default=7)
    a=p.parse_args();run(a.root,a.compose_dir,a.run_id,a.repeats)
if __name__=="__main__":main()
