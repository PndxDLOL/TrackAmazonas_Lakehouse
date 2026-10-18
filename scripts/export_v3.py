"""Exportación de lectura del replica set de la V3 por su propia red Docker."""
import argparse
from pathlib import Path
import subprocess
from datetime import datetime,timezone
from .common import dump_json,file_hash

def export(compose_dir,run_id,output):
    if not run_id.isalnum() or len(run_id)>40:raise ValueError("run_id inválido")
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    remote="""
import os
from pymongo import MongoClient
from bson.json_util import dumps
from datetime import datetime, timezone
client=MongoClient(os.environ['MONGO_URI'],tz_aware=True,serverSelectionTimeoutMS=6000)
db=client['trackamazonas_v3_'+RUN_ID]
with client.start_session(snapshot=True) as session:
    for doc in db.position_events.find({},session=session).sort('_id',1):
        print(dumps(doc),flush=True)
client.close()
""".replace("RUN_ID",repr(run_id))
    # Exportar después de PASS y con captura cerrada. No se modifican las colecciones.
    command=["docker","compose","exec","-T","app","python","-c",remote]
    with output.open("x",encoding="utf-8") as stream:
        result=subprocess.run(command,cwd=Path(compose_dir),stdout=stream,stderr=subprocess.PIPE,text=True)
    if result.returncode:
        raise RuntimeError("Exportación incompleta conservada para diagnóstico: "+result.stderr)
    if output.stat().st_size==0:raise ValueError("No hay eventos; comprobar run_id")
    dump_json(output.with_suffix(".meta.json"),dict(run_id=run_id,sha256=file_hash(output),
         exported_at=datetime.now(timezone.utc).isoformat(),read_only=True,snapshot_session=True))
    print("Exportado:",output)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--compose-dir",type=Path,required=True)
    p.add_argument("--run-id",required=True)
    p.add_argument("--output",type=Path,required=True)
    a=p.parse_args();export(a.compose_dir,a.run_id,a.output)
if __name__=="__main__":main()
