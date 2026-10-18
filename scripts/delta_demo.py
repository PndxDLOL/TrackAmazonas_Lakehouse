"""Profundización aislada: dos versiones Delta y un evento tardío de laboratorio."""
from pathlib import Path
import argparse
from datetime import timedelta
import uuid
import pyarrow as pa
from .common import open_gold,dump_json,load_json,now_iso

def run(root,sample_size=1000):
    if sample_size<1:raise ValueError("El tamaño de muestra debe ser positivo")
    try:
        from deltalake import DeltaTable,write_deltalake
    except ImportError as exc:
        raise RuntimeError("Instalar requirements-delta.txt para la profundización") from exc
    root=Path(root).resolve()
    con,gold,snapshot=open_gold(root)
    before_current=(root/"lakehouse"/"current.json").read_bytes()
    try:
        sample=con.execute("SELECT * FROM fact_telemetria ORDER BY event_time,event_id LIMIT ?",[sample_size]).to_arrow_table()
    finally:
        con.close()
    if sample.num_rows<1:raise ValueError("No hay hechos para demostrar")
    sample=sample.append_column("demo_event",pa.array([False]*sample.num_rows))
    identifier=uuid.uuid4().hex[:12]
    directory=root/"lakehouse"/"delta_demo"/identifier
    table_path=directory/"fact_demo"
    directory.mkdir(parents=True,exist_ok=False)
    write_deltalake(str(table_path),sample,mode="error")
    new=sample.slice(0,1).to_pylist()[0]
    new["event_id"]="LAB-DELTA-LATE:"+identifier
    new["source_record_id"]=-1  # Fila sintética del ejercicio, ajena a Bronze.
    new["event_time"]=new["event_time"]+timedelta(milliseconds=500)
    new["ingestion_time"]=new["event_time"]+timedelta(hours=2)
    new["ingestion_delay_s"]=7200.0
    new["late_sample"]=1
    new["sequence_number"]=999999
    new["demo_event"]=True
    extra=pa.Table.from_pylist([new],schema=sample.schema)
    write_deltalake(str(table_path),extra,mode="append")
    old=DeltaTable(str(table_path),version=0).to_pyarrow_table()
    current=DeltaTable(str(table_path),version=1).to_pyarrow_table()
    checks=dict(version0_rows=old.num_rows,version1_rows=current.num_rows,
       expected_delta_rows=1,version0_demo_events=sum(old.column("demo_event").to_pylist()),
       version1_demo_events=sum(current.column("demo_event").to_pylist()),
       original_publication_unchanged=(root/"lakehouse"/"current.json").read_bytes()==before_current,
       log_files=len(list((table_path/"_delta_log").glob("*.json"))))
    if checks["version0_rows"]!=sample.num_rows or checks["version1_rows"]!=sample.num_rows+1:
        raise ValueError("El time travel no reconstruyó los conteos esperados")
    if checks["version0_demo_events"]!=0 or checks["version1_demo_events"]!=1 or not checks["original_publication_unchanged"]:
        raise ValueError("La demostración contaminó el snapshot o la historia")
    result=dict(result="PASS",created_at=now_iso(),source_batch=snapshot["batch_id"],
       table=str(table_path.relative_to(root)),checks=checks,
       scope="Una tabla Delta aislada. No es un commit atómico entre hechos y dimensiones.",
       synthetic_append=True,history=DeltaTable(str(table_path)).history())
    dump_json(directory/"evidence_delta.json",result)
    dump_json(root/"evidence"/"delta_latest.json",result)
    print("Delta PASS:",checks)
    return result

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--root",type=Path,default=Path(__file__).resolve().parents[1])
    p.add_argument("--sample-size",type=int,default=1000)
    a=p.parse_args();run(a.root,a.sample_size)
if __name__=="__main__":main()
