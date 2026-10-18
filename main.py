"""Ejecutar: python main.py. Una publicación visible solo tras completar todas las etapas."""
import argparse
from pathlib import Path
import os
import sys
import time
import uuid
import traceback
from scripts.common import VERSION, dump_json, load_json, atomic_json, file_hash, now_iso
from scripts.source import seed_academic, import_v3
from scripts.extract import run as extract
from scripts.transform_silver import run as silver_step
from scripts.transform_gold import run as gold_step

def run_pipeline(root, rows=50000, seed=42, input_json=None, repeats=7):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    lock=root/".pipeline.lock"
    fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
    os.write(fd,str(os.getpid()).encode());os.close(fd)
    execution=uuid.uuid4().hex[:12]
    evidence=root/"evidence"/execution;evidence.mkdir(parents=True,exist_ok=True)
    started=time.perf_counter()
    try:
        source=import_v3(root,input_json) if input_json else seed_academic(root,rows,seed)
        bronze=extract(root,source)
        cache=root/"lakehouse"/"builds"/(bronze["batch_id"]+"_"+VERSION+".json")
        reused=False
        if cache.exists():
            snapshot=load_json(cache)
            for name,digest in snapshot["files"].items():
                if file_hash(root/name)!=digest: raise ValueError("Snapshot publicado alterado: "+name)
            reused=True
            print("  Silver/Gold reutilizados: mismos hechos, claves y archivos")
        else:
            build=bronze["batch_id"]+"_"+execution
            silver=root/"lakehouse"/"silver"/build
            gold=root/"lakehouse"/"gold"/build
            quality=silver_step(root,bronze,silver)
            validation=gold_step(root,bronze,silver,gold)
            hashes={str(p.relative_to(root)):file_hash(p) for directory in (silver,gold)
                      for p in directory.rglob("*") if p.is_file()}
            snapshot=dict(batch_id=bronze["batch_id"],transform_version=VERSION,
                          source=str(source.relative_to(root)),bronze=bronze["bronze"],
                          silver=str(silver.relative_to(root)),gold=str(gold.relative_to(root)),
                          quality=quality,validation=validation,files=hashes,
                          source_metadata=bronze["source_metadata"],published_at=now_iso())
            dump_json(cache,snapshot)
        # Consultas, igualdad y benchmarks se validan ANTES de mover el puntero current.
        from scripts.analyze import run as analyze
        from scripts.benchmark import run as benchmark
        results=analyze(root,snapshot,evidence)
        bench=benchmark(root,snapshot,evidence,repeats)
        import shutil
        shutil.copy2(root/snapshot["gold"]/"scd_demo.csv",evidence/"scd_demo.csv")
        catalog={"owner":"Equipo 4 TrackAmazonas","classification":"Telemetría sintética o exportada; ubicaciones sensibles en producción",
            "snapshot":snapshot["batch_id"],"grain":snapshot["validation"]["fact_grain"],
            "layers":{name:snapshot[name] for name in ("bronze","silver","gold")},
            "lineage":["SQLite o Mongo V3 exportado","Bronze","Silver","Gold","DuckDB"],
            "event_time":"Tiempo del GPS; gobierna partición, SCD y agrupaciones",
            "ingestion_time":"Tiempo de recepción central; incluye posible atraso offline",
            "retention":"Local sin borrado automático; política productiva por definir",
            "delta_or_iceberg":False,"concurrent_writers_supported":False}
        dump_json(evidence/"catalog.json",catalog)
        finished=dict(execution_id=execution,batch_id=snapshot["batch_id"],reused=reused,
            duration_s=round(time.perf_counter()-started,3),result="PASS",quality=snapshot["quality"],
            validation=snapshot["validation"],benchmark=bench,source_metadata=snapshot["source_metadata"],
            gold=snapshot["gold"],silver=snapshot["silver"],bronze=snapshot["bronze"],
            snapshot_hashes=snapshot["files"],queries=list(results),created_at=now_iso())
        dump_json(evidence/"run.json",finished)
        from scripts.dashboard import run as dashboard
        dashboard(snapshot,finished,evidence)
        snapshot["evidence"]=str(evidence.relative_to(root))
        atomic_json(root/"lakehouse"/"current.json",snapshot)
        atomic_json(root/"evidence"/"latest.json",finished)
        print(f"  PASS en {finished['duration_s']} s | evidencia: {evidence.relative_to(root)}")
        return finished
    except Exception as exc:
        dump_json(evidence/"failure.json",dict(result="FAIL",error=str(exc),traceback=traceback.format_exc()))
        raise
    finally:
        lock.unlink(missing_ok=True)

def main():
    parser=argparse.ArgumentParser(description="Taller 3 TrackAmazonas")
    parser.add_argument("--rows",type=int,default=50000)
    parser.add_argument("--seed",type=int,default=42)
    parser.add_argument("--root",type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument("--input-v3",type=Path,help="JSONL exportado de MongoDB; crea espejo SQLite fiel")
    parser.add_argument("--repeats",type=int,default=7)
    args=parser.parse_args()
    if args.repeats<3: parser.error("El benchmark necesita al menos tres repeticiones")
    try: run_pipeline(args.root,args.rows,args.seed,args.input_v3,args.repeats)
    except Exception as exc:
        print("ERROR:",exc,file=sys.stderr);return 1
    return 0
if __name__=="__main__":
    sys.exit(main())
