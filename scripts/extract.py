"""Bronze inmutable: snapshot completo y consistente de SQLite."""
from pathlib import Path
import sqlite3
from contextlib import closing
import hashlib
import pandas as pd
from .common import dump_json, load_json, file_hash, now_iso

def run(root, source_path):
    root=Path(root)
    # Una transacción de lectura mantiene el mismo snapshot para eventos y catálogo.
    with closing(sqlite3.connect(f"file:{Path(source_path).resolve().as_posix()}?mode=ro",uri=True)) as source:
        source.execute("BEGIN")
        events=pd.read_sql_query("SELECT * FROM position_events ORDER BY record_id",source)
        history=pd.read_sql_query("SELECT * FROM vessel_history ORDER BY vessel_id,effective_from",source)
        metadata=load_metadata(source)
    h=hashlib.sha256()
    for frame in (events,history):
        h.update(pd.util.hash_pandas_object(frame,index=False).values.tobytes())
        h.update("|".join(frame.columns).encode())
    h.update(str(metadata).encode())
    batch=h.hexdigest()[:20]
    bronze=root/"lakehouse"/"bronze"/batch
    manifest=bronze/"manifest.json"
    if manifest.exists():
        info=load_json(manifest)
        for name,digest in info["files"].items():
            if file_hash(bronze/name)!=digest:
                raise ValueError("Bronze cambió después de su publicación")
        print("  Bronze reutilizado, sin sobrescribir:",batch)
        return info
    bronze.mkdir(parents=True,exist_ok=False)
    events["_source"]=metadata["profile"]
    events["_batch_id"]=batch
    events["_extracted_at"]=now_iso()
    events.to_parquet(bronze/"position_events.parquet",index=False,compression="zstd")
    history.to_parquet(bronze/"vessel_history.parquet",index=False,compression="zstd")
    info=dict(batch_id=batch,bronze=str(bronze.relative_to(root)),source=str(Path(source_path).relative_to(root)),
              rows=len(events),source_metadata=metadata,extracted_at=now_iso(),
              files={p.name:file_hash(p) for p in bronze.glob("*.parquet")})
    dump_json(manifest,info)
    print("  Bronze:",len(events),"filas; lote",batch)
    return info

def load_metadata(source):
    import json
    return json.loads(source.execute("SELECT payload FROM source_metadata").fetchone()[0])
