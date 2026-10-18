"""Rutas por snapshot y utilidades compartidas; no hay escaneo global de Bronze."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import os
import duckdb

VERSION = "2.0.0"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def dump_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))

def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    dump_json(tmp, value)
    os.replace(tmp, path)

def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def stable_key(*values):
    # 60 bits: positivo y representable en INTEGER de 64 bits.
    return int(hashlib.sha256("|".join(map(str, values)).encode()).hexdigest()[:15], 16)

def literal(path):
    return "'" + Path(path).resolve().as_posix().replace("'", "''") + "'"

def connect():
    con = duckdb.connect()
    con.execute("SET threads=1")  # Benchmark controlado, un hilo en ambos motores.
    con.execute("SET TimeZone='UTC'")
    return con

def export_query(con, query, path, partition=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    options = "FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 65536"
    if partition:
        options += ", PARTITION_BY (" + ",".join(partition) + ")"
    con.execute(f"COPY ({query}) TO {literal(path)} ({options})")

def parquet_view(con, name, path, partition=False):
    glob = str(path) + "/**/*.parquet" if partition else str(path)
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({literal(glob)}, hive_partitioning={str(partition).lower()})")

def open_gold(root, manifest=None):
    root = Path(root)
    manifest = manifest or load_json(root/"lakehouse"/"current.json")
    gold = root/manifest["gold"]
    con = connect()
    parquet_view(con, "fact_telemetria", gold/"fact_telemetria", True)
    for table in ("dim_tiempo","dim_embarcacion","dim_rio","dim_ruta","agg_rio_mes","agg_embarcacion_hora"):
        parquet_view(con, table, gold/(table+".parquet"))
    return con, gold, manifest
