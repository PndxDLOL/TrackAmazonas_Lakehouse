"""Ejecuta las seis preguntas del dominio y conserva resultados CSV."""
from pathlib import Path
import argparse
import time
from .common import open_gold, dump_json

def run(root, snapshot, evidence):
    con,gold,_=open_gold(root,snapshot)
    result={}
    directory=Path(evidence)/"queries";directory.mkdir(parents=True,exist_ok=True)
    sqlroot=Path(__file__).resolve().parents[1]/"queries"
    for file in sorted(sqlroot.glob("*.sql")):
        sql=file.read_text(encoding="utf-8")
        started=time.perf_counter()
        df=con.execute(sql).fetchdf()
        elapsed=(time.perf_counter()-started)*1000
        df.to_csv(directory/(file.stem+".csv"),index=False)
        result[file.stem]=dict(rows=len(df),elapsed_ms=round(elapsed,3))
        print(f"  {file.stem}: {len(df)} filas ({elapsed:.2f} ms)")
        print(df.head(3).to_string(index=False))
        (directory/(file.stem+"_explain.txt")).write_text(
            "\n".join(str(x[-1]) for x in con.execute("EXPLAIN ANALYZE "+sql).fetchall()),encoding="utf-8")
    dump_json(directory/"summary.json",result);con.close()
    return result

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--query",type=int,choices=range(1,9),default=1)
    parser.add_argument("--root",type=Path,default=Path(__file__).resolve().parents[1])
    args=parser.parse_args()
    con,_,_=open_gold(args.root)
    path=next((Path(__file__).resolve().parents[1]/"queries").glob(f"{args.query:02d}_*.sql"))
    print(con.execute(path.read_text(encoding="utf-8")).fetchdf().to_string(index=False))
    con.close()
if __name__=="__main__":main()
