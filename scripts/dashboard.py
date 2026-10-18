"""Reporte HTML local de una corrida. Sin servidor, CDN ni refresco automático."""
from pathlib import Path
import html
import math
import pandas as pd

def esc(value):
    if value is None or (isinstance(value,float) and math.isnan(value)): return "Sin medición"
    return html.escape(str(value))

def table(headers,rows):
    return "<table><thead><tr>"+"".join("<th>"+esc(x)+"</th>" for x in headers)+"</tr></thead><tbody>"+\
           "".join("<tr>"+"".join("<td>"+esc(x)+"</td>" for x in row)+"</tr>" for row in rows)+"</tbody></table>"

def run(snapshot,finished,evidence):
    evidence=Path(evidence)
    q=finished["quality"];v=finished["validation"]
    panels=[]
    panels.append(("resumen","Resumen",
       "<p>Corrida <strong>"+esc(finished["execution_id"])+"</strong> · Snapshot "+esc(finished["batch_id"])+"</p>"+
       "<p>Perfil: "+esc(snapshot["source_metadata"]["profile"])+". "+esc(snapshot["source_metadata"].get("description","Exportación V3"))+"</p>"+
       table(["Control","Resultado"],[["Estado","PASS"],["Bronze",q["bronze_rows"]],["Silver y hechos",q["silver_rows"]],
        ["Barcos",v["vessels"]],["Versiones SCD",v["vessel_versions"]],["Filas horarias",v["hourly_rows"]]])+
       "<p>Reporte de una ejecución validada. Los intervalos sin muestras no se interpretan como pérdida demostrada.</p>"))
    panels.append(("calidad","Calidad",
       "<p>Bronze = Silver + descartes auditados. Los nulos opcionales se conservan.</p>"+
       table(["Causa","Filas"], sorted(q["reasons"].items()))+
       "<p>Velocidades sin medición: "+esc(q["missing_speed_rows"])+"</p>"))
    query_html=""
    for path in sorted((evidence/"queries").glob("*.csv")):
        df=pd.read_csv(path)
        query_html+=("<h2>"+esc(path.stem)+"</h2><p>"+esc(len(df))+" filas. Vista previa de 30; "+
            '<a href="queries/'+esc(path.name)+'">CSV completo</a>.</p>')
        query_html+=table(df.columns,df.head(30).itertuples(index=False,name=None))
    panels.append(("consultas","Consultas",query_html))
    benchmark=finished["benchmark"]["comparisons"]
    panels.append(("benchmark","Benchmark",
        "<p>Medianas de tiempos reales; ratio = izquierda / derecha. Menor a 1 significa segunda alternativa más lenta.</p>"+
        table(["Comparación","Antes ms","Después ms","Ratio","Igualdad"],[
            [x["label"],round(x["left_median_ms"],3),round(x["right_median_ms"],3),
             round(x["ratio_left_over_right"],3),x["equal_results"]] for x in benchmark])+
        "<p>SQLite y Gold usan la misma población válida. Conversión de tipos y métricas preparadas también influyen; no se mide solo el formato columnar.</p>"))
    scd=pd.read_csv(evidence/"scd_demo.csv") if (evidence/"scd_demo.csv").exists() else None
    if scd is not None:
        panels.append(("historia","Historia SCD",table(scd.columns,scd.itertuples(index=False,name=None))+
          "<p>Los eventos usan la versión vigente en event_time, con intervalos [desde, hasta). Fin vacío es versión actual.</p>"))
    css="""body{font-family:Arial,sans-serif;max-width:1240px;margin:30px auto;padding:0 22px;color:#16212b;background:#fff}
h1{font-size:28px}h2{font-size:19px;margin-top:28px}nav{display:flex;gap:8px;flex-wrap:wrap;border-bottom:1px solid #bbb;padding-bottom:12px}
button{font:inherit;padding:10px 16px;border:1px solid #aaa;background:#fff;cursor:pointer;border-radius:5px}
button[aria-selected=true]{background:#173452;color:#fff;border-color:#173452}
section{padding-top:18px;overflow-x:auto}table{border-collapse:collapse;width:100%;margin:15px 0}
th,td{border:1px solid #d9d9d9;text-align:left;padding:9px;white-space:nowrap}th{background:#eee}
tr:nth-child(even){background:#fafafa}p{line-height:1.5}input{font:inherit;padding:9px;width:min(500px,90%);margin:15px 0}
footer{border-top:1px solid #ccc;padding-top:16px;margin-top:30px;color:#555}a{color:#173452}"""
    nav="".join('<button role="tab" aria-controls="'+key+'" aria-selected="'+str(i==0).lower()+'">'+esc(label)+'</button>'
         for i,(key,label,_) in enumerate(panels))
    sections="".join('<section role="tabpanel" id="'+key+'"'+("" if i==0 else " hidden")+">"+body+"</section>"
         for i,(key,_,body) in enumerate(panels))
    js="""document.querySelectorAll('[role=tab]').forEach(b=>b.addEventListener('click',()=>{
document.querySelectorAll('[role=tab]').forEach(x=>x.setAttribute('aria-selected',String(x===b)));
document.querySelectorAll('[role=tabpanel]').forEach(x=>x.hidden=x.id!==b.getAttribute('aria-controls'));
document.querySelector('#filter').value='';document.querySelectorAll('tbody tr').forEach(r=>r.hidden=false);
}));document.querySelector('#filter').addEventListener('input',e=>{
const text=e.target.value.toLowerCase();document.querySelectorAll('section:not([hidden]) tbody tr').forEach(r=>r.hidden=!r.textContent.toLowerCase().includes(text));
});"""
    doc='<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'+\
        "<title>TrackAmazonas reporte del Lakehouse</title><style>"+css+"</style><body><h1>TrackAmazonas · Reporte del Lakehouse</h1>"+\
        "<p>Equipo 4 · Taller 3 · Evidencia de una corrida</p><nav role=\"tablist\">"+nav+"</nav>"+\
        '<label for="filter">Filtrar filas de la pestaña actual</label><br><input id="filter" placeholder="Buscar barco, río o causa">'+\
        sections+"<footer>Sin actualización automática. Ejecutar de nuevo el pipeline crea otro reporte. Los datos académicos no acreditan resultados de navegación real.</footer>"+\
        "<script>"+js+"</script></body></html>"
    (evidence/"reporte.html").write_text(doc,encoding="utf-8")
    return evidence/"reporte.html"
