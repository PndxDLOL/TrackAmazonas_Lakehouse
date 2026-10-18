"""Pruebas de comportamiento: idempotencia, rechazos, SCD temporal y publicación."""
import tempfile
from pathlib import Path
import unittest
import pandas as pd
from main import run_pipeline
from scripts.common import load_json,open_gold
from scripts.transform_gold import dimension_vessels

class PipelineTests(unittest.TestCase):
    def test_end_to_end_and_idempotence(self):
        with tempfile.TemporaryDirectory() as d:
            first=run_pipeline(d,1600,42,repeats=3)
            current1=load_json(Path(d)/"lakehouse"/"current.json")
            second=run_pipeline(d,1600,42,repeats=3)
            current2=load_json(Path(d)/"lakehouse"/"current.json")
            self.assertEqual(first["batch_id"],second["batch_id"])
            self.assertEqual(current1["files"],current2["files"])
            self.assertEqual(first["validation"],second["validation"])
            self.assertTrue(second["reused"])
            self.assertEqual(first["quality"]["bronze_rows"],1618)
            self.assertEqual(first["quality"]["silver_rows"],1589)
            self.assertEqual(first["quality"]["reasons"]["IDENTITY_CONFLICT"],4)
            self.assertEqual(first["quality"]["reasons"]["DUPLICATE_RETRY"],16)
            self.assertTrue(all(x["equal_results"] for x in first["benchmark"]["comparisons"]))
            self.assertEqual(len(first["queries"]),8)
            self.assertTrue(first["validation"]["hourly_count_reconciled"])
            report=Path(d)/"evidence"/first["execution_id"]/"reporte.html"
            self.assertTrue(report.exists())
            self.assertNotIn("setInterval",report.read_text(encoding="utf-8"))
            self.assertEqual(first["validation"]["scd_changed_vessels"],80)
            con,_,_=open_gold(d)
            q=con.execute("""SELECT v.operator_name,f.event_time,v.valid_from,v.valid_to
                FROM fact_telemetria f JOIN dim_embarcacion v USING(vessel_sk)
                WHERE v.vessel_id='TA-001' ORDER BY f.event_time""").fetchall()
            self.assertEqual([x[0] for x in q],["Operador B"]) # primer evento inválido, segunda muestra posterior.
            con.close()
    def test_scd_boundary_and_stable_keys(self):
        h=pd.DataFrame([
            ["TA-001","2026-01-01T00:00:00Z","A","REGULAR"],
            ["TA-001","2026-02-15T00:00:00Z","B","PRIORITARIA"]],
            columns=["vessel_id","effective_from","operator_name","service_class"])
        d=dimension_vessels(h)
        self.assertEqual(len(d),2)
        self.assertEqual(d.iloc[0].valid_to,d.iloc[1].valid_from)
        self.assertEqual(d.is_current.tolist(),[False,True])
        self.assertEqual(d.vessel_sk.tolist(),dimension_vessels(h.iloc[::-1]).vessel_sk.tolist())
        from scripts.common import connect
        c=connect();c.register("v",d)
        for date,expected in [("2026-02-14 23:59:59","A"),("2026-02-15 00:00:00","B")]:
            rows=c.execute("""SELECT operator_name FROM v WHERE ?::TIMESTAMP>=valid_from
              AND (valid_to IS NULL OR ?::TIMESTAMP<valid_to)""",[date,date]).fetchall()
            self.assertEqual(rows,[(expected,)])
        c.close()
    def test_ambiguous_scd_is_rejected(self):
        h=pd.DataFrame([
            ["TA-001","2026-01-01T00:00:00Z","A","X"],
            ["TA-001","2026-01-01T00:00:00Z","B","Y"]],
            columns=["vessel_id","effective_from","operator_name","service_class"])
        with self.assertRaises(ValueError):dimension_vessels(h)
    def test_lock_preserves_current(self):
        with tempfile.TemporaryDirectory() as d:
            sentinel=Path(d)/"lakehouse"/"current.json";sentinel.parent.mkdir()
            sentinel.write_text('{"sentinel":true}',encoding="utf-8")
            (Path(d)/".pipeline.lock").write_text("active",encoding="utf-8")
            with self.assertRaises(FileExistsError):run_pipeline(d,800,repeats=3)
            self.assertEqual(sentinel.read_text(), '{"sentinel":true}')
if __name__=="__main__":unittest.main()
