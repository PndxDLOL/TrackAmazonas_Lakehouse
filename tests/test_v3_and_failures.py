import json
from pathlib import Path
import tempfile
import unittest
from main import run_pipeline
from scripts.common import load_json,open_gold

class SourceAndPublicationTests(unittest.TestCase):
    def test_v3_export_preserves_raw_and_does_not_invent_scd(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'export.jsonl'
            events=[]
            for seq in (1,2):
                events.append(dict(event_id=f'run123:TA-001:E1:{seq:07d}',vessel_id='TA-001',
                    event_time=f'2026-09-29T12:00:0{seq}+00:00',
                    ingestion_time=f'2026-09-29T12:02:0{seq}+00:00',river='Amazonas',
                    route_id='R-Amazonas-sintetica',location={'type':'Point','coordinates':[-69.9,-4.2]},
                    route_reference_lat=-4.2,speed_kmh=18.0,device_epoch=1,sequence_number=seq,
                    original_extra_field='preservar'))
            text='\n'.join(json.dumps(x) for x in events)+'\n'
            p.write_text(text,encoding='utf-8')
            result=run_pipeline(folder,input_json=p,repeats=3)
            self.assertEqual(result['validation']['fact_rows'],2)
            self.assertEqual(result['validation']['scd_changed_vessels'],0)
            import pandas as pd
            bronze=pd.read_parquet(Path(folder)/result['bronze']/'position_events.parquet')
            self.assertEqual(bronze.raw_extended_json.tolist(),text.splitlines())
            c,_,_=open_gold(folder)
            self.assertEqual(c.execute('SELECT DISTINCT operator_name FROM dim_embarcacion').fetchall(),[('NO_DOCUMENTADO',)])
            self.assertEqual(c.execute('SELECT DISTINCT ingestion_delay_s FROM fact_telemetria').fetchall(),[(120.0,)])
            c.close()
    def test_failed_pipeline_preserves_last_publication(self):
        with tempfile.TemporaryDirectory() as folder:
            current=Path(folder)/'lakehouse'/'current.json';current.parent.mkdir()
            current.write_text('{"previous":"available"}',encoding='utf-8')
            with self.assertRaises(ValueError):run_pipeline(folder,rows=10,repeats=3)
            self.assertEqual(current.read_text(),'{"previous":"available"}')
            self.assertFalse((Path(folder)/'.pipeline.lock').exists())
            self.assertEqual(len(list((Path(folder)/'evidence').glob('*/failure.json'))),1)

if __name__=='__main__':unittest.main()
