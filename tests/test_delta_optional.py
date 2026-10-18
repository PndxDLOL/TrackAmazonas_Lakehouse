import importlib.util
import os
import tempfile
import unittest
from main import run_pipeline
from scripts.delta_demo import run

@unittest.skipUnless(os.environ.get('RUN_DELTA')=='1' and importlib.util.find_spec('deltalake'),
                     'Profundización opcional: instalar Delta y definir RUN_DELTA=1')
class DeltaOptionalTest(unittest.TestCase):
    def test_two_versions_and_original_snapshot_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            run_pipeline(d,1600,42,repeats=3)
            result=run(d,20)
            self.assertEqual(result['checks']['version0_rows'],20)
            self.assertEqual(result['checks']['version1_rows'],21)
            self.assertTrue(result['checks']['original_publication_unchanged'])
            self.assertEqual(result['checks']['version1_demo_events'],1)
            self.assertEqual(result['checks']['log_files'],2)
if __name__=='__main__':unittest.main()
