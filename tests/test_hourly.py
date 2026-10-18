import unittest
from scripts.common import connect
class HourlyTest(unittest.TestCase):
    def test_weighted_mean_with_null_and_unequal_groups(self):
        con=connect()
        # La media correcta de 10,20,40 es 23.33; el promedio de medias es 27.5.
        con.execute("CREATE TABLE f(hour_utc INTEGER,speed_kmh DOUBLE)")
        con.execute("INSERT INTO f VALUES (1,10),(1,20),(1,NULL),(2,40)")
        direct=con.execute("SELECT avg(speed_kmh),count(*) FROM f").fetchone()
        weighted=con.execute("""WITH h AS (SELECT hour_utc,sum(speed_kmh) s,count(speed_kmh) n,count(*) e
          FROM f GROUP BY hour_utc) SELECT sum(s)/sum(n),sum(e) FROM h""").fetchone()
        wrong=con.execute("SELECT avg(a) FROM (SELECT avg(speed_kmh) a FROM f GROUP BY hour_utc)").fetchone()[0]
        self.assertEqual(direct,weighted)
        self.assertNotEqual(direct[0],wrong)
        con.close()
if __name__=="__main__":unittest.main()
