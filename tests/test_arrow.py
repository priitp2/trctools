import tempfile
import unittest
import duckdb as d
from filer import Filer
from db.arrow import DB
from call_tracker import CallTracker

class TestArrow(unittest.TestCase):
    def test_lobs(self):
        with tempfile.TemporaryDirectory() as db_dir:
            dbs = DB(db_dir, 'unittest')
            tracker = CallTracker(dbs)
            filer = Filer()
            filer.process_file(tracker, 'tests/traces/lobs.trc')
            tracker.flush()

            res = d.sql("select ops, event_name, event_raw, file_name, line from "
                        + f"read_parquet('{db_dir}/*') order by line;")
            print(res)
            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops like 'LOB%';")
            self.assertEqual(res.fetchone()[0], 14)
            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops = 'WAIT';")
            self.assertEqual(res.fetchone()[0], 26)
            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*');")
            self.assertEqual(res.fetchone()[0], 66)
if __name__ == '__main__':
    unittest.main()