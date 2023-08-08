import tempfile
import unittest
import duckdb as d
import filer
from db.arrow import DB
from call_tracker import CallTracker

class TestArrow(unittest.TestCase):
    def test_lobs(self):
        """ Checks that we have correct number of rows in the parquet file."""
        with tempfile.TemporaryDirectory() as db_dir:
            dbs = DB(db_dir, 'unittest')
            tracker = CallTracker(dbs)
            filer.process_file(tracker, 'tests/traces/lobs.trc')
            tracker.flush()

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops like 'LOB%';")
            self.assertEqual(res.fetchone()[0], 14)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops = 'WAIT';")
            self.assertEqual(res.fetchone()[0], 26)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*');")
            self.assertEqual(res.fetchone()[0], 58)
if __name__ == '__main__':
    unittest.main()
