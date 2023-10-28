import tempfile
import unittest
import duckdb as d
import parser
import datetime
from datetime import tzinfo
from db.arrow import DB
from call_tracker import CallTracker

class TestArrow(unittest.TestCase):
    def test_lobs(self):
        """ Checks that we have correct number of rows in the parquet file."""
        with tempfile.TemporaryDirectory() as db_dir:
            dbs = DB(db_dir, 'unittest')
            tracker = CallTracker(dbs)
            parser.process_file(tracker, 'tests/traces/lobs.trc')
            tracker.flush()

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops like 'LOB%';")
            self.assertEqual(res.fetchone()[0], 14)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops = 'WAIT';")
            self.assertEqual(res.fetchone()[0], 26)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*');")
            self.assertEqual(res.fetchone()[0], 58)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ts is null;")
            self.assertEqual(res.fetchone()[0], 11)

            res = d.sql(f"select ts from read_parquet('{db_dir}/*') where ops = 'STAR' "
                            +"and event_name = 'CLIENT DRIVER';")
            # Time zones are mangled, Duckdb returns
            # 2023-08-02T18:08:39.807701, which should be 2023-08-02T16:08:39.807701+02:00
            self.assertEqual(res.fetchone()[0], datetime.datetime(2023, 8, 2, 16, 8, 39, 807701))

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops = 'WAIT' "
                        "and service_name = 'xxx_stg';")
            self.assertEqual(res.fetchone()[0], 26)
if __name__ == '__main__':
    unittest.main()
