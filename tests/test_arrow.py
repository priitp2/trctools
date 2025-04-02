from os import path
import tempfile
import unittest
import datetime
import duckdb as d
from backend.arrow import Backend, PARQUET_SCHEMA_VERSION
from call_tracker import CallTracker
import trcparser

class TestArrow(unittest.TestCase):
    def test_lobs(self):
        """ Checks that we have correct number of rows in the parquet file."""
        with tempfile.TemporaryDirectory() as db_dir:
            dbs = Backend(db_dir, 'unittest')
            tracker = CallTracker(dbs)
            trcparser.process_file(tracker, 'tests/traces/lobs.trc')
            tracker.flush()

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops like 'LOB%';")
            self.assertEqual(res.fetchone()[0], 14)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops = 'WAIT';")
            self.assertEqual(res.fetchone()[0], 26)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*');")
            self.assertEqual(res.fetchone()[0], 59)

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ts is null;")
            self.assertEqual(res.fetchone()[0], 12)

            res = d.sql(f"select ts from read_parquet('{db_dir}/*') where ops = 'STAR' "
                            +"and event_name = 'CLIENT DRIVER';")
            # Time zones are mangled, Duckdb returns
            # 2023-08-02T18:08:39.807701, which should be 2023-08-02T16:08:39.807701+02:00
            self.assertEqual(res.fetchone()[0], datetime.datetime(2023, 8, 2, 16, 8, 39, 807701))

            res = d.sql(f"select count(*) from read_parquet('{db_dir}/*') where ops = 'WAIT' "
                        +"and service_name = 'xxx_stg';")
            self.assertEqual(res.fetchone()[0], 26)
    def test_schema_version(self):
        """Checks if generated parquet file has a schema version record"""
        with tempfile.TemporaryDirectory() as db_dir:
            dbs = Backend(db_dir, 'unittest')
            tracker = CallTracker(dbs)
            trcparser.process_file(tracker, 'tests/traces/lobs.trc')
            tracker.flush()

            res = d.sql(f"select event_raw from read_parquet('{db_dir}/*') "
                        +"where ops = 'HEADER' and event_name = 'PARQUET_SCHEMA';")
            self.assertEqual(res.fetchone()[0], PARQUET_SCHEMA_VERSION)

    def test_make_set_fs(self):
        fstype = 'local'
        with tempfile.TemporaryDirectory() as db_dir:
            tmpdir = f'{db_dir}/tmpdir'
            dbs = Backend(tmpdir, 'unittest')
            self.assertTrue(path.exists(tmpdir))

            with self.assertRaises(ValueError):
                dbs.set_fs('unknown-file-system')

        with self.assertRaises(TypeError):
            dbs.set_fs('subtree')
        with self.assertRaises(TypeError):
            dbs.set_fs('hadoop')

if __name__ == '__main__':
    unittest.main()
