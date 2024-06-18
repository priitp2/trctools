from dataclasses import dataclass
import tempfile
import unittest
import duckdb as d
import trc2db

@dataclass
class DummyArgs:
    """Dummy arguments for the functions from trc2db.py"""
    db: str() = 'parquet'
    trace_files: tuple() = tuple(['tests/traces/lobs.trc'])
    dbdir: str() = 'out'
    file_prefix: str() = 'unittest'
    traceid: str() = 'CLIENT ID'
    orphans: bool() = True

class TestTrc2db(unittest.TestCase):
    """Tests for trc2db.py"""
    def test_get_backend(self):
        """Test if we can initialize a backend"""
        backend = trc2db.get_backend(DummyArgs())
        self.assertIsNotNone(backend)

        backend = trc2db.get_backend(DummyArgs(db='mongodb'))
        self.assertIsNone(backend)
    def test_process_one_file(self):
        """High level test for process_file."""
        with tempfile.TemporaryDirectory() as db_dir:
            args = DummyArgs(dbdir=db_dir)
            trc2db.process_files(args)

            pfile = f"{db_dir}/{args.file_prefix}.0"
            res = d.sql(f"select count(*) from read_parquet('{pfile}');")
            self.assertEqual(res.fetchone()[0], 59)
    def test_process_comp_file(self):
        with tempfile.TemporaryDirectory() as db_dir:
            args = DummyArgs(dbdir=db_dir, trace_files=('tests/traces/two_statements_one_cursor.trc.gz',
                'tests/traces/mixed_execs.trc.bz2', 'tests/traces/lobread.trc.xz',
                'tests/traces/lobread.trc.lzma'))
            trc2db.process_files(args)
            pfile = f"{db_dir}/{args.file_prefix}.0"
            res = d.sql(f"select count(*) from read_parquet('{pfile}');")
            self.assertEqual(res.fetchone()[0], 283)

            res = d.sql(f"select count(distinct file_name) from read_parquet('{pfile}');")
            self.assertEqual(res.fetchone()[0], 4)
if __name__ == '__main__':
    unittest.main()
