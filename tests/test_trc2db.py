import tempfile
import unittest
import datetime
import duckdb as d
from backend.arrow import Backend, PARQUET_SCHEMA_VERSION
from call_tracker import CallTracker
import trcparser
import trc2db

class TestTrc2db(unittest.TestCase):
    def test_stuff(self):
        pass
if __name__ == '__main__':
    unittest.main()
