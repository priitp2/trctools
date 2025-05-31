import unittest
import trcparser
from call_tracker import CallTracker
from tests.mock_backend import Backend
from datetime import timedelta
from zoneinfo import ZoneInfo

class TestParser(unittest.TestCase):
    def setUp(self):
        dbs = Backend()
        self.tracker = CallTracker(dbs)
    def get_aggregates(self, batches, pos=None, ops=None):
        cpu = 0
        elapsed = 0
        nowait = 0
        for item in batches:
            if pos and item[pos] != ops:
                continue
            if item[4]:
                cpu += item[4]
            if item[5]:
                elapsed += item[5]
            if item[5] and item[3] != 'WAIT':
                nowait += item[5]
        return (cpu, elapsed, nowait)
    def get_count(self, batches, pos, ops):
        count = 0
        for item in batches:
            if item[pos] == ops:
                count += 1
        return count
    def test_process_file_simple(self):
        # Calculated from the trace file
        cpu = 553
        elapsed = 1353
        ela_nowait = 598
        waits = 5
        fetches = 2
        stars = 9
        headers = 11
        binds = 1
        pics = 1
        sql_id = 'atxg62s17nkj4'

        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/simple_trace.trc')
        self.assertEqual(lines, 51)

        # There is special statement for cursor #0, so len == 2
        self.assertEqual(len(self.tracker.cursors), 1)

        self.assertEqual(len(self.tracker.db.batches), 34)
        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(self.tracker.db.batches)

        self.assertEqual(cpu, db_cpu)
        self.assertEqual(elapsed, db_elapsed)
        self.assertEqual(ela_nowait, db_nowait)

        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'WAIT'), waits)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'FETCH'), fetches)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'STAR'), stars)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'HEADER'), headers)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'BINDS'), binds)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'PIC'), pics)

        self.assertTrue(sql_id in self.tracker.cursors.values())
    def test_process_file_simple_2x(self):
        # Calculated from the trace file
        # From 1st exec
        cpu = 1105
        # From 2nd exec
        elapsed = 3710
        ela_nowait = 1744
        waits = 12
        fetches = 4

        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/simple_trace_2x.trc')
        self.assertEqual(lines, 69)

        self.assertEqual(len(self.tracker.cursors), 1)

        self.assertEqual(len(self.tracker.db.batches), 48)
        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(self.tracker.db.batches)
        self.assertEqual(cpu, db_cpu)
        self.assertEqual(elapsed, db_elapsed)
        self.assertEqual(ela_nowait, db_nowait)

        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'WAIT'), waits)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'FETCH'), fetches)
    def test_process_file_missing_parse(self):
        """Test the case when tracing starts after the PIC. Trace file contains two executions
            for cursor #140641987987624. PIC comes with the second execution."""
        # Calculated from the trace file
        # From 2nd exec
        cpu = 552
        # Elapsed is for both executions
        elapsed_both = 3710

        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/simple_trace_missing_parse.trc')
        self.assertEqual(lines, 71)

        self.assertEqual(len(self.tracker.cursors), 1)

        self.assertEqual(len(self.tracker.db.batches), 48)

        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(self.tracker.db.batches, 2,
                '#140641987987624')
        self.assertEqual(elapsed_both, db_elapsed)

        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(self.tracker.db.batches, 1,
                'atxg62s17nkj4')
        self.assertEqual(cpu, db_cpu)

        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'XCTEND'), 2)

    def test_process_file_2_statements_1_cursor(self):
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/two_statements_one_cursor.trc')
        self.tracker.flush()
        self.assertEqual(lines, 119)

        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'FETCH'), 14)
        self.assertEqual(self.get_count(self.tracker.db.batches, 1, 'cdgn9f8spbxnt'), 13)

        self.assertEqual(self.get_count(self.tracker.db.batches, 1, 'atxg62s17nkj4'), 13)

        self.assertEqual(self.get_count(self.tracker.db.batches, 1, '6ssxu7vjxb51a'), 42)
        self.assertEqual(self.get_count(self.tracker.db.batches, 1, 'dummy1'), 0)

        # Cursor #139623166535832 gets overwritten by every execution
        self.assertEqual(len(self.tracker.cursors), 1)

        for batch in self.tracker.db.batches:
            if batch[3] == 'BINDS' and len(batch[17]) < 5:
                self.fail(f'BINDS not set, wait_raw is {batch[17]}')

    def test_stray_close(self):
        # PARSE/PIC happened before start of the trace. Just add the call to the database w/o sql_id
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/stray_close.trc')
        self.tracker.flush()
        self.assertEqual(lines, 26)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'CLOSE'), 1)

    def test_lobs(self):
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/lobs.trc')
        self.tracker.flush()
        self.assertEqual(lines, 65)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'LOBWRITE'), 5)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'LOBTMPCREATE'), 5)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'LOBPGSIZE'), 4)

    def test_error(self):
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/error.trc')
        self.tracker.flush()
        self.assertEqual(lines, 35)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'ERROR'), 2)

    def test_get_timestamp(self):
        instr1 = '2023-05-19T03:28:00.339309'
        ts1 = trcparser.get_timestamp(instr1)
        instr2 = '2023-05-19T05:28:00.339309+02:00'
        ts2 = trcparser.get_timestamp(instr2)

        self.assertEqual(ts1.astimezone(tz=ZoneInfo('UTC')), ts2.astimezone(tz=ZoneInfo('UTC')))

    def test_broken_date(self):
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/no_timezone.trc')
        self.tracker.flush()
        self.assertEqual(lines, 51)

    def test_parse_error(self):
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/parse_error.trc')
        self.tracker.flush()
        self.assertEqual(lines, 36)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'PARSE ERROR'), 1)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'CLOSE'), 1)

    def test_broken_tracefile(self):
        '''Test for broken tracefile. There's a missing CRLF in the file and we expect parser
            not to throw an exception'''
        (lines, err) = trcparser.process_file(self.tracker, 'tests/traces/broken_trace.trc')
        self.assertEqual(err, 4)
    def test_init_fmeta(self):
        with self.assertRaises(ValueError):
            trcparser.init_fmeta('')

if __name__ == '__main__':
    unittest.main()
