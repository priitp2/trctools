import unittest
import parser
from call_tracker import CallTracker
from tests.mock_db import DB

cursor1 = '#123223'
class TestParser(unittest.TestCase):
    def setUp(self):
        dbs = DB()
        self.tracker = CallTracker(dbs)
    def get_aggregates(self, batches, pos=None, op=None):
        cpu = 0
        elapsed = 0
        nowait = 0
        for s in batches:
            if pos and s[pos] != op:
                continue
            if s[4]:
                cpu += s[4]
            if s[5]:
                elapsed += s[5]
            if s[5] and s[3] != 'WAIT':
                nowait += s[5]
        return (cpu, elapsed, nowait)
    def get_count(self, batches, pos, op):
        c = 0
        for s in batches:
            if s[pos] == op:
                c += 1
        return c
    def test_process_file_simple(self):
        # Calculated from the trace file
        cpu = 553
        elapsed = 1353
        ela_diff = 1221
        ela_nowait = 598
        waits = 5
        fetches = 2
        stars = 9
        headers = 11
        binds = 1
        pics = 1
        sql_id = 'atxg62s17nkj4'

        lines = parser.process_file(self.tracker, 'tests/traces/simple_trace.trc')
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

        lines = parser.process_file(self.tracker, 'tests/traces/simple_trace_2x.trc')
        self.assertEqual(lines, 59)

        self.assertEqual(len(self.tracker.cursors), 1)

        self.assertEqual(len(self.tracker.db.batches), 39)
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

        lines = parser.process_file(self.tracker, 'tests/traces/simple_trace_missing_parse.trc')
        self.assertEqual(lines, 61)

        # There is special statement for cursor #0, so len == 2
        # FIXME: decide if we should fix the sql_id for dummy1 or not
        self.assertEqual(len(self.tracker.cursors), 1)

        self.assertEqual(len(self.tracker.db.batches), 39)

        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(self.tracker.db.batches, 2, '#140641987987624')
        self.assertEqual(elapsed_both, db_elapsed)

        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(self.tracker.db.batches, 1, 'atxg62s17nkj4')
        self.assertEqual(cpu, db_cpu)

        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'XCTEND'), 2)

    def test_process_file_3_statements_1_cursor(self):
        lines = parser.process_file(self.tracker, 'tests/traces/two_statements_one_cursor.trc')
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

    @unittest.skip("Decide if this test is needed or not")
    def test_mixed_execs(self):
        # FIXME: is this test needed?
        """There are stray wait events for cursor #140386304541280, after '*** <date>'.
           Suspicion is that these are stray events and shouldn't be experienced by the db client.
           Add them as a different exec_id.
           """
        lines = parser.process_file(tracker, 'tests/traces/mixed_execs.trc')
        self.assertEqual(lines, 107)
        # 3 EXEC calls + 1 dummy for the stray WAITs
        self.assertEqual(self.tracker.statements['6v48b7j2tc4a0'].execs, 3)

    def test_stray_close(self):
        # PARSE/PIC happened before start of the trace. Just add the call to the database w/o sql_id
        lines = parser.process_file(self.tracker, 'tests/traces/stray_close.trc')
        self.tracker.flush()
        self.assertEqual(lines, 16)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'CLOSE'), 1)

    def test_lobs(self):
        lines = parser.process_file(self.tracker, 'tests/traces/lobs.trc')
        self.tracker.flush()
        self.assertEqual(lines, 65)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'LOBWRITE'), 5)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'LOBTMPCREATE'), 5)
        self.assertEqual(self.get_count(self.tracker.db.batches, 3, 'LOBPGSIZE'), 4)
if __name__ == '__main__':
    unittest.main()