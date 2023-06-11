import unittest
import util
from cursor_tracker import CursorTracker
from test_db import DB

cursor1 = '#123223'
sql_ids = []
class TestUtil(unittest.TestCase):
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

        db = DB()
        tracker = CursorTracker(db)
        util.process_file(tracker, 'tests/simple_trace.trc', sql_ids)

        # There is special statement for cursor #0, so len == 2
        self.assertEqual(len(tracker.statements), 2)
        self.assertEqual(len(tracker.cursors), 2)

        self.assertEqual(len(db.batches), 11)
        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(db.batches)

        self.assertEqual(cpu, db_cpu)
        self.assertEqual(elapsed, db_elapsed)
        self.assertEqual(ela_nowait, db_nowait)

        self.assertEqual(self.get_count(db.batches, 3, 'WAIT'), waits)
        self.assertEqual(self.get_count(db.batches, 3, 'FETCH'), fetches)
    def test_process_file_simple_2x(self):
        # Calculated from the trace file
        # From 1st exec
        cpu = 1105
        # From 2nd exec
        elapsed = 3710
        ela_nowait = 1744
        waits = 12
        fetches = 4

        db = DB()
        tracker = CursorTracker(db)
        util.process_file(tracker, 'tests/simple_trace_2x.trc', sql_ids)

        # There is special statement for cursor #0, so len == 2
        self.assertEqual(len(tracker.statements), 2)
        self.assertEqual(len(tracker.cursors), 2)

        self.assertEqual(len(db.batches), 23)
        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(db.batches)
        self.assertEqual(cpu, db_cpu)
        self.assertEqual(elapsed, db_elapsed)
        self.assertEqual(ela_nowait, db_nowait)

        self.assertEqual(self.get_count(db.batches, 3, 'WAIT'), waits)
        self.assertEqual(self.get_count(db.batches, 3, 'FETCH'), fetches)
    def test_process_file_missing_parse(self):
        # Calculated from the trace file
        # From 1st exec
        cpu = 553
        # From 2nd exec
        elapsed = 2357
        ela_diff = 1637
        ela_nowait = 1146

        db = DB()
        tracker = CursorTracker(db)
        util.process_file(tracker, 'tests/simple_trace_missing_parse.trc', sql_ids)

        # There is special statement for cursor #0, so len == 2
        # FIXME: decide if we should fix the sql_id for dummy1 or not
        self.assertEqual(len(tracker.statements), 3)
        self.assertEqual(len(tracker.cursors), 2)

        self.assertEqual(len(db.batches), 23)

        # First execution of #140641987987624 gets sql_id dummy1, b/c of missing PIC
        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(db.batches, 1, 'dummy1')
        self.assertEqual(cpu, db_cpu)

        (db_cpu, db_elapsed, db_nowait) = self.get_aggregates(db.batches, 1, 'atxg62s17nkj4')
        self.assertEqual(elapsed, db_elapsed)
        self.assertEqual(ela_nowait, db_nowait)

    def test_process_file_3_statements_1_cursor(self):
        db = DB()
        tracker = CursorTracker(db)
        util.process_file(tracker, 'tests/two_statements_one_cursor.trc', sql_ids)

        s = tracker.statements['cdgn9f8spbxnt']
        self.assertEqual(s.fetches, 1)
        self.assertEqual(self.get_count(db.batches, 1, 'cdgn9f8spbxnt'), 12)
        # Counts by sql_id and exec_id should match
        self.assertEqual(self.get_count(db.batches, 0, 1), self.get_count(db.batches, 1, 'cdgn9f8spbxnt'))

        s = tracker.statements['atxg62s17nkj4']
        self.assertEqual(s.fetches, 2)
        self.assertEqual(self.get_count(db.batches, 1, 'atxg62s17nkj4'), 11)
        self.assertEqual(self.get_count(db.batches, 0, 2), self.get_count(db.batches, 1, 'atxg62s17nkj4'))

        s = tracker.statements['6ssxu7vjxb51a']
        self.assertEqual(s.fetches, 11)
        self.assertEqual(self.get_count(db.batches, 1, '6ssxu7vjxb51a'), 40)
        self.assertEqual(self.get_count(db.batches, 0, 3), self.get_count(db.batches, 1, '6ssxu7vjxb51a'))

        self.assertEqual(len(tracker.statements), 4)

        # Cursor #139623166535832 gets overwritten by every execution
        self.assertEqual(len(tracker.cursors), 2)
    def test_mixed_execs(self):
        # FIXME: is this test needed?
        """There are stray wait events for cursor #140386304541280, after '*** <date>'.
           Suspicion is that these are stray events and shouldn't be experienced by the db client.
           Add them as a different exec_id.
           """
        db = DB()
        tracker = CursorTracker(db)
        util.process_file(tracker, 'tests/mixed_execs.trc', sql_ids)
        # 3 EXEC calls + 1 dummy for the stray WAITs
        self.assertEqual(tracker.statements['6v48b7j2tc4a0'].execs, 3)

if __name__ == '__main__':
    unittest.main()
