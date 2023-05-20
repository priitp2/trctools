import unittest
import util
from cursor_tracker import CursorTracker

cursor1 = '#123223'
sql_ids = []
class TestUtil(unittest.TestCase):
    def test_process_file_simple(self):
        # Calculated from the trace file
        cpu = 553
        elapsed = 1353
        ela_diff = 1221
        ela_nowait = 598

        tracker = CursorTracker(None)
        util.process_file(tracker, 'tests/simple_trace.trc', sql_ids)

        # There is special statement for cursor #0, so len == 2
        self.assertEqual(len(tracker.statements), 2)
        self.assertEqual(len(tracker.cursors), 2)

        s = tracker.statements['atxg62s17nkj4']
        self.assertEqual(s.fetches, 2)
        self.assertEqual(s.exec_hist_elapsed.total_count, 1)
        self.assertEqual(s.exec_hist_cpu.total_count, 1)
        self.assertEqual(s.exec_hist_elapsed.max_value, elapsed)
        self.assertEqual(s.exec_hist_cpu.max_value, cpu)
        self.assertEqual(s.resp_hist.max_value, ela_diff)
        self.assertEqual(s.resp_without_waits_hist.max_value, ela_nowait)

    def test_process_file_simple_2x(self):
        # Calculated from the trace file
        # From 1st exec
        cpu = 553
        # From 2nd exec
        elapsed = 2357
        ela_diff = 1637
        ela_nowait = 1146

        tracker = CursorTracker(None)
        util.process_file(tracker, 'tests/simple_trace_2x.trc', sql_ids)

        # There is special statement for cursor #0, so len == 2
        self.assertEqual(len(tracker.statements), 2)
        self.assertEqual(len(tracker.cursors), 2)

        s = tracker.statements['atxg62s17nkj4']
        self.assertEqual(s.fetches, 4)
        self.assertEqual(s.exec_hist_elapsed.total_count, 2)
        self.assertEqual(s.exec_hist_cpu.total_count, 2)
        self.assertEqual(s.exec_hist_elapsed.max_value, elapsed)
        self.assertEqual(s.exec_hist_cpu.max_value, cpu)
        self.assertEqual(s.resp_hist.max_value, ela_diff)
        self.assertEqual(s.resp_without_waits_hist.max_value, ela_nowait)

    def test_process_file_missing_parse(self):
        # Calculated from the trace file
        # From 1st exec
        cpu = 553
        # From 2nd exec
        elapsed = 2357
        ela_diff = 1637
        ela_nowait = 1146

        tracker = CursorTracker(None)
        util.process_file(tracker, 'tests/simple_trace_missing_parse.trc', sql_ids)

        # There is special statement for cursor #0, so len == 2
        self.assertEqual(len(tracker.statements), 2)
        self.assertEqual(len(tracker.cursors), 2)

        s = tracker.statements['atxg62s17nkj4']
        self.assertEqual(s.fetches, 4)
        self.assertEqual(s.exec_hist_elapsed.total_count, 2)
        self.assertEqual(s.exec_hist_cpu.total_count, 2)
        self.assertEqual(s.exec_hist_elapsed.max_value, elapsed)
        self.assertEqual(s.exec_hist_cpu.max_value, cpu)
        self.assertEqual(s.resp_hist.max_value, ela_diff)
        self.assertEqual(s.resp_without_waits_hist.max_value, ela_nowait)

    def test_process_file_3_statements_1_cursor(self):
        tracker = CursorTracker(None)
        util.process_file(tracker, 'tests/two_statements_one_cursor.trc', sql_ids)

        s = tracker.statements['cdgn9f8spbxnt']
        self.assertEqual(s.fetches, 1)
        self.assertEqual(s.exec_hist_elapsed.total_count, 1)

        s = tracker.statements['atxg62s17nkj4']
        self.assertEqual(s.fetches, 2)
        self.assertEqual(s.exec_hist_elapsed.total_count, 1)

        s = tracker.statements['6ssxu7vjxb51a']
        self.assertEqual(s.fetches, 11)
        self.assertEqual(s.exec_hist_elapsed.total_count, 1)

        self.assertEqual(len(tracker.statements), 4)
        self.assertEqual(len(tracker.cursors), 4)
if __name__ == '__main__':
    unittest.main()
