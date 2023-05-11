import unittest
import util
from cursor_tracker import CursorTracker

cursor1 = '#123223'
class TestUtil(unittest.TestCase):
    def test_merge_lat_objects(self):
        obj1 = (cursor1, 0, 0)
        obj2 = (cursor1, 0, 0)
        obj3 = (cursor1, 1, 2)

        ret = util.merge_lat_objects(obj1, obj2)
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 0)
        self.assertEqual(ret[2], 0)

        ret = util.merge_lat_objects(obj1, obj3)
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 1)
        self.assertEqual(ret[2], 2)

        with self.assertRaises(BaseException):
            ret = util.merge_lat_objects(obj3, ('123', 0, 0))

        ret = util.merge_lat_objects(obj3, None)
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 1)
        self.assertEqual(ret[2], 2)

        ret = util.merge_lat_objects(obj3, [])
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 1)
        self.assertEqual(ret[2], 2)

        ret = util.merge_lat_objects([cursor1, -1], obj3)
        self.assertEqual(ret, None)

        ret = util.merge_lat_objects(obj1, [obj3, obj3])
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 2)
        self.assertEqual(ret[2], 4)

        ret = util.merge_lat_objects(obj3, ())
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], obj3[1])
        self.assertEqual(ret[2], obj3[2])

        with self.assertRaises(TypeError):
            ret = util.merge_lat_objects(obj3, 666)

        ret = util.merge_lat_objects(obj3, (cursor1, -1))
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], obj3[1])
        self.assertEqual(ret[2], obj3[2])

        ret = util.merge_lat_objects(None, (None, obj3[1], obj3[2]))
        self.assertEqual(ret[0], None)
        self.assertEqual(ret[1], obj3[1])
        self.assertEqual(ret[2], obj3[2])
    def test_split_event(self):
        ev = 'c=143,e=143,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=1164377159,tim=5185921053409'

        out = util.split_event('')
        self.assertEqual(len(out), 0)

        out = util.split_event('xxx yyyy')
        self.assertEqual(len(out), 0)

        out = util.split_event(ev)
        self.assertEqual(len(out), 11)
        self.assertEqual(out['og'], '1')
    def test_process_file_simple(self):
        # Calculated from the trace file
        cpu = 553
        elapsed = 1353
        ela_diff = 1221
        ela_nowait = 598

        tracker = CursorTracker(None)
        util.process_file(tracker, 'tests/simple_trace.trc')

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
        util.process_file(tracker, 'tests/simple_trace_2x.trc')

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



if __name__ == '__main__':
    unittest.main()
