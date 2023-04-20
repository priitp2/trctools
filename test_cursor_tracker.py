import unittest
from cursor_tracker import CursorTracker

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"

class TestCursorTracker(unittest.TestCase):
    def test_init(self):
        tr = CursorTracker({}, {})
    def test_add_cursor(self):
        tr = CursorTracker({}, {})
        tr.add_parsing_in(cursor, params)
        self.assertEqual(len(tr.cursors), 1)
        self.assertEqual(len(tr.statements), 1)


        tr.add_latest_cursor(cursor)
        # There is a special cursor '#0'
        self.assertEqual(len(tr.latest_cursors), 2)
        tr.add_latest_cursor(cursor)
        self.assertEqual(len(tr.latest_cursors), 2)
    def test_add_parse(self):
        tr = CursorTracker({}, {})
        tr.add_parsing_in(cursor, params)

        parse_lat = (cursor, 1, 100)
        cs = tr.add_parse(cursor, parse_lat)
        # add_parsing_in adds item to latest_cursors
        self.assertNotEqual(cs, None)
        self.assertNotEqual(cs.parse, None)
        self.assertEqual(len(tr.latest_cursors), 2)
        self.assertEqual(len(tr.statements), 1)
        self.assertEqual(len(tr.cursors), 1)
        st = tr.statements[tr.cursors[cursor]]
        # Histograms should be empty
        self.assertEqual(st.fetch_hist_cpu.get_total_count(), 0)
        self.assertEqual(st.fetch_hist_elapsed.get_total_count(), 0)

        # This merges the item in tr.latest_cursors with statements, overwrites the item, and returns the old (overwritten) item
        cs = tr.add_parse(cursor, parse_lat)
        self.assertNotEqual(cs, None)
        self.assertNotEqual(cs.parse, None)
        self.assertEqual(len(tr.latest_cursors), 2)
        self.assertEqual(len(tr.statements), 1)
        self.assertEqual(len(tr.cursors), 1)

        # Since we merged the current cursor with the statement, there should be 1 item in 'fetch' histograms
        self.assertEqual(st.fetch_hist_cpu.get_total_count(), 1)
        self.assertEqual(st.fetch_hist_elapsed.get_total_count(), 1)
    def test_add_exec(self):
        tr = CursorTracker({}, {})
        tr.add_parsing_in(cursor, params)
        exec_lat = (cursor, 1, 100)
        cs = tr.add_exec(cursor, exec_lat)
        self.assertEqual(cs, None)

        cs = tr.add_exec(cursor, exec_lat)
        # Got back the old object
        self.assertNotEqual(cs, None)
        self.assertNotEqual(cs.exec, None)

        self.assertEqual(len(tr.latest_cursors), 2)
        self.assertEqual(len(tr.statements), 1)
        self.assertEqual(len(tr.cursors), 1)

        st = tr.statements[tr.cursors[cursor]]
        self.assertEqual(st.fetch_hist_cpu.get_total_count(), 1)
        self.assertEqual(st.fetch_hist_elapsed.get_total_count(), 1)
    def test_add_fetch(self):
        tr = CursorTracker({}, {})
        tr.add_parsing_in(cursor, params)
        fetch_lat = (cursor, 1, 100)
        tr.add_fetch(cursor, fetch_lat)
        tr.add_fetch(cursor, fetch_lat)
        tr.add_fetch(cursor, fetch_lat)
        cs = tr.latest_cursors[cursor]
        self.assertEqual(len(cs.fetches), 3)
    def test_add_wait(self):
        tr = CursorTracker({}, {})
        tr.add_parsing_in(cursor, params)
        wait_lat = (cursor, 1, 100)
        tr.add_wait(cursor, wait_lat)
        tr.add_wait(cursor, wait_lat)
        tr.add_wait(cursor, wait_lat)
        cs = tr.latest_cursors[cursor]
        self.assertEqual(len(cs.waits), 3)
    def test_add_close(self):
        tr = CursorTracker({}, {})
        tr.add_parsing_in(cursor, params)
        close_lat = (cursor, 1, 100)
        current_st = tr.add_close(cursor, close_lat)
        cs = tr.latest_cursors[cursor]

        # Got back the old object
        self.assertNotEqual(current_st.close, None)

        # New one hasn't been touched
        self.assertEqual(cs.close, None)
        self.assertEqual(cs.exec, None)
        st = tr.statements[tr.cursors[cursor]]
        self.assertEqual(st.fetch_hist_cpu.get_total_count(), 1)
        self.assertEqual(st.fetch_hist_elapsed.get_total_count(), 1)
    def test_stray_cursors(self):
        # When PARSING is missing in the trace file
        tr = CursorTracker({}, {})
        fetch_lat = ('#1234', 1, 100)
        tr.add_fetch('#1234', fetch_lat)
        tr.add_parsing_in('#1234', params)
        # FIXME: what to do with those? What if there's another parse call later?


if __name__ == '__main__':
    unittest.main()

