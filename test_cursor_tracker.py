import unittest
from cursor_tracker import CursorTracker
from ops import Ops

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"
fname = 'trace.trc'

class TestCursorTracker(unittest.TestCase):
    def test_init(self):
        tr = CursorTracker(None)
    def test_add_cursor(self):
        tr = CursorTracker(None)
        tr.add_parsing_in(cursor, params)
        self.assertEqual(len(tr.cursors), 2)
        self.assertEqual(len(tr.statements), 2)


        tr.add_latest_cursor(cursor)
        # There is a special cursor '#0'
        self.assertEqual(len(tr.latest_cursors), 2)
        tr.add_latest_cursor(cursor)
        self.assertEqual(len(tr.latest_cursors), 2)
    def test_add_parse(self):
        tr = CursorTracker(None)
        tr.add_parsing_in(cursor, params)

        parse_lat = Ops('PARSE', cursor, '', fname, 0)
        tr.add_parse(cursor, parse_lat)
        # add_parsing_in adds item to latest_cursors
        cs = tr._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertNotEqual(cs.parse, None)
        self.assertEqual(len(tr.latest_cursors), 2)
        self.assertEqual(len(tr.statements), 2)
        self.assertEqual(len(tr.cursors), 2)
        st = tr.statements[tr.cursors[cursor]]
        # Histograms should be empty
        self.assertEqual(st.exec_hist_cpu.get_total_count(), 0)
        self.assertEqual(st.exec_hist_elapsed.get_total_count(), 0)

        # This merges the item in tr.latest_cursors with statements and overwrites the item
        tr.add_parse(cursor, parse_lat)
        cs = tr._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertNotEqual(cs.parse, None)
        self.assertEqual(len(tr.latest_cursors), 2)
        self.assertEqual(len(tr.statements), 2)
        self.assertEqual(len(tr.cursors), 2)

        # Since we merged the current cursor with the statement, there should be 1 item in 'fetch' histograms
        self.assertEqual(st.exec_hist_cpu.get_total_count(), 1)
        self.assertEqual(st.exec_hist_elapsed.get_total_count(), 1)
    def test_add_exec(self):
        tr = CursorTracker(None)
        tr.add_parsing_in(cursor, params)
        exec_lat = Ops('EXEC', cursor, '', fname, 0)
        tr.add_exec(cursor, exec_lat)

        tr.add_exec(cursor, exec_lat)

        cs = tr._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertNotEqual(cs.exec, None)

        self.assertEqual(len(tr.latest_cursors), 2)
        self.assertEqual(len(tr.statements), 2)
        self.assertEqual(len(tr.cursors), 2)

        st = tr.statements[tr.cursors[cursor]]
        self.assertEqual(st.exec_hist_cpu.get_total_count(), 1)
        self.assertEqual(st.exec_hist_elapsed.get_total_count(), 1)
    def test_add_fetch(self):
        tr = CursorTracker(None)
        tr.add_parsing_in(cursor, params)
        fetch_lat = Ops('FETCH', cursor, '', fname, 0)
        tr.add_fetch(cursor, fetch_lat)
        tr.add_fetch(cursor, fetch_lat)
        tr.add_fetch(cursor, fetch_lat)
        cs = tr.latest_cursors[cursor]
        self.assertEqual(len(cs.fetches), 3)
    def test_add_wait(self):
        tr = CursorTracker(None)
        tr.add_parsing_in(cursor, params)
        wait_lat = Ops('WAIT', cursor, '', fname, 0)
        tr.add_wait(cursor, wait_lat)
        tr.add_wait(cursor, wait_lat)
        tr.add_wait(cursor, wait_lat)
        cs = tr.latest_cursors[cursor]
        self.assertEqual(len(cs.waits), 3)
    def test_add_close(self):
        tr = CursorTracker(None)
        tr.add_parsing_in(cursor, params)

        exec_lat = Ops('EXEC', cursor, '', fname, 0)
        tr.add_exec(cursor, exec_lat)

        close_lat = Ops('CLOSE', cursor, '', fname, 0)
        tr.add_close(cursor, close_lat)

        # Cursor got cleaned up
        cs = tr._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertEqual(cs.close, None)
        self.assertEqual(cs.exec, None)

        # Check if cursor got added to the statement
        st = tr.statements[tr.cursors[cursor]]
        self.assertEqual(st.exec_hist_cpu.get_total_count(), 1)
        self.assertEqual(st.exec_hist_elapsed.get_total_count(), 1)
    def test_stray_cursors(self):
        # When PARSING IN CURSOR is missing in the trace file
        tr = CursorTracker(None)
        o = Ops('EXEC', '#1234', '', fname, 0)
        tr.add_exec('#1234', o)
        self.assertEqual(len(tr.cursors), 2)
        self.assertEqual(len(tr.statements), 2)

        tr = CursorTracker(None)
        o = Ops('FETCH', '#1234', '', fname, 0)
        tr.add_fetch('#1234', o)
        self.assertEqual(len(tr.cursors), 2)
        self.assertEqual(len(tr.statements), 2)

        tr = CursorTracker(None)
        o = Ops('WAIT', '#1234', '', fname, 0)
        tr.add_wait('#1234', o)
        self.assertEqual(len(tr.cursors), 2)
        self.assertEqual(len(tr.statements), 2)

        tr.add_parsing_in('#1234', params)
        # FIXME: what to do with those? What if there's another parse call later?
    def test__get_cursor(self):
        tr = CursorTracker(None)
        self.assertEqual(tr._get_cursor('#123'), None)
        tr.add_parsing_in(cursor, params)
        self.assertEqual(tr._get_cursor(cursor).cursor, cursor)
        self.assertEqual(tr._get_cursor('#123'), None)
    def test__add_dummy_statement(self):
        tr = CursorTracker(None)
        tr._add_dummy_statement('#1234')
        self.assertNotEqual(tr._get_cursor('#1234'), None)
        self.assertEqual(len(tr.cursors), 2)
        self.assertEqual(len(tr.statements), 2)
        self.assertTrue('dummy1' in tr.statements.keys())
        with self.assertRaisesRegex(BaseException, '_add_dummy_statement: *'):
            tr._add_dummy_statement('#1234')



if __name__ == '__main__':
    unittest.main()

