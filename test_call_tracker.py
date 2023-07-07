import unittest
from CallTracker import CallTracker
from ops import Ops
from test_db import DB

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"
fname = 'trace.trc'
line = 1

class TestCallTracker(unittest.TestCase):
    def setUp(self):
        db = DB()
        self.tracker = CallTracker(db)
    def test_add_cursor(self):
        self.tracker.add_pic(cursor, Ops('PIC', cursor, params, fname, line))
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)


        self.tracker.add_latest_cursor(cursor)
        # There is a special cursor '#0'
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.tracker.add_latest_cursor(cursor)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
    def test_add_parse(self):
        self.tracker.add_pic(cursor, Ops('PIC', cursor, params, fname, line))

        parse_lat = Ops('PARSE', cursor, '', fname, 0)
        self.tracker.add_ops(cursor, parse_lat)
        # add_parsing_in adds item to latest_cursors
        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertEqual(cs.count_ops('PARSE'), 1)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)

        self.assertEqual(len(self.tracker.db.batches), 0)

        # This merges the item in tr.latest_cursors with statements and overwrites the item
        self.tracker.add_ops(cursor, parse_lat)
        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('PARSE'))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)

    def test_add_exec(self):
        self.tracker.add_pic(cursor, Ops('PIC', cursor, params, fname, line))
        exec_lat = Ops('EXEC', cursor, '', fname, 0)
        self.tracker.add_ops(cursor, exec_lat)
        self.assertEqual(len(self.tracker.db.batches), 0)

        self.tracker.add_ops(cursor, exec_lat)

        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('EXEC'))

        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)

        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_fetch(self):
        self.tracker.add_pic(cursor, Ops('PIC', cursor, params, fname, line))
        fetch_lat = Ops('FETCH', cursor, '', fname, 0)
        for i in range(0, 3):
            self.tracker.add_ops(cursor, fetch_lat)
        cs = self.tracker.latest_cursors[cursor]
        self.assertEqual(cs.count_ops('FETCH'), 3)
    def test_add_wait(self):
        self.tracker.add_pic(cursor, Ops('PIC', cursor, params, fname, line))
        wait_lat = Ops('WAIT', cursor, '', fname, 0)
        for i in range(0, 3):
            self.tracker.add_ops(cursor, wait_lat)
        cs = self.tracker.latest_cursors[cursor]
        self.assertEqual(cs.count_ops('WAIT'), 3)
    def test_add_close(self):
        self.tracker.add_pic(cursor, Ops('PIC', cursor, params, fname, line))

        exec_lat = Ops('EXEC', cursor, '', fname, line)
        self.tracker.add_ops(cursor, exec_lat)

        close_lat = Ops('CLOSE', cursor, '', fname, line)
        self.tracker.add_ops(cursor, close_lat)

        # This closes current statement/client int
        self.tracker.add_ops(cursor, close_lat)

        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('CLOSE'))
        self.assertFalse(cs.is_set('EXEC'))

        self.assertEqual(len(self.tracker.db.batches), 3)
    def test_stray_cursors(self):
        # When PARSING IN CURSOR is missing in the trace file
        ops = Ops('EXEC', '#1234', '', fname, line)
        self.tracker.add_ops('#1234', ops)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker = CallTracker(None)
        ops = Ops('FETCH', '#1234', '', fname, line)
        self.tracker.add_ops('#1234', ops)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker = CallTracker(None)
        ops = Ops('WAIT', '#1234', '', fname, line)
        self.tracker.add_ops('#1234', ops)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker.add_pic('#1234', Ops('PIC', '#1234', params, fname, line))
        # FIXME: what to do with those? What if there's another parse call later?
    def test__get_cursor(self):
        tr = CallTracker(None)
        self.assertEqual(tr._get_cursor('#123'), None)
        tr.add_pic(cursor, Ops('PIC', cursor, params, fname, line))
        self.assertEqual(tr._get_cursor(cursor).cursor, cursor)
        self.assertEqual(tr._get_cursor('#123'), None)
    def test__add_dummy_statement(self):
        tr = CallTracker(None)
        tr._add_dummy_statement('#1234')
        self.assertNotEqual(tr._get_cursor('#1234'), None)
        self.assertEqual(len(tr.cursors), 2)
        self.assertEqual(len(tr.statements), 2)
        self.assertTrue('dummy1' in tr.statements.keys())
        with self.assertRaisesRegex(BaseException, '_add_dummy_statement: *'):
            tr._add_dummy_statement('#1234')

    def test_reset(self):
        tracker = CallTracker(None)
        tracker._add_dummy_statement('#123')
        tracker._add_dummy_statement('#223')
        tracker._add_dummy_statement('#323')
        tracker._add_dummy_statement('#423')

        self.assertEqual(len(tracker.latest_cursors), 5)
        tracker.reset()
        self.assertEqual(len(tracker.latest_cursors), 0)

    def test_add_pic(self):
        pic = Ops('PIC', cursor, params, fname, line)
        self.tracker.add_pic(cursor, pic)
        self.tracker.add_ops(cursor,  Ops('EXEC', cursor, '', fname, line))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertTrue(pic.sqlid in self.tracker.statements)

        # Another PIC should close existing cursor/interaction
        self.tracker.add_pic(cursor, pic)
        crs = self.tracker.latest_cursors[cursor]
        self.assertTrue(crs.is_set('PIC'))
        self.assertFalse(crs.is_set('EXEC'))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_ops_add_pic(self):
        pic = Ops('PIC', cursor, params, fname, line)
        self.tracker.add_pic(cursor, pic)
        ops = Ops('EXEC', cursor, '', fname, line)
        self.tracker.add_ops(cursor, ops)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertTrue(pic.sqlid in self.tracker.statements)

        # This closes existing CurrentStatement/db interaction and adds new one
        self.tracker.add_ops(cursor, ops)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)

        # This won't close the db interaction
        wait = Ops('WAIT', cursor, '', fname, line)
        self.tracker.add_ops(cursor, wait)
        self.assertEqual(len(self.tracker.db.batches), 2)
if __name__ == '__main__':
    unittest.main()
