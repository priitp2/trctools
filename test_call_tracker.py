import unittest
from CallTracker import CallTracker
from ops import Ops
from test_db import DB
import test

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"
fname = 'trace.trc'
line = 1

class TestCallTracker(unittest.TestCase):
    def setUp(self):
        db = DB()
        self.tracker = CallTracker(db)
    def test_add_cursor(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)


        self.tracker.add_latest_cursor(cursor)
        # There is a special cursor '#0'
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.tracker.add_latest_cursor(cursor)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
    def test_add_parse(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])

        self.tracker.add_ops(cursor, test.CORRECT_OPS['PARSE'])
        # add_parsing_in adds item to latest_cursors
        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertEqual(cs.count_ops('PARSE'), 1)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)

        self.assertEqual(len(self.tracker.db.batches), 0)

        # This merges the item in tr.latest_cursors with statements and overwrites the item
        self.tracker.add_ops(cursor, test.CORRECT_OPS['PARSE'])
        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('PARSE'))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)

    def test_add_exec(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        self.tracker.add_ops(cursor, test.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.db.batches), 0)

        self.tracker.add_ops(cursor, test.CORRECT_OPS['EXEC'])

        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('EXEC'))

        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)

        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_fetch(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        for i in range(0, 3):
            self.tracker.add_ops(cursor, test.CORRECT_OPS['FETCH'])
        cs = self.tracker.latest_cursors[cursor]
        self.assertEqual(cs.count_ops('FETCH'), 3)
    def test_add_wait(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        for i in range(0, 3):
            self.tracker.add_ops(cursor, test.CORRECT_OPS['WAIT'])
        cs = self.tracker.latest_cursors[cursor]
        self.assertEqual(cs.count_ops('WAIT'), 3)
    def test_add_close(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])

        self.tracker.add_ops(cursor, test.CORRECT_OPS['EXEC'])

        self.tracker.add_ops(cursor, test.CORRECT_OPS['CLOSE'])

        # This closes current statement/client int
        self.tracker.add_ops(cursor, test.CORRECT_OPS['CLOSE'])

        cs = self.tracker._get_cursor(cursor)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('CLOSE'))
        self.assertFalse(cs.is_set('EXEC'))

        self.assertEqual(len(self.tracker.db.batches), 3)
    def test_stray_cursors(self):
        # FIXME: is it needed?
        # When PARSING IN CURSOR is missing in the trace file
        self.tracker.add_ops(test.CURSOR, test.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker = CallTracker(None)
        self.tracker.add_ops(test.CURSOR, test.CORRECT_OPS['FETCH'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker = CallTracker(None)
        self.tracker.add_ops(test.CURSOR, test.CORRECT_OPS['WAIT'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker.add_pic(test.CURSOR, test.CORRECT_OPS['PIC'])
        # FIXME: what to do with those? What if there's another parse call later?
    def test__get_cursor(self):
        self.assertEqual(self.tracker._get_cursor('#123'), None)
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        self.assertEqual(self.tracker._get_cursor(cursor).cursor, cursor)
        self.assertEqual(self.tracker._get_cursor('#123'), None)
    def test__add_dummy_statement(self):
        self.tracker._add_dummy_statement('#1234')
        self.assertNotEqual(self.tracker._get_cursor('#1234'), None)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertTrue('dummy1' in self.tracker.statements)
        with self.assertRaisesRegex(BaseException, '_add_dummy_statement: *'):
            self.tracker._add_dummy_statement('#1234')

    def test_reset(self):
        self.tracker = CallTracker(None)
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        for i in range(0, 4):
            self.tracker._add_dummy_statement(f'#c{i}')

        self.assertEqual(len(self.tracker.latest_cursors), 6)
        self.tracker.reset()
        self.assertEqual(len(self.tracker.latest_cursors), 1)

    def test_add_pic(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        self.tracker.add_ops(cursor,  test.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertTrue(test.CORRECT_OPS['PIC'].sqlid in self.tracker.statements)

        # Another PIC should close existing cursor/interaction
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        crs = self.tracker.latest_cursors[cursor]
        self.assertTrue(crs.is_set('PIC'))
        self.assertFalse(crs.is_set('EXEC'))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_ops_add_pic(self):
        self.tracker.add_pic(cursor, test.CORRECT_OPS['PIC'])
        self.tracker.add_ops(cursor, test.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertTrue(test.CORRECT_OPS['PIC'].sqlid in self.tracker.statements)

        # This closes existing CurrentStatement/db interaction and adds new one
        self.tracker.add_ops(cursor, test.CORRECT_OPS['PIC'])
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)

        # This won't close the db interaction
        self.tracker.add_ops(cursor, test.CORRECT_OPS['WAIT'])
        self.assertEqual(len(self.tracker.db.batches), 2)
if __name__ == '__main__':
    unittest.main()
