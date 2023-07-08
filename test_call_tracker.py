import unittest
from CallTracker import CallTracker
from ops import Ops
from test_db import DB
import test_constants

class TestCallTracker(unittest.TestCase):
    def setUp(self):
        db = DB()
        self.tracker = CallTracker(db)
    def test_add_cursor(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)


        self.tracker.add_latest_cursor(test_constants.CURSOR)
        # There is a special test_constants.CURSOR '#0'
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.tracker.add_latest_cursor(test_constants.CURSOR)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
    def test_add_parse(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])

        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['PARSE'])
        # add_parsing_in adds item to latest_test_constants.CURSORs
        cs = self.tracker._get_cursor(test_constants.CURSOR)
        self.assertNotEqual(cs, None)
        self.assertEqual(cs.count_ops('PARSE'), 1)
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)

        self.assertEqual(len(self.tracker.db.batches), 0)

        # This merges the item in tr.latest_test_constants.CURSORs with statements and overwrites the item
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['PARSE'])
        cs = self.tracker._get_cursor(test_constants.CURSOR)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('PARSE'))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)

    def test_add_exec(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.db.batches), 0)

        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['EXEC'])

        cs = self.tracker._get_cursor(test_constants.CURSOR)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('EXEC'))

        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)
        self.assertEqual(len(self.tracker.cursors), 2)

        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_fetch(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        for i in range(0, 3):
            self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['FETCH'])
        cs = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertEqual(cs.count_ops('FETCH'), 3)
    def test_add_wait(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        for i in range(0, 3):
            self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['WAIT'])
        cs = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertEqual(cs.count_ops('WAIT'), 3)
    def test_add_close(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])

        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['EXEC'])

        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['CLOSE'])

        # This closes current statement/client int
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['CLOSE'])

        cs = self.tracker._get_cursor(test_constants.CURSOR)
        self.assertNotEqual(cs, None)
        self.assertTrue(cs.is_set('CLOSE'))
        self.assertFalse(cs.is_set('EXEC'))

        self.assertEqual(len(self.tracker.db.batches), 3)
    def test_stray_cursors(self):
        # FIXME: is it needed?
        # When PARSING IN CURSOR is missing in the trace file
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker = CallTracker(None)
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['FETCH'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker = CallTracker(None)
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['WAIT'])
        self.assertEqual(len(self.tracker.cursors), 2)
        self.assertEqual(len(self.tracker.statements), 2)

        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        # FIXME: what to do with those? What if there's another parse call later?
    def test__get_cursor(self):
        self.assertEqual(self.tracker._get_cursor('#123'), None)
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        self.assertEqual(self.tracker._get_cursor(test_constants.CURSOR).cursor, test_constants.CURSOR)
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
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        for i in range(0, 4):
            self.tracker._add_dummy_statement(f'#c{i}')

        self.assertEqual(len(self.tracker.latest_cursors), 6)
        self.tracker.reset()
        self.assertEqual(len(self.tracker.latest_cursors), 1)

    def test_add_pic(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        self.tracker.add_ops(test_constants.CURSOR,  test_constants.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertTrue(test_constants.CORRECT_OPS['PIC'].sqlid in self.tracker.statements)

        # Another PIC should close existing test_constants.CURSOR/interaction
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        crs = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertTrue(crs.is_set('PIC'))
        self.assertFalse(crs.is_set('EXEC'))
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_ops_add_pic(self):
        self.tracker.add_pic(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['EXEC'])
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertTrue(test_constants.CORRECT_OPS['PIC'].sqlid in self.tracker.statements)

        # This closes existing CurrentStatement/db interaction and adds new one
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['PIC'])
        self.assertEqual(len(self.tracker.latest_cursors), 2)
        self.assertEqual(len(self.tracker.db.batches), 2)

        # This won't close the db interaction
        self.tracker.add_ops(test_constants.CURSOR, test_constants.CORRECT_OPS['WAIT'])
        self.assertEqual(len(self.tracker.db.batches), 2)
if __name__ == '__main__':
    unittest.main()
