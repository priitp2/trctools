import unittest
from call_tracker import CallTracker
from tests.mock_backend import Backend
import tests.test_constants as test_constants

class TestCallTracker(unittest.TestCase):
    def setUp(self):
        dbs = Backend()
        self.tracker = CallTracker(dbs)
    def test_no_backend(self):
        tracker = CallTracker(None)
        with self.assertRaises(RuntimeError):
            tracker.flush()
    def test_add_cursor(self):
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        self.assertEqual(len(self.tracker.cursors), 1)


        self.tracker.add_latest_cursor(test_constants.CURSOR)
        # There is a special test_constants.CURSOR '#0'
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertEqual(len(self.tracker.db.batches), 1)

        self.tracker.add_latest_cursor(test_constants.CURSOR)
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertEqual(len(self.tracker.db.batches), 1)
    def test_add_parse(self):
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])

        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PARSE'])
        # add_parsing_in adds item to latest_test_constants.CURSORs
        cursor = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count_ops('PARSE'), 1)
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertEqual(len(self.tracker.cursors), 1)

        self.assertEqual(len(self.tracker.db.batches), 0)

        # This merges the item in tr.latest_test_constants.CURSORs with statements
        # and overwrites the item
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PARSE'])
        cursor = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertNotEqual(cursor, None)
        self.assertTrue('PARSE' in cursor.ops)
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertEqual(len(self.tracker.cursors), 1)
        self.assertEqual(len(self.tracker.db.batches), 2)

    def test_add_fetch(self):
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        for _ in range(0, 3):
            self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['FETCH'])
        cursor = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertEqual(cursor.count_ops('FETCH'), 3)
    def test_stray_cursors(self):
        """ When PARSING IN CURSOR is missing in the trace file. """
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['EXEC'])
        self.assertEqual(len(self.tracker.cursors), 1)

        self.tracker = CallTracker(None)
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['FETCH'])
        self.assertEqual(len(self.tracker.cursors), 1)

        self.tracker = CallTracker(None)
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['WAIT'])
        self.assertEqual(len(self.tracker.cursors), 1)

    def test__latest_cursors(self):
        self.assertEqual(self.tracker.latest_cursors['#123'], None)
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        self.assertEqual(self.tracker.latest_cursors[test_constants.CURSOR].cursor,
                test_constants.CURSOR)
        self.assertEqual(self.tracker.latest_cursors['#123'], None)

    def test_reset(self):
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        for i in range(0, 4):
            self.tracker.add_latest_cursor(f'#c{i}')

        self.assertEqual(len(self.tracker.latest_cursors), 5)
        self.tracker.reset()
        self.assertEqual(len(self.tracker.latest_cursors), 1)

    def test_add_ops(self):
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        self.tracker.add_ops(test_constants.CURSOR,  test_constants.TRACKED_OPS['EXEC'])
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertTrue(test_constants.TRACKED_OPS['PIC'].sqlid in self.tracker.cursors.values())

        # Another PIC should close existing test_constants.CURSOR/interaction
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        crs = self.tracker.latest_cursors[test_constants.CURSOR]
        self.assertTrue('PIC' in crs.ops)
        self.assertFalse('EXEC' in crs.ops)
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_ops_add_ops(self):
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['EXEC'])
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertTrue(test_constants.TRACKED_OPS['PIC'].sqlid in self.tracker.cursors.values())

        # This closes existing CurrentStatement/db interaction and adds new one
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['PIC'])
        self.assertEqual(len(self.tracker.latest_cursors), 1)
        self.assertEqual(len(self.tracker.db.batches), 2)

        # This won't close the db interaction
        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['WAIT'])
        self.assertEqual(len(self.tracker.db.batches), 2)
    def test_add_lob(self):
        self.assertFalse(self.tracker.add_lob(None))
        self.assertFalse(self.tracker.add_lob(test_constants.UNTRACKED_OPS['LOB']))

        self.tracker.add_ops(test_constants.CURSOR, test_constants.TRACKED_OPS['EXEC'])
        self.assertTrue(self.tracker.add_lob(test_constants.UNTRACKED_OPS['LOB']))

        self.tracker.flush()
        self.assertEqual(len(self.tracker.db.batches), 2)

if __name__ == '__main__':
    unittest.main()
