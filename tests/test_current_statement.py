import unittest
from current_statement import CurrentStatement
from ops import ops_factory
from tests.mock_backend import Backend
import tests.test_constants as test_constants

class TestCurrentStatement(unittest.TestCase):
    """Tests for CurrentStatement."""
    def setUp(self):
        self.cstat = CurrentStatement(test_constants.CURSOR, None)
    def test_to_list(self):
        """Check if to_list returns correct # of elements."""
        self.cstat.dbs = Backend()
        for ops in test_constants.TRACKED_OPS.values():
            self.cstat.add_ops(ops)
        out = self.cstat.to_list(1)
        self.assertEqual(len(out), len(test_constants.TRACKED_OPS))
    def test_empty_cursor(self):
        """Malformed cursor should trigger exception"""
        with self.assertRaises(ValueError):
            self.cstat = CurrentStatement('#1', None)
        with self.assertRaises(ValueError):
            self.cstat = CurrentStatement('#', None)
        with self.assertRaises(ValueError):
            self.cstat = CurrentStatement(None, None)
    def test_add_ops(self):
        """Check if adding ops works."""
        for ops in test_constants.TRACKED_OPS.values():
            self.cstat.add_ops(ops)
        stat = test_constants.TRACKED_OPS['STAT']
        self.cstat.add_ops(stat)
        wait = test_constants.TRACKED_OPS['WAIT']
        self.cstat.add_ops(wait)

        self.assertTrue('PARSE' in self.cstat.ops)
        self.assertTrue('EXEC' in self.cstat.ops)
        self.assertTrue('CLOSE' in self.cstat.ops)
        self.assertEqual(self.cstat.count_ops('STAT'), 2)
        self.assertEqual(self.cstat.count_ops('WAIT'), 2)

        # Missing in TRACKED_OPS
        self.assertFalse('BINDS' in self.cstat.ops)

    def test_add_wrong_cursor(self):
        """Adding ops with different cursor should fail"""
        # This sets the cursor
        ops = test_constants.TRACKED_OPS['EXEC']
        self.cstat.add_ops(ops)

        wrong_cs = ops_factory('WAIT', test_constants.WRONG_CURSOR,
                               " nam='db file sequential read' ela= 403 file#=414 block#=2682927 ",
                               test_constants.FMETA, lambda x: None)
        with self.assertRaisesRegex(BaseException, 'add_ops: wrong cursor *'):
            self.cstat.add_ops(wrong_cs)

        wrong_ops = test_constants.UNTRACKED_OPS['STAR']
        with self.assertRaisesRegex(BaseException, 'add_ops: unknown ops type:*'):
            self.cstat.add_ops(wrong_ops)

    def test_ops_already_set(self):
        """Adding tracked non-container ops should fail"""
        ops = test_constants.TRACKED_OPS['EXEC']
        self.cstat.add_ops(ops)

        with self.assertRaisesRegex(BaseException, 'add_ops: already set: *'):
            self.cstat.add_ops(ops)

    def test_count_ops(self):
        """Check if count_ops works."""
        self.assertEqual(self.cstat.count_ops('FETCH'), 0)

        ops = test_constants.TRACKED_OPS['EXEC']
        self.cstat.add_ops(ops)
        self.assertEqual(self.cstat.count_ops('EXEC'), 1)

        wait = test_constants.TRACKED_OPS['WAIT']
        for _ in range(0, 3):
            self.cstat.add_ops(wait)
        self.assertEqual(self.cstat.count_ops('WAIT'), 3)
    def test_len(self):
        """Check if we have correct # of ops."""
        self.assertEqual(len(self.cstat), 0)

        ops = test_constants.TRACKED_OPS['EXEC']
        self.cstat.add_ops(ops)
        self.assertEqual(len(self.cstat), 1)

        wait = test_constants.TRACKED_OPS['WAIT']
        for _ in range(0, 3):
            self.cstat.add_ops(wait)
        self.assertEqual(len(self.cstat), 4)
if __name__ == '__main__':
    unittest.main()
