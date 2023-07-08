import unittest
from current_statement import CurrentStatement
from ops import Ops
from test_db import DB
import test

class TestCurrentStatement(unittest.TestCase):
    def setUp(self):
        self.cstat = CurrentStatement(test.CURSOR, None)
    def test_dump_to_db(self):
        with self.assertRaisesRegex(BaseException, 'dump_to_db: database not set!'):
            self.cstat.dump_to_db()

        self.cstat.dbs = DB()
        for ops in test.CORRECT_OPS:
            self.cstat.add_ops(test.CORRECT_OPS[ops])
        self.cstat.dump_to_db()
        self.assertEqual(len(self.cstat.dbs.batches), len(test.CORRECT_OPS))
    def test_is_not_empty(self):
        self.assertFalse(self.cstat.is_not_empty())
        self.cstat.add_ops(test.CORRECT_OPS['EXEC'])
        self.assertTrue(self.cstat.is_not_empty())

        self.cstat = CurrentStatement(test.CURSOR, None)
        wait = test.CORRECT_OPS['WAIT']
        for i in range(0, 3):
            self.cstat.add_ops(wait)
        self.assertTrue(self.cstat.is_not_empty())
    def test_add_ops(self):
        for ops in test.CORRECT_OPS:
            self.cstat.add_ops(test.CORRECT_OPS[ops])
        stat = test.CORRECT_OPS['STAT']
        self.cstat.add_ops(stat)
        wait = test.CORRECT_OPS['WAIT']
        self.cstat.add_ops(wait)

        self.assertTrue(self.cstat.is_set('PARSE'))
        self.assertTrue(self.cstat.is_set('EXEC'))
        self.assertTrue(self.cstat.is_set('CLOSE'))
        self.assertTrue(self.cstat.is_set('STAT'))
        self.assertTrue(self.cstat.is_set('WAIT'))

        self.assertFalse(self.cstat.is_set('BINDS'))
        self.assertFalse(self.cstat.is_set('PIC'))

        wrong_cs = Ops('WAIT', test.WRONG_CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 ", test.FNAME, 3)
        with self.assertRaisesRegex(BaseException, 'add_ops: wrong cursor *'):
            self.cstat.add_ops(wrong_cs)

        wrong_ops = Ops('STAR', test.CURSOR, " ", test.FNAME, 3) 
        with self.assertRaisesRegex(BaseException, 'add_ops: unknown ops type:*'):
            self.cstat.add_ops(wrong_ops)

        ops = test.CORRECT_OPS['EXEC']
        with self.assertRaisesRegex(BaseException, 'add_ops: already set: *'):
            self.cstat.add_ops(ops)

    def test_is_set(self):
        self.assertFalse(self.cstat.is_set('EXEC'))

    def test_count_ops(self):
        self.assertEqual(self.cstat.count_ops('FETCH'), 0)

        ops = test.CORRECT_OPS['EXEC']
        self.cstat.add_ops(ops)
        self.assertEqual(self.cstat.count_ops('EXEC'), 1)

        wait = test.CORRECT_OPS['WAIT']
        for i in range(0, 3):
            self.cstat.add_ops(wait)
        self.assertEqual(self.cstat.count_ops('WAIT'), 3)
if __name__ == '__main__':
    unittest.main()
