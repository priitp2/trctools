import datetime
import unittest
from backend import arrow
from tests import test_constants

class TestOps(unittest.TestCase):
    def test_init(self):
        ops = test_constants.TRACKED_OPS['EXEC']
        self.assertEqual(ops.op_type, 'EXEC')
        self.assertEqual(ops.cursor, test_constants.CURSOR)
        self.assertEqual(ops.c, 73)
        self.assertEqual(ops.e, 73)
        self.assertEqual(ops.p, 1)
        self.assertEqual(ops.cr, 2)
        self.assertEqual(ops.cu, 3)
        self.assertEqual(ops.mis, 4)
        self.assertEqual(ops.r, 5)
        self.assertEqual(ops.dep, 6)
        self.assertEqual(ops.og, 7)
        self.assertEqual(ops.plh, 2725028981)
        self.assertEqual(ops.tim, 5793511830834)
        # Isn't set for EXEC, should be 0
        self.assertEqual(ops.err, 0)

    def test_init_close(self):
        ops = test_constants.TRACKED_OPS['CLOSE']
        self.assertEqual(ops.op_type, 'CLOSE')
        self.assertEqual(ops.cursor, test_constants.CURSOR)
        self.assertEqual(ops.c, 0)
        self.assertEqual(ops.e, 4)
        self.assertEqual(ops.dep, 0)
        self.assertEqual(ops.type, 3)
        self.assertEqual(ops.tim, 5793512315335)

        # Missing attribute,but present in __slots__: __getattr__ returns 0 instead
        self.assertEqual(ops.cu, 0)

    def test_init_wait(self):
        ops = test_constants.TRACKED_OPS['WAIT']
        self.assertEqual(ops.dbop.op_type, 'WAIT')
        self.assertEqual(ops.dbop.cursor, test_constants.CURSOR)
        self.assertEqual(ops.dbop.e, 403)
        self.assertEqual(ops.dbop.tim, 5793512314261)
        self.assertEqual(ops.dbop.name, 'db file sequential read')

        # Missing attribute, not in __slots__
        with self.assertRaises(AttributeError):
            self.assertEqual(ops.doesnotexist, 1)

    def test_astuple(self):
        sql_id = 'abc123'
        ops = test_constants.TRACKED_OPS['EXEC']
        today = datetime.datetime.now()
        ops.ts_callback = lambda x: today
        lst = ops.astuple(0, sql_id)
        self.assertEqual(len(lst), len(arrow.PARQUET_SCHEMA))
        self.assertEqual(lst[0], 0)
        self.assertEqual(lst[1], sql_id)
        self.assertEqual(lst[2], test_constants.CURSOR)
        self.assertEqual(lst[3], 'EXEC')
        self.assertEqual(lst[4], ops.c)
        self.assertEqual(lst[5], ops.e)
        self.assertEqual(lst[6], ops.p)
        self.assertEqual(lst[7], ops.cr)
        self.assertEqual(lst[8], ops.cu)
        self.assertEqual(lst[9], ops.mis)
        self.assertEqual(lst[10], ops.r)
        self.assertEqual(lst[11], ops.dep)
        self.assertEqual(lst[12], ops.og)
        self.assertEqual(lst[13], ops.plh)
        self.assertEqual(lst[14], ops.tim)
        self.assertEqual(lst[15], 0)
        self.assertEqual(lst[16], '')
        self.assertEqual(lst[17], '')
        self.assertEqual(lst[18], test_constants.FMETA['FILE_NAME'])
        self.assertEqual(lst[19], test_constants.FMETA['LINE_COUNT'])
        # ts2
        self.assertEqual(lst[20], today)

    def test_str(self):
        ops = test_constants.TRACKED_OPS['WAIT']
        self.assertRegex(str(ops), f'^{test_constants.CURSOR}: WAIT*')

        ops = test_constants.TRACKED_OPS['EXEC']
        self.assertRegex(str(ops), f'^{test_constants.CURSOR}: EXEC*')

        ops = test_constants.TRACKED_OPS['CLOSE']
        self.assertRegex(str(ops), f'^{test_constants.CURSOR}: CLOSE*')

        ops = test_constants.UNTRACKED_OPS['STAR']
        self.assertRegex(str(ops), r'^\*\*\* CLIENT DRIVER:*')

        ops = test_constants.TRACKED_OPS['PIC']
        self.assertRegex(str(ops), f'^PARSING IN CURSOR (.*) tim={ops.dbop.tim}(.*)')

        ops = test_constants.UNTRACKED_OPS['XCTEND']
        self.assertRegex(str(ops), '^XCTEND rlbk=0,*')

    def test_error(self):
        ops = test_constants.TRACKED_OPS['ERROR']
        self.assertEqual(ops.err, 27403)
        self.assertEqual(ops.tim, 3034700189155)

    def test_to_dict(self):
        ops = test_constants.TRACKED_OPS['FETCH']
        span_id = 666
        d = ops.to_dict(span_id, test_constants.CURSOR)
        self.assertTrue(isinstance(d, dict))
        self.assertEqual(span_id, d['span_id'])
        self.assertEqual(test_constants.CURSOR, d['cursor'])
        self.assertEqual(ops.e, d['e'])
        self.assertEqual(ops.r, d['r'])

        ops = test_constants.TRACKED_OPS['PIC']
        ops.dbop.ts = None
        d = ops.to_dict(span_id, test_constants.CURSOR)
        self.assertEqual('PARSING IN CURSOR', d['op_type'])
        # If ts is not set, to_dict generates it from the callback
        self.assertIsNotNone(d['ts'])

if __name__ == '__main__':
    unittest.main()
