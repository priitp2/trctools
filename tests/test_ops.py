import datetime
import unittest
from db import arrow
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
        self.assertEqual(ops.op_type, 'WAIT')
        self.assertEqual(ops.cursor, test_constants.CURSOR)
        self.assertEqual(ops.e, 403)
        self.assertEqual(ops.tim, 5793512314261)
        self.assertEqual(ops.name, 'db file sequential read')

        # Missing attribute, not in __slots__
        with self.assertRaises(AttributeError):
            self.assertEqual(ops.og, 1)

    def test_to_list(self):
        sql_id = 'abc123'
        ops = test_constants.TRACKED_OPS['EXEC']
        today = datetime.datetime.today()
        ops.ts_callback = lambda x: today
        lst = ops.to_list(0, sql_id)
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
        self.assertEqual(lst[15], 0) # type will be 0
        self.assertEqual(lst[16], '')
        self.assertEqual(lst[17], '')
        self.assertEqual(lst[18], test_constants.FMETA['FILE_NAME'])
        self.assertEqual(lst[19], test_constants.FMETA['LINE_COUNT'])
        # ts2
        self.assertIs(lst[20], today)

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
        self.assertRegex(str(ops), f'^PARSING IN CURSOR (.*) tim={ops.tim}(.*)')

        ops = test_constants.UNTRACKED_OPS['XCTEND']
        self.assertRegex(str(ops), '^XCTEND rlbk=0,*')
if __name__ == '__main__':
    unittest.main()
