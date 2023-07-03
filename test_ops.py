import unittest
from ops import Ops

CURSOR = '#140641987987624'
FNAME = 'file.trc'
class TestOps(unittest.TestCase):
    def test_init(self):
        ops = Ops('EXEC', CURSOR, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', FNAME, 3)
        self.assertEqual(ops.op_type, 'EXEC')
        self.assertEqual(ops.cursor, CURSOR)
        self.assertEqual(ops.c, 73)
        self.assertEqual(ops.e, 73)
        self.assertEqual(ops.p, 0)
        self.assertEqual(ops.cr, 0)
        self.assertEqual(ops.cu, 0)
        self.assertEqual(ops.mis, 0)
        self.assertEqual(ops.r, 0)
        self.assertEqual(ops.dep, 0)
        self.assertEqual(ops.og, 1)
        self.assertEqual(ops.plh, 2725028981)
        self.assertEqual(ops.tim, 5793511830834)

    def test_init_close(self):
        ops = Ops('CLOSE', '#140641987987624', 'c=3,e=3,dep=0,type=1,tim=5793511831927', FNAME, 2)
        self.assertEqual(ops.op_type, 'CLOSE')
        self.assertEqual(ops.cursor, CURSOR)
        self.assertEqual(ops.c, 3)
        self.assertEqual(ops.e, 3)
        self.assertEqual(ops.dep, 0)
        self.assertEqual(ops.type, 1)
        self.assertEqual(ops.tim, 5793511831927)

        # Missing attribute,but present in __slots__: __getattr__ returns 0 instead
        self.assertEqual(ops.cu, 0)

    def test_init_wait(self):
        # FIXME: space in the beginning of the param string
        ops = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", FNAME, 2)
        self.assertEqual(ops.op_type, 'WAIT')
        self.assertEqual(ops.cursor, CURSOR)
        self.assertEqual(ops.e, 1)
        self.assertEqual(ops.tim, 5793511831582)
        self.assertEqual(ops.name, 'SQL*Net message to client')

        # Missing attribute, not in __slots__
        with self.assertRaises(AttributeError):
            self.assertEqual(ops.og, 1)

    def test_merge(self):
        ops1 = Ops('EXEC', CURSOR, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', FNAME, 2)

        ops2 = Ops('EXEC', '#0', 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', FNAME, 1)
        with self.assertRaises(ValueError):
            ops3 = ops2.merge(ops1)

        ops2 = Ops('EXEC', CURSOR, 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', FNAME, 1)
        ops3 = ops2.merge(ops1)
        self.assertEqual(ops2.op_type, ops3.op_type)
        self.assertEqual(ops3.c, 196)
        self.assertEqual(ops3.e, 296)
        self.assertEqual(ops3.tim, 0)

        ops3 = Ops('EXEC', CURSOR, 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', FNAME, 1)
        ops4 = ops3.merge([ops1, ops2])
        self.assertEqual(ops4.c, ops1.c + ops2.c + ops3.c)
        self.assertEqual(ops4.e, ops1.e + ops2.e + ops3.e)

    def test_to_list(self):
        sql_id = 'abc123'
        ops = Ops('EXEC', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 1)
        lst = ops.to_list(0, sql_id)
        self.assertEqual(len(lst), len(ops.__slots__) + 13)
        self.assertEqual(lst[0], 0)
        self.assertEqual(lst[1], sql_id)
        self.assertEqual(lst[2], CURSOR)
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
        self.assertEqual(lst[18], FNAME)
        self.assertEqual(lst[19], 1)
        # ts2
        self.assertEqual(lst[20], None)

    def test_empty_wait(self):
        ops1 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", FNAME, 3)
        ops2 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 66 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", FNAME, 4)
        ops3 = ops2.merge(ops1)
        self.assertEqual(ops3.e, 67)
        self.assertEqual(ops3.c, 0)

    def test_str(self):
        ops = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", FNAME, 2)
        self.assertRegex(str(ops), '^#140641987987624: WAIT*')

        ops = Ops('EXEC', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 4)
        self.assertRegex(str(ops), '^#140641987987624: EXEC*')

        ops = Ops('CLOSE', '#140641987987624', 'c=3,e=3,dep=0,type=1,tim=5793511831927', FNAME, 4)
        self.assertRegex(str(ops), '^#140641987987624: CLOSE*')

        ops = Ops('STAR', None, '123.223', FNAME, 4, name='SESSION ID')
        self.assertRegex(str(ops), r'^\*\*\* SESSION ID*')

        ops = Ops('PIC', CURSOR, "len=80 dep=0 uid=331 oct=3 lid=331 tim=7104844976089 hv=1167462720 ad='9d4125228' sqlid='6v48b7j2tc4a0'", FNAME, 4, name='select dummy from dual')
        self.assertRegex(str(ops), '^PARSING IN CURSOR (.*) tim=7104844976089(.*)')

        ops = Ops('XCTEND', None, 'rlbk=0, rd_only=1, tim=5793512315347', FNAME, 4)
        self.assertRegex(str(ops), '^XCTEND rlbk=0,*')
if __name__ == '__main__':
    unittest.main()
