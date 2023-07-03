import unittest
from ops import Ops

cursor = '#140641987987624'
fname = 'file.trc'
class TestOps(unittest.TestCase):
    def test_init(self):
        o = Ops('EXEC', cursor, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', fname, 3)
        self.assertEqual(o.op_type, 'EXEC')
        self.assertEqual(o.cursor, cursor)
        self.assertEqual(o.c, 73)
        self.assertEqual(o.e, 73)
        self.assertEqual(o.p, 0)
        self.assertEqual(o.cr, 0)
        self.assertEqual(o.cu, 0)
        self.assertEqual(o.mis, 0)
        self.assertEqual(o.r, 0)
        self.assertEqual(o.dep, 0)
        self.assertEqual(o.og, 1)
        self.assertEqual(o.plh, 2725028981)
        self.assertEqual(o.tim, 5793511830834)

    def test_init_close(self):
        o = Ops('CLOSE', '#140641987987624', 'c=3,e=3,dep=0,type=1,tim=5793511831927', fname, 2)
        self.assertEqual(o.op_type, 'CLOSE')
        self.assertEqual(o.cursor, cursor)
        self.assertEqual(o.c, 3)
        self.assertEqual(o.e, 3)
        self.assertEqual(o.dep, 0)
        self.assertEqual(o.type, 1)
        self.assertEqual(o.tim, 5793511831927)

        # Missing attribute,but present in __slots__: __getattr__ returns 0 instead
        self.assertEqual(o.cu, 0)

    def test_init_wait(self):
        # FIXME: space in the beginning of the param string
        o = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", fname, 2)
        self.assertEqual(o.op_type, 'WAIT')
        self.assertEqual(o.cursor, cursor)
        self.assertEqual(o.e, 1)
        self.assertEqual(o.tim, 5793511831582)
        self.assertEqual(o.name, 'SQL*Net message to client')

        # Missing attribute, not in __slots__
        with self.assertRaises(AttributeError):
            self.assertEqual(o.og, 1)

    def test_merge(self):
        o1 = Ops('EXEC', cursor, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', fname, 2)

        o2 = Ops('EXEC', '#0', 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', fname, 1)
        with self.assertRaises(ValueError):
            o3 = o2.merge(o1)

        o2 = Ops('EXEC', cursor, 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', fname, 1)
        o3 = o2.merge(o1)
        self.assertEqual(o2.op_type, o3.op_type)
        self.assertEqual(o3.c, 196)
        self.assertEqual(o3.e, 296)
        self.assertEqual(o3.tim, 0)

        o3 = Ops('EXEC', cursor, 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834', fname, 1)
        o4 = o3.merge([o1, o2])
        self.assertEqual(o4.c, o1.c + o2.c + o3.c)
        self.assertEqual(o4.e, o1.e + o2.e + o3.e)

        o5 = o3.merge(None)
    def test_to_list(self):
        sql_id = 'abc123'
        o1 = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 1)
        l = o1.to_list(0, sql_id)
        self.assertEqual(len(l), len(o1.__slots__) + 11)
        self.assertEqual(l[0], 0)
        self.assertEqual(l[1], sql_id)
        self.assertEqual(l[2], cursor)
        self.assertEqual(l[3], 'EXEC')
        self.assertEqual(l[4], o1.c)
        self.assertEqual(l[5], o1.e)
        self.assertEqual(l[6], o1.p)
        self.assertEqual(l[7], o1.cr)
        self.assertEqual(l[8], o1.cu)
        self.assertEqual(l[9], o1.mis)
        self.assertEqual(l[10], o1.r)
        self.assertEqual(l[11], o1.dep)
        self.assertEqual(l[12], o1.og)
        self.assertEqual(l[13], o1.plh)
        self.assertEqual(l[14], o1.tim)
        self.assertEqual(l[15], 0) # type will be 0
        self.assertEqual(l[16], '') 
        self.assertEqual(l[17], '')
        self.assertEqual(l[18], fname)
        self.assertEqual(l[19], 1)
        # ts2
        self.assertEqual(l[20], None)

    def test_empty_wait(self):
        o1 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", fname, 3)
        o2 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 66 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", fname, 4)
        o3 = o2.merge(o1)
        self.assertEqual(o3.e, 67)
        self.assertEqual(o3.c, 0)

    def test_str(self):
        o1 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582", fname, 2)
        self.assertRegex(str(o1), '^#140641987987624: WAIT*')

        o1 = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 4)
        self.assertRegex(str(o1), '^#140641987987624: EXEC*')

        o1 = Ops('CLOSE', '#140641987987624', 'c=3,e=3,dep=0,type=1,tim=5793511831927', fname, 4)
        self.assertRegex(str(o1), '^#140641987987624: CLOSE*')

        o1 = Ops('STAR', None, '123.223', fname, 4, name='SESSION ID')
        self.assertRegex(str(o1), '^\*\*\* SESSION ID*')

        o1 = Ops('PIC', cursor, "len=80 dep=0 uid=331 oct=3 lid=331 tim=7104844976089 hv=1167462720 ad='9d4125228' sqlid='6v48b7j2tc4a0'", fname, 4, name='select dummy from dual')
        self.assertRegex(str(o1), '^PARSING IN CURSOR (.*) tim=7104844976089(.*)')
if __name__ == '__main__':
    unittest.main()

