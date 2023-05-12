import unittest
from ops import Ops

cursor = '#140641987987624'
class TestOps(unittest.TestCase):
    def test_init(self):
        o = Ops('EXEC', cursor, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834')
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
        o = Ops('CLOSE', '#140641987987624', 'c=3,e=3,dep=0,type=1,tim=5793511831927')
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
        o = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582")
        self.assertEqual(o.op_type, 'WAIT')
        self.assertEqual(o.cursor, cursor)
        self.assertEqual(o.e, 1)
        self.assertEqual(o.tim, 5793511831582)
        self.assertEqual(o.name, 'SQL*Net message to client')

        # Missing attribute, not in __slots__
        with self.assertRaises(AttributeError):
            self.assertEqual(o.og, 1)

    def test_merge(self):
        o1 = Ops('EXEC', cursor, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834')

        o2 = Ops('EXEC', '#0', 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834')
        with self.assertRaises(ValueError):
            o3 = o2.merge(o1)

        o2 = Ops('EXEC', cursor, 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834')
        o3 = o2.merge(o1)
        self.assertEqual(o2.op_type, o3.op_type)
        self.assertEqual(o3.c, 196)
        self.assertEqual(o3.e, 296)
        self.assertEqual(o3.tim, 0)

        o3 = Ops('EXEC', cursor, 'c=123,e=223,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834')
        o4 = o3.merge([o1, o2])
        self.assertEqual(o4.c, o1.c + o2.c + o3.c)
        self.assertEqual(o4.e, o1.e + o2.e + o3.e)
    def test_to_list(self):
        o1 = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834')
        l = o1.to_list(0)
        self.assertEqual(len(l), len(o1.__slots__) + 1)
        self.assertEqual(l[0], 0)
        self.assertEqual(l[1], cursor)
        self.assertEqual(l[2], 'EXEC')
        self.assertEqual(l[3], o1.c)
        self.assertEqual(l[4], o1.e)
        self.assertEqual(l[5], o1.p)
        self.assertEqual(l[6], o1.cr)
        self.assertEqual(l[7], o1.cu)
        self.assertEqual(l[8], o1.mis)
        self.assertEqual(l[9], o1.r)
        self.assertEqual(l[10], o1.dep)
        self.assertEqual(l[11], o1.og)
        self.assertEqual(l[12], o1.plh)
        self.assertEqual(l[13], o1.tim)
        self.assertEqual(l[14], 0) # type will be 0

    def test_empty_wait(self):
        o1 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 1 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582")
        o2 = Ops('WAIT', '#140641987987624', " nam='SQL*Net message to client' ela= 66 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831582")
        o3 = o2.merge(o1)
        self.assertEqual(o3.e, 67)
        self.assertEqual(o3.c, 0)
if __name__ == '__main__':
    unittest.main()

