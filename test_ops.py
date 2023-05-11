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

if __name__ == '__main__':
    unittest.main()

