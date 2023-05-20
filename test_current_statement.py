import unittest
from current_statement import CurrentStatement
from ops import Ops

cursor = '#123'
wrong_cursor = '#321'
fname = 'trace.trc'
class TestCurrentStatement(unittest.TestCase):
    def test_add_parse(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 0)

        cs.add_parse(o)
        with self.assertRaisesRegex(BaseException, 'add_parse: already set!'):
            cs.add_parse(o)

        cs = CurrentStatement(cursor, None)
        o = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 2)
        with self.assertRaisesRegex(BaseException, 'add_parse: wrong op_type *'):
            cs.add_parse(o)

        o = Ops('PARSE', wrong_cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 9)
        with self.assertRaisesRegex(BaseException, 'add_parse: got cursor *'):
            cs.add_parse(o)

    def test_add_exec(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 9)

        cs.add_exec(o)
        with self.assertRaisesRegex(BaseException, 'add_exec: already set!'):
            cs.add_exec(o)

        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 8)
        with self.assertRaisesRegex(BaseException, 'add_exec: wrong op_type *'):
            cs.add_exec(o)

        o = Ops('EXEC', wrong_cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 9)
        with self.assertRaisesRegex(BaseException, 'add_exec: got cursor *'):
            cs.add_exec(o)

    def test_add_wait(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 9)
        with self.assertRaisesRegex(BaseException, 'add_wait: wrong op_type *'):
            cs.add_wait(o)

        o = Ops('WAIT', wrong_cursor, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 blocks=1 obj#=89440 tim=5793512314261", fname, 3)
        with self.assertRaisesRegex(BaseException, 'add_wait: got cursor *'):
            cs.add_wait(o)

    def test_add_fetch(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 8)
        with self.assertRaisesRegex(BaseException, 'add_fetch: wrong op_type *'):
            cs.add_fetch(o)

        o = Ops('FETCH', wrong_cursor, 'c=475,e=474,p=1,cr=4,cu=0,mis=0,r=10,dep=0,og=1,plh=2725028981,tim=5793512314300', fname, 2)
        with self.assertRaisesRegex(BaseException, 'add_fetch: got cursor *'):
            cs.add_fetch(o)

    def test_add_close(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('CLOSE', cursor, 'c=0,e=4,dep=0,type=3,tim=5793512315335', fname, 8)

        cs.add_close(o)
        #with self.assertRaisesRegex(BaseException, 'add_close: already set!'):
        #    cs.add_close(o)
        cs.add_close(o)
        self.assertEqual(cs.close.c, 0)
        self.assertEqual(cs.close.e, 8)

        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 7)
        with self.assertRaisesRegex(BaseException, 'add_close: wrong op_type *'):
            cs.add_close(o)

        o = Ops('CLOSE', wrong_cursor, 'c=0,e=4,dep=0,type=3,tim=5793512315335', fname, 9)
        with self.assertRaisesRegex(BaseException, 'add_close: got cursor *'):
            cs.add_close(o)
    def test_merge(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 9)
        cs.add_parse(o)

        o = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 1)
        cs.add_exec(o)

        o = Ops('WAIT', cursor, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 blocks=1 obj#=89440 tim=5793512314261", fname, 9)
        cs.add_wait(o)

        o = Ops('FETCH', cursor, 'c=475,e=474,p=1,cr=4,cu=0,mis=0,r=10,dep=0,og=1,plh=2725028981,tim=5793512314300', fname, 9)
        cs.add_fetch(o)

        o = Ops('CLOSE', cursor, 'c=0,e=4,dep=0,type=3,tim=5793512315335', fname, 8)
        cs.add_close(o)

        m = cs.merge()
        self.assertEqual(m.c, 621)
        self.assertEqual(m.e, 1027)

        cs.parse = None
        m = cs.merge()
        self.assertEqual(m.c, 548)
        self.assertEqual(m.e, 954)

        cs.close = None
        m = cs.merge()
        self.assertEqual(m.c, 548)
        self.assertEqual(m.e, 950)
    def test_get_elapsed(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 9)
        cs.add_parse(o)
        o = Ops('CLOSE', cursor, 'c=0,e=4,dep=0,type=3,tim=5793512315335', fname, 7)
        cs.add_close(o)
        ela = cs.get_elapsed()
        self.assertEqual(ela, 484501)

        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', fname, 8)
        cs.add_parse(o)
        o = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511831834', fname, 7)
        cs.add_exec(o)
        o = Ops('CLOSE', cursor, 'c=0,e=4,dep=0,type=3,tim=5793512315335', fname, 9)
        cs.add_close(o)
        ela = cs.get_elapsed()
        self.assertEqual(ela, 484501)

        cs.parse = None
        ela = cs.get_elapsed()
        self.assertEqual(ela, 483501)

        cs.close = None
        ela = cs.get_elapsed()
        self.assertEqual(ela, None)

    def test_dump_to_db(self):
        cs = CurrentStatement(cursor, None)
        with self.assertRaisesRegex(BaseException, 'dump_to_db: database not set!'):
            cs.dump_to_db()
if __name__ == '__main__':
    unittest.main()
