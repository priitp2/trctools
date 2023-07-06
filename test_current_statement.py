import unittest
from current_statement import CurrentStatement
from ops import Ops

CURSOR = '#123'
WRONG_CURSOR = '#321'
FNAME = 'trace.trc'
class TestCurrentStatement(unittest.TestCase):
    def setUp(self):
        self.cstat = CurrentStatement(CURSOR, None)
        self.happy_ops = {
                'PARSE':Ops('PARSE', CURSOR, 
                    'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 0),
                'EXEC':Ops('EXEC', CURSOR,
                    'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 2),
                'CLOSE':Ops('CLOSE', CURSOR, 'c=0,e=4,dep=0,type=3,tim=5793512315335', FNAME, 8),
                'STAT':Ops('STAT', CURSOR, "id=1 cnt=1 pid=0 pos=1 obj=89434 op='TABLE ACCESS BY INDEX ROWID " \
                        + "CUSTOMER_SEGMENT (cr=5 pr=0 pw=0 str=1 time=173 us cost=4 size=103 card=1)'", FNAME, 8),
                'WAIT': Ops('WAIT', CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 " \
                        + "blocks=1 obj#=89440 tim=5793512314261", FNAME, 3)
                }
    def test_add_parse(self):
        cs = CurrentStatement(CURSOR, None)
        ops = Ops('PARSE', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 0)

        cs.add_parse(ops)
        with self.assertRaisesRegex(BaseException, 'add_parse: already set!'):
            cs.add_parse(ops)

        cs = CurrentStatement(CURSOR, None)
        ops = Ops('EXEC', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 2)
        with self.assertRaisesRegex(BaseException, 'add_parse: wrong op_type *'):
            cs.add_parse(ops)

        ops = Ops('PARSE', WRONG_CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 9)
        with self.assertRaisesRegex(BaseException, 'add_parse: got cursor *'):
            cs.add_parse(ops)

    def test_add_exec(self):
        cs = CurrentStatement(CURSOR, None)
        ops = Ops('EXEC', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 9)

        cs.add_exec(ops)
        with self.assertRaisesRegex(BaseException, 'add_exec: already set!'):
            cs.add_exec(ops)

        cs = CurrentStatement(CURSOR, None)
        ops = Ops('PARSE', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 8)
        with self.assertRaisesRegex(BaseException, 'add_exec: wrong op_type *'):
            cs.add_exec(ops)

        ops = Ops('EXEC', WRONG_CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 9)
        with self.assertRaisesRegex(BaseException, 'add_exec: got cursor *'):
            cs.add_exec(ops)

    def test_add_wait(self):
        cs = CurrentStatement(CURSOR, None)
        ops = Ops('PARSE', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 9)
        with self.assertRaisesRegex(BaseException, 'add_wait: wrong op_type *'):
            cs.add_wait(ops)

        ops = Ops('WAIT', WRONG_CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 blocks=1 obj#=89440 tim=5793512314261", FNAME, 3)
        with self.assertRaisesRegex(BaseException, 'add_wait: got cursor *'):
            cs.add_wait(ops)

    def test_add_fetch(self):
        cs = CurrentStatement(CURSOR, None)
        ops = Ops('PARSE', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 8)
        with self.assertRaisesRegex(BaseException, 'add_fetch: wrong op_type *'):
            cs.add_fetch(ops)

        ops = Ops('FETCH', WRONG_CURSOR, 'c=475,e=474,p=1,cr=4,cu=0,mis=0,r=10,dep=0,og=1,plh=2725028981,tim=5793512314300', FNAME, 2)
        with self.assertRaisesRegex(BaseException, 'add_fetch: got cursor *'):
            cs.add_fetch(ops)

    def test_add_close(self):
        cs = CurrentStatement(CURSOR, None)
        ops = Ops('CLOSE', CURSOR, 'c=0,e=4,dep=0,type=3,tim=5793512315335', FNAME, 8)

        cs.add_close(ops)
        #with self.assertRaisesRegex(BaseException, 'add_close: already set!'):
        #    cs.add_close(o)
        cs.add_close(ops)
        self.assertEqual(cs.close.c, 0)
        self.assertEqual(cs.close.e, 8)

        cs = CurrentStatement(CURSOR, None)
        ops = Ops('PARSE', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 7)
        with self.assertRaisesRegex(BaseException, 'add_close: wrong op_type *'):
            cs.add_close(ops)

        ops = Ops('CLOSE', WRONG_CURSOR, 'c=0,e=4,dep=0,type=3,tim=5793512315335', FNAME, 9)
        with self.assertRaisesRegex(BaseException, 'add_close: got cursor *'):
            cs.add_close(ops)
    def test_add_stat(self):
        cs = CurrentStatement(CURSOR, None)
        ops = Ops('STAT', CURSOR, "id=1 cnt=1 pid=0 pos=1 obj=89434 op='TABLE ACCESS BY INDEX ROWID CUSTOMER_SEGMENT (cr=5 pr=0 pw=0 str=1 time=173 us cost=4 size=103 card=1)'", FNAME, 8)
        cs.add_stat(ops)
        self.assertEqual(len(cs.stat), 1)

        ops = Ops('PARSE', CURSOR, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 7)
        with self.assertRaisesRegex(BaseException, 'add_stat: wrong op_type *'):
            cs.add_stat(ops)

        ops = Ops('STAT', WRONG_CURSOR, "id=1 cnt=1 pid=0 pos=1 obj=89434 op='TABLE ACCESS BY INDEX ROWID CUSTOMER_SEGMENT (cr=5 pr=0 pw=0 str=1 time=173 us cost=4 size=103 card=1)'", FNAME, 8)
        with self.assertRaisesRegex(BaseException, 'add_stat: got cursor *'):
            cs.add_stat(ops)

    def test_dump_to_db(self):
        with self.assertRaisesRegex(BaseException, 'dump_to_db: database not set!'):
            self.cstat.dump_to_db()
    def test_is_not_empty(self):
        self.assertFalse(cs.is_not_empty())
        self.cstat.add_stat(self.happy_ops['STAT'])
        self.assertTrue(self.cstat.is_not_empty())
    def test_add_ops(self):
        for ops in self.happy_ops:
            self.cstat.add_ops(self.happy_ops[ops])
        stat = self.happy_ops['STAT']
        self.cstat.add_ops(stat)
        wait = self.happy_ops['WAIT']
        self.cstat.add_ops(wait)

        self.assertTrue(self.cstat.is_set('PARSE'))
        self.assertTrue(self.cstat.is_set('EXEC'))
        self.assertTrue(self.cstat.is_set('CLOSE'))
        self.assertTrue(self.cstat.is_set('STAT'))
        self.assertTrue(self.cstat.is_set('WAIT'))

        self.assertFalse(self.cstat.is_set('BINDS'))
        self.assertFalse(self.cstat.is_set('PIC'))

        wrong_cs = Ops('WAIT', WRONG_CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 ", FNAME, 3)
        with self.assertRaisesRegex(BaseException, 'add_ops: wrong cursor *'):
            self.cstat.add_ops(wrong_cs)

        wrong_ops = Ops('STAR', CURSOR, " ", FNAME, 3) 
        with self.assertRaisesRegex(BaseException, 'add_ops: unknown ops type:*'):
            self.cstat.add_ops(wrong_ops)

        ops = self.happy_ops['EXEC']
        with self.assertRaisesRegex(BaseException, 'add_ops: already set: *'):
            self.cstat.add_ops(ops)

    def test_is_set(self):
        self.assertFalse(self.cstat.is_set('EXEC'))

if __name__ == '__main__':
    unittest.main()
