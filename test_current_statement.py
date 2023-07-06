import unittest
from current_statement import CurrentStatement
from ops import Ops
from test_db import DB

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
    def test_dump_to_db(self):
        with self.assertRaisesRegex(BaseException, 'dump_to_db: database not set!'):
            self.cstat.dump_to_db()

        self.cstat.dbs = DB()
        for ops in self.happy_ops:
            self.cstat.add_ops(self.happy_ops[ops])
        self.cstat.dump_to_db()
        self.assertEqual(len(self.cstat.dbs.batches), len(self.happy_ops))
    def test_is_not_empty(self):
        self.assertFalse(self.cstat.is_not_empty())
        self.cstat.add_ops(self.happy_ops['EXEC'])
        self.assertTrue(self.cstat.is_not_empty())

        self.cstat = CurrentStatement(CURSOR, None)
        wait = self.happy_ops['WAIT']
        for i in range(0, 3):
            self.cstat.add_ops(wait)
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

    def test_count_ops(self):
        self.assertEqual(self.cstat.count_ops('FETCH'), 0)

        ops = self.happy_ops['EXEC']
        self.cstat.add_ops(ops)
        self.assertEqual(self.cstat.count_ops('EXEC'), 1)

        wait = self.happy_ops['WAIT']
        for i in range(0, 3):
            self.cstat.add_ops(wait)
        self.assertEqual(self.cstat.count_ops('WAIT'), 3)
if __name__ == '__main__':
    unittest.main()
