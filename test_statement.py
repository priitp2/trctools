import unittest

from statement import Statement
from current_statement import CurrentStatement
from ops import Ops

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"

class TestStatement(unittest.TestCase):
    def test_init(self):
        s = Statement(cursor, params, False, None)

        self.assertEqual(s.cursor, cursor)

        self.assertEqual(s.statement_length, '80')
        self.assertEqual(s.rec_depth, '0')
        self.assertEqual(s.schema_uid, '331')
        self.assertEqual(s.command_type, '3')
        self.assertEqual(s.priv_user_id, '331')
        self.assertEqual(s.timestamp, '1648763822995')
        self.assertEqual(s.hash_id, '1167462720')
        self.assertEqual(s.address, '8ff705c50')
        self.assertEqual(s.sql_id, '6v48b7j2tc4a0')

        s = Statement(cursor, '', False, None)
        with self.assertRaises(AttributeError):
            self.assertEqual(s.statement_length, '80')
    def test_normality(self):
        # FIXME: this needs proper test case
        s = Statement(cursor, params, False, None)
        s.record_exec_cpu(1)
        s.record_exec_elapsed(1)

        with self.assertRaises(AttributeError):
            self.assertEqual(len(s.exec_cpu), 0)
        with self.assertRaises(AttributeError):
            self.assertEqual(len(s.exec_elapsed), 0)
        with self.assertRaises(AttributeError):
            self.assertEqual(len(s.fetch_cpu), 0)
        with self.assertRaises(AttributeError):
            self.assertEqual(len(s.fetch_elapsed), 0)

        s = Statement(cursor, params, True, None)
        s.record_exec_cpu(1)
        s.record_exec_elapsed(1)

        self.assertEqual(len(s.exec_cpu), 1)
        self.assertEqual(len(s.exec_elapsed), 1)

        self.assertEqual(s.exec_hist_elapsed.total_count, 1)
        self.assertEqual(s.exec_hist_cpu.total_count, 1)
    def test_increment(self):
        # FIXME: increase* methods are unused
        s = Statement(cursor, params, False, None)
        s.increase_exec_count()
        s.increase_fetch_count()

        self.assertEqual(s.execs, 1)
        self.assertEqual(s.fetches, 1)
    def test_add_current_statement(self):
        s = Statement(cursor, params, False, None)
        cs = CurrentStatement(cursor, None)

        cs.add_parse(Ops('PARSE', cursor, 'c=33,e=33,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830706'))
        cs.add_exec(Ops('EXEC', cursor, 'c=73,e=73,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=2725028981,tim=5793511830834'))
        cs.add_fetch(Ops('FETCH', cursor, 'c=444,e=444,p=1,cr=4,cu=0,mis=0,r=10,dep=0,og=1,plh=2725028981,tim=5793511831311'))
        cs.add_fetch(Ops('FETCH', cursor, 'c=0,e=45,p=0,cr=1,cu=0,mis=0,r=4,dep=0,og=1,plh=2725028981,tim=5793511831594'))
        cs.add_wait(Ops('WAIT', cursor, " nam='db file sequential read' ela= 343 file#=414 block#=2090520 blocks=1 obj#=89440 tim=5793511831255"))
        cs.add_wait(Ops('WAIT', cursor, " nam='SQL*Net message from client' ela= 186 driver id=675562835 #bytes=1 p3=0 obj#=89440 tim=5793511831524"))
        cs.add_close(Ops('CLOSE', cursor, 'c=3,e=3,dep=0,type=1,tim=5793511831927'))

        # Calculated manually from the operations
        elapsed = 1127
        cpu = 553
        # close.ela - parse.ela
        ela_diff = 1221
        ela_nowait = 598

        s.add_current_statement(cs)

        self.assertEqual(s.exec_hist_cpu.total_count, 1)
        self.assertEqual(s.exec_hist_elapsed.total_count, 1)
        self.assertEqual(s.exec_hist_elapsed.max_value, elapsed)
        self.assertEqual(s.exec_hist_cpu.max_value, cpu)
        self.assertEqual(s.resp_hist.max_value, ela_diff)
        self.assertEqual(s.resp_without_waits_hist.max_value, ela_nowait)
if __name__ == '__main__':
    unittest.main()

