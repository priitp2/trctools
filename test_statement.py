import unittest

from statement import Statement

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"

class TestStatement(unittest.TestCase):
    def test_init(self):
        s = Statement(cursor, params, False)

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

        s = Statement(cursor, '', False)
        with self.assertRaises(AttributeError):
            self.assertEqual(s.statement_length, '80')
    def test_normality(self):
        s = Statement(cursor, params, False)
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

        s = Statement(cursor, params, True)
        s.record_exec_cpu(1)
        s.record_exec_elapsed(1)

        self.assertEqual(len(s.exec_cpu), 1)
        self.assertEqual(len(s.exec_elapsed), 1)

        self.assertEqual(s.exec_hist_elapsed.total_count, 1)
        self.assertEqual(s.exec_hist_cpu.total_count, 1)
    def test_increment(self):
        s = Statement(cursor, params, False)
        s.increase_exec_count()
        s.increase_fetch_count()

        self.assertEqual(s.execs, 1)
        self.assertEqual(s.fetches, 1)


if __name__ == '__main__':
    unittest.main()

