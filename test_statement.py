import unittest

from statement import Statement
from current_statement import CurrentStatement
from ops import Ops

cursor = '#140131077570528'
params = "len=80 dep=0 uid=331 oct=3 lid=331 tim=1648763822995 hv=1167462720 ad='8ff705c50' sqlid='6v48b7j2tc4a0'"
fname = 'trace.trc'

class TestStatement(unittest.TestCase):
    def test_init(self):
        s = Statement(cursor, params)

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

        s = Statement(cursor, '')
        with self.assertRaises(AttributeError):
            self.assertEqual(s.statement_length, '80')

if __name__ == '__main__':
    unittest.main()
