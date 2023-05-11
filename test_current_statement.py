import unittest
from current_statement import CurrentStatement
from ops import Ops

cursor = '#123'
wrong_cursor = '#321'
class TestCurrentStatement(unittest.TestCase):
    def test_add_parse(self):
        cs = CurrentStatement(cursor, None)
        o = Ops('PARSE', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834')

        cs.add_parse(o)
        with self.assertRaisesRegex(BaseException, 'add_parse: already set!'):
            cs.add_parse(o)

        cs = CurrentStatement(cursor, None)
        o = Ops('EXEC', cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834')
        with self.assertRaisesRegex(BaseException, 'add_parse: wrong op_type *'):
            cs.add_parse(o)

        o = Ops('PARSE', wrong_cursor, 'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834')
        with self.assertRaisesRegex(BaseException, 'add_parse: got cursor *'):
            cs.add_parse(o)
if __name__ == '__main__':
    unittest.main()
