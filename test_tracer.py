import unittest
from tracer import Tracer

class TestTracer(unittest.TestCase):
    def test_init(self):
        t = Tracer(True)
    def test_handle_parse(self):
        t = Tracer(True)

        with self.assertRaisesRegex(AttributeError, 'sql_id'):
            t.handle_parse('', '')

        t.handle_parse('666', 'sqlid=666 fubar = xxx')

        self.assertEqual(len(t.statements), 1)

if __name__ == '__main__':
    unittest.main()
