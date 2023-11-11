import unittest
import datetime
import summary

class TestSummary(unittest.TestCase):
    def test_create_time_predicate(self):
        pred = summary.create_time_predicate(None, None)
        self.assertEqual(pred, '')

if __name__ == '__main__':
    unittest.main()
