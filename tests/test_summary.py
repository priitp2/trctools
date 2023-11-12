import unittest
import datetime
import summary

class TestSummary(unittest.TestCase):
    def test_create_preds(self):
        filters = {}
        preds = summary.create_preds(filters)
        self.assertEqual(preds, '')

        filters['start'] = ''
        preds = summary.create_preds(filters)
        self.assertRegex(preds, 'ts >= TIMESTAMP')

        filters['client_id'] = 'superdry'
        preds = summary.create_preds(filters)
        self.assertRegex(preds, "client_id = 'superdry'")

if __name__ == '__main__':
    unittest.main()
