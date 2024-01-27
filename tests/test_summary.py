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

        filters['sql_id'] = 'xxx'
        preds = summary.create_preds(filters)
        self.assertRegex(preds, "sql_id in \('xxx'\)")

        filters['sql_id'] = 'xxx,yyy'
        preds = summary.create_preds(filters)
        self.assertRegex(preds, "sql_id in \('xxx','yyy'\)")
if __name__ == '__main__':
    unittest.main()
