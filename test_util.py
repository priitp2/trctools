import unittest
import util

cursor1 = '#123223'
class TestUtil(unittest.TestCase):
    def test_merge_lat_objects(self):
        obj1 = (cursor1, 0, 0)
        obj2 = (cursor1, 0, 0)
        obj3 = (cursor1, 1, 2)

        ret = util.merge_lat_objects(obj1, obj2)
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 0)
        self.assertEqual(ret[2], 0)

        ret = util.merge_lat_objects(obj1, obj3)
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 1)
        self.assertEqual(ret[2], 2)

        with self.assertRaises(BaseException):
            ret = util.merge_lat_objects(obj3, ('123', 0, 0))

        ret = util.merge_lat_objects(obj3, None)
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 1)
        self.assertEqual(ret[2], 2)

        ret = util.merge_lat_objects(obj3, [])
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 1)
        self.assertEqual(ret[2], 2)

        ret = util.merge_lat_objects([cursor1, -1], obj3)
        self.assertEqual(ret, None)

        ret = util.merge_lat_objects(obj1, [obj3, obj3])
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], 2)
        self.assertEqual(ret[2], 4)

if __name__ == '__main__':
    unittest.main()
