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

        ret = util.merge_lat_objects(obj3, ())
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], obj3[1])
        self.assertEqual(ret[2], obj3[2])

        with self.assertRaises(TypeError):
            ret = util.merge_lat_objects(obj3, 666)

        ret = util.merge_lat_objects(obj3, (cursor1, -1))
        self.assertEqual(ret[0], cursor1)
        self.assertEqual(ret[1], obj3[1])
        self.assertEqual(ret[2], obj3[2])

        ret = util.merge_lat_objects(None, (None, obj3[1], obj3[2]))
        self.assertEqual(ret[0], None)
        self.assertEqual(ret[1], obj3[1])
        self.assertEqual(ret[2], obj3[2])
    def test_split_event(self):
        ev = 'c=143,e=143,p=0,cr=0,cu=0,mis=0,r=0,dep=0,og=1,plh=1164377159,tim=5185921053409'

        out = util.split_event('')
        self.assertEqual(len(out), 0)

        out = util.split_event('xxx yyyy')
        self.assertEqual(len(out), 0)

        out = util.split_event(ev)
        self.assertEqual(len(out), 11)
        self.assertEqual(out['og'], '1')

if __name__ == '__main__':
    unittest.main()
