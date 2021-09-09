import unittest
from mts.core import ObjectId


class TestObjectId(unittest.TestCase):
    def test_default(self):
        a = ObjectId()
        b = ObjectId(a)
        c = ObjectId(str(a))
        self.assertTrue(a == b)
        self.assertTrue(a == c)

    def test_comp(self):
        a = ObjectId()
        b = ObjectId()
        self.assertTrue(a < b)

    def test_register(self):
        a = ObjectId()
        ObjectId.register({'service_code': 4})
        c = ObjectId()
        d = ObjectId(service_code=0)
        self.assertNotEqual(a.__repr__().split('\n')[1], c.__repr__().split('\n')[1])
        self.assertEqual(a.__repr__().split('\n')[1], d.__repr__().split('\n')[1])
        b = ObjectId(a)
        print(a.__repr__())
        print(b.__repr__())
        print(c.__repr__())
        print(d.__repr__())
        self.assertTrue(a == b)


if __name__ == '__main__':
    unittest.main()
