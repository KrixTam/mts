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


if __name__ == '__main__':
    unittest.main()
