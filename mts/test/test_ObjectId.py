import unittest
from mts.core import ObjectId
from mts.utils import logger


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
        ObjectId.register({'service_code': 41})
        c = ObjectId()
        d = ObjectId(service_code=40)
        self.assertNotEqual(a.__repr__().split('\n')[1], c.__repr__().split('\n')[1])
        self.assertEqual(a.__repr__().split('\n')[1], d.__repr__().split('\n')[1])
        b = ObjectId(a)
        logger.log(a.__repr__())
        logger.log(b.__repr__())
        logger.log(c.__repr__())
        logger.log(d.__repr__())
        self.assertTrue(a == b)


if __name__ == '__main__':
    unittest.main()
