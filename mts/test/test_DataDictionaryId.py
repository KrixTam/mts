import unittest
from mts.const import *
from mts.core import DataDictionaryId
from mts.utils import logger


class TestDataDictionaryId(unittest.TestCase):
    def test_default(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        self.assertEqual(a.oid, 'a00548d4e5abb001')
        self.assertEqual(a.dd_type, DD_TYPE_METRIC)
        b = DataDictionaryId(dd_type=DD_TYPE_OWNER, oid=int('a00548d4e5abb002', 16))
        self.assertEqual(b.oid, 'a00548d4e5abb002')
        self.assertEqual(b.dd_type, DD_TYPE_OWNER)
        logger.log(a.__repr__())
        logger.log(b.__repr__())

    def test_error_construct_ddid_type(self):
        with self.assertRaises(TypeError):
            DataDictionaryId(ddid=['a00548d4e5abb001'])

    def test_error_construct_parameter(self):
        with self.assertRaises(ValueError):
            DataDictionaryId(id='a00548d4e5abb001', dd_type=0)


if __name__ == '__main__':
    unittest.main()
