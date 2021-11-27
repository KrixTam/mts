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

    def test_error_construct_parameter_01(self):
        with self.assertRaises(ValueError):
            DataDictionaryId(id='a00548d4e5abb001', dd_type=0)

    def test_error_construct_parameter_02(self):
        with self.assertRaises(ValueError):
            DataDictionaryId(a=123212123)

    def test_error_unpack_01(self):
        with self.assertRaises(TypeError):
            DataDictionaryId.unpack('1231123')

    def test_error_unpack_02(self):
        with self.assertRaises(ValueError):
            DataDictionaryId.unpack('aa00548d4e5abb002')

    def test_error_pack(self):
        with self.assertRaises(ValueError):
            DataDictionaryId.pack(dd_type=11, oid=int('a00548d4e5abb002', 16))

    def test_eq_01(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid=a)
        self.assertTrue(a == b)

    def test_eq_02(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        self.assertFalse(a == 123)

    def test_ne_01(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='3a00548d4e5abb001')
        self.assertTrue(a != b)

    def test_ne_02(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        self.assertTrue(a != 123)

    def test_lt_01(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='2a00548d4e5abb002')
        self.assertTrue(a < b)

    def test_lt_02(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        with self.assertRaises(TypeError):
            a < 123

    def test_gt_01(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='2a00548d4e5abb002')
        self.assertTrue(b > a)

    def test_gt_02(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        with self.assertRaises(TypeError):
            a > 123

    def test_le_01(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='2a00548d4e5abb001')
        self.assertTrue(a <= b)

    def test_le_02(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='2a00548d4e5abb002')
        self.assertTrue(a <= b)

    def test_le_03(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        with self.assertRaises(TypeError):
            a <= 123

    def test_ge_01(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='2a00548d4e5abb001')
        self.assertTrue(b >= a)

    def test_ge_02(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        b = DataDictionaryId(ddid='2a00548d4e5abb002')
        self.assertTrue(b >= a)

    def test_gt_03(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        with self.assertRaises(TypeError):
            a >= 123


if __name__ == '__main__':
    unittest.main()
