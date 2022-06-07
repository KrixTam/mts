import unittest
from mts.commons.const import *
from mts.core.id import DataDictionaryId, Service


class TestDataDictionaryId(unittest.TestCase):
    def test_default(self):
        a = DataDictionaryId(ddid='2a00548d4e5abb001')
        self.assertEqual(a.oid, 'a00548d4e5abb001')
        self.assertEqual(a.dd_type, DD_TYPE_METRIC)
        b = DataDictionaryId(dd_type=DD_TYPE_OWNER, oid=int('a00548d4e5abb002', 16))
        self.assertEqual(b.oid, 'a00548d4e5abb002')
        self.assertEqual(b.dd_type, DD_TYPE_OWNER)
        self.assertEqual(a.__repr__().split('\n')[0], 'DataDictionaryId(2a00548d4e5abb001)')
        self.assertNotEqual(a.__repr__().split('\n')[0], b.__repr__().split('\n')[0])

    def test_init_01(self):
        a = DataDictionaryId(dd_type=DD_TYPE_OWNER, service_code=43)
        b = DataDictionaryId(dd_type=DD_TYPE_OWNER, service_id='53')
        self.assertNotEqual(a, b)
        self.assertEqual(bin(a.value)[2:][:10], bin(b.value)[2:][:10])

    def test_init_02(self):
        with self.assertRaises(ValueError):
            a = DataDictionaryId(dd_type=DD_TYPE_OWNER)

    def test_init_03(self):
        with self.assertRaises(ValueError):
            a = DataDictionaryId(dd_type=1)

    def test_init_04(self):
        with self.assertRaises(ValueError):
            a = DataDictionaryId()

    def test_init_05(self):
        with self.assertRaises(ValueError):
            a = DataDictionaryId(dd_type=1, service_code=43)

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

    def test_sid(self):
        a = DataDictionaryId(dd_type=DD_TYPE_OWNER, service_code=43)
        self.assertEqual(Service.to_service_id(43), a.sid)

    def test_validate(self):
        self.assertTrue(DataDictionaryId.validate(int('2a00548d4e5abb001', 16)))


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
