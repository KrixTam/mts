import unittest
from mts.core import DataDictionaryId


class TestDataDictionaryId(unittest.TestCase):
    def test_default(self):
        a = DataDictionaryId(ddid='a000548d4e5abb001')
        self.assertEqual(a.oid, int('000548d4e5abb001', 16))
        self.assertEqual(a.dd_type, int('a', 16))
        b = DataDictionaryId(dd_type=1, oid=int('000548d4e5abb002', 16))
        self.assertEqual(b.oid, int('000548d4e5abb002', 16))
        self.assertEqual(b.dd_type, 1)

    def test_TypeError(self):
        try:
            DataDictionaryId(ddid='a00548d4e5abb001')
        except TypeError:
            pass
        except Exception:
            self.fail('unexpected exception raised')
        else:
            self.fail('ExpectedException not raised')

    def test_ValueError(self):
        try:
            DataDictionaryId(oid='a00548d4e5abb001', dd_type=0)
        except TypeError:
            pass
        except Exception:
            self.fail('unexpected exception raised')
        else:
            self.fail('ExpectedException not raised')


if __name__ == '__main__':
    unittest.main()
