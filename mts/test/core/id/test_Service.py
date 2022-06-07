import unittest
from mts.core.id import Service
from mts.commons.const import *


class TestServiceId(unittest.TestCase):
    def test_init_01(self):
        service = Service()
        self.assertEqual(SERVICE_CODE_MIN, service.code)

    def test_init_02(self):
        sid = '53'
        service = Service(sid)
        self.assertEqual(sid, service.id)

    def test_init_03(self):
        sid = '53'
        service = Service(int(sid, 8))
        self.assertEqual(sid, service.id)

    def test_set(self):
        sid = '53'
        service = Service()
        service.set(sid)
        self.assertEqual(sid, service.id)

    def test_error_01(self):
        sid = '53'
        service = Service(sid)
        service.set('13')
        self.assertEqual(sid, service.id)

    def test_error_02(self):
        with self.assertRaises(TypeError):
            Service.to_service_code([])

    def test_eq_01(self):
        a = Service()
        b = Service()
        self.assertTrue(a == b)

    def test_eq_02(self):
        a = Service()
        self.assertFalse(a == 123)

    def test_ne_01(self):
        a = Service()
        b = Service('53')
        self.assertTrue(a != b)

    def test_ne_02(self):
        a = Service()
        self.assertTrue(a != 123)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
