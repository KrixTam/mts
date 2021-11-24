import unittest
from mts.core import ObjectId
from mts.const import *
from mts.utils import logger


class TestObjectId(unittest.TestCase):
    def test_default(self):
        a = ObjectId()
        b = ObjectId(a)
        c = ObjectId(str(a))
        self.assertTrue(a == b)
        self.assertTrue(a == c)
        service_code, last_ts, pid_code, sequence = ObjectId.unpack('a4059507fd30c005')
        ts = ObjectId.timestamp(last_ts)
        d = ObjectId.pack(service_code, ts, sequence + 1)
        self.assertEqual(pid_code, ObjectId.unpack(d)[2])

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

    def test_error_service_code(self):
        with self.assertRaises(ValueError):
            ObjectId(service_code=SERVICE_CODE_MIN-1)
        with self.assertRaises(ValueError):
            ObjectId(service_code=SERVICE_CODE_MAX+1)

    def test_error_oid(self):
        with self.assertRaises(TypeError):
            ObjectId(123)

    def test_error_unpack_time(self):
        with self.assertRaises(ValueError):
            sc = 40
            ts = 0
            pid_code = ObjectId._generate_pid_code()
            new_id = (sc << SERVICE_CODE_BITS_SHIFT) | (ts << TIMESTAMP_BITS_SHIFT) | (pid_code << PID_CODE_BITS_SHIFT) | 0
            logger.log(new_id)
            ObjectId.unpack(new_id)

    def test_error_unpack_sc(self):
        with self.assertRaises(ValueError):
            ObjectId.unpack(int('900616afc99d6000', 16))

    def test_validate(self):
        self.assertFalse(ObjectId.validate('900616afc99d6000'))
        self.assertTrue(ObjectId.validate('a00616afc99d6000'))


if __name__ == '__main__':
    unittest.main()
