import unittest
from mts.core.id import ObjectId, Service
from mts.commons.const import *
from mts.commons import logger


class TestObjectId(unittest.TestCase):
    def test_default(self):
        a = ObjectId()
        b = ObjectId(a)
        c = ObjectId(str(a))
        self.assertTrue(a == b)
        self.assertTrue(a == c)
        service_code, last_ts, pid_code, sequence = ObjectId.unpack('a4059507fd30cfff')
        ts = ObjectId.timestamp(last_ts)
        d = ObjectId('a4059507fd30cfff')
        e = ObjectId.pack(service_code, ts)
        ObjectId._pid = ObjectId._pid - 1
        f = ObjectId.pack(service_code, ts)
        self.assertEqual(pid_code, ObjectId.unpack(str(d))[2])
        self.assertNotEqual(ObjectId.unpack(e)[2], ObjectId.unpack(f)[2])

    def test_comp(self):
        a = ObjectId()
        b = ObjectId()
        self.assertTrue(a < b)

    def test_register_01(self):
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

    def test_register_02(self):
        ObjectId.register({'epoch': 0})
        service_code, last_ts, pid_code, sequence = ObjectId.unpack('a4059507fd30cfff')
        ts_01 = ObjectId.timestamp(last_ts)
        ObjectId.register({'epoch': EPOCH_DEFAULT})
        service_code, last_ts, pid_code, sequence = ObjectId.unpack('a4059507fd30cfff')
        ts_02 = ObjectId.timestamp(last_ts)
        self.assertEqual((ts_02.unix() - ts_01.unix()) * 1000, EPOCH_DEFAULT)

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

    def test_error_unpack_et(self):
        with self.assertRaises(TypeError):
            ObjectId.unpack([])

    def test_error_pack_ts(self):
        with self.assertRaises(ValueError):
            ts = moment('2019-11-21')
            ObjectId.pack(41, ts)

    def test_validate(self):
        self.assertFalse(ObjectId.validate('900616afc99d6000'))
        self.assertTrue(ObjectId.validate('a00616afc99d6000'))

    def test_eq(self):
        oid = ObjectId()
        self.assertFalse(oid == 2)

    def test_ne_01(self):
        oid = ObjectId()
        self.assertTrue(oid != 2)

    def test_ne_02(self):
        a = ObjectId()
        b = ObjectId()
        self.assertTrue(a != b)

    def test_lt_01(self):
        oid = ObjectId()
        with self.assertRaises(TypeError):
            oid < 2

    def test_le_01(self):
        a = ObjectId()
        b = ObjectId()
        self.assertTrue(a <= b)

    def test_le_02(self):
        a = ObjectId()
        b = ObjectId(a)
        self.assertTrue(a <= b)

    def test_le_03(self):
        oid = ObjectId()
        with self.assertRaises(TypeError):
            oid <= 2

    def test_gt_01(self):
        a = ObjectId()
        b = ObjectId()
        self.assertTrue(b > a)

    def test_gt_02(self):
        oid = ObjectId()
        with self.assertRaises(TypeError):
            oid > 2

    def test_ge_01(self):
        a = ObjectId()
        b = ObjectId()
        self.assertTrue(b >= a)

    def test_ge_02(self):
        a = ObjectId()
        b = ObjectId(a)
        self.assertTrue(b >= a)

    def test_ge_03(self):
        oid = ObjectId()
        with self.assertRaises(TypeError):
            oid >= 2

    def test_sid(self):
        ObjectId.register({'service_code': 43})
        oid = ObjectId()
        self.assertEqual(Service.to_service_id(43), oid.sid)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
