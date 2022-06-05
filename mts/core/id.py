from os import getpid
from random import getrandbits
import threading
from mts.commons import logger
from mts.commons.const import *


class Service(object):
    __slots__ = ('code', )

    def __init__(self, value=None):
        if value is None:
            self.code = SERVICE_CODE_MIN
        else:
            self.set(value)

    def set(self, value):
        service_code = Service.to_service_code(value)
        if Service.valid(service_code):
            self.code = service_code
        else:
            logger.warning([6001, service_code, SERVICE_CODE_MIN, SERVICE_CODE_MAX])

    @property
    def id(self):
        return Service.to_service_id(self.code)

    @staticmethod
    def to_service_id(value: int):
        return oct(value)[2:]

    @staticmethod
    def to_service_code(value):
        if isinstance(value, int):
            service_code = value
        else:
            if isinstance(value, str):
                service_code = int(value, 8)
            else:
                raise TypeError(logger.warning([6000]))
        return service_code

    @staticmethod
    def valid(service_code):
        if (service_code >= SERVICE_CODE_MIN) and (service_code <= SERVICE_CODE_MAX):
            return True
        else:
            return False

    def __eq__(self, other):
        if isinstance(other, Service):
            return self.code == other.code
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, Service):
            return self.code != other.code
        return NotImplemented


class ObjectId(object):
    _pid = getpid()
    _lock = threading.Lock()
    _epoch = EPOCH_DEFAULT
    _service = Service()
    _pid_code = getrandbits(4)
    _last_ts = None
    _sequence = 0
    __slots__ = ('_id', '_sc',)

    def __init__(self, oid=None, service_code: int = None):
        self._sc = None
        if service_code is None:
            self._sc = ObjectId._service.code
        else:
            if Service.valid(service_code):
                self._sc = service_code
            else:
                raise ValueError(logger.error([5500, service_code, SERVICE_CODE_MIN, SERVICE_CODE_MAX]))
        if oid is None:
            self._id = ObjectId.pack(self._sc)
        else:
            if isinstance(oid, str) and len(oid) == OID_LEN:
                self._generate(oid)
            else:
                if isinstance(oid, ObjectId):
                    self._generate(str(oid))
                else:
                    raise TypeError(logger.error([5501, type(oid)]))

    def __str__(self):
        return hex_str(self._id, OID_LEN)

    def __repr__(self):
        service_code, ts, pid_code, sequence = ObjectId.unpack(self._id)
        now = moment()
        gap = (now.unix() - ObjectId._epoch) * 1000 + now.milliseconds() - ts
        content = [str(self), service_code, now.subtract(gap, 'ms').format(), pid_code, sequence]
        return 'ObjectId({0})\nService Code: {1}\nTimestamp: {2}\nPID Code: {3}\nSequence No.: {4}\n'.format(*content)

    @classmethod
    def _generate_pid_code(cls):
        pid = getpid()
        if pid != cls._pid:
            cls._pid = pid
            cls._pid_code = getrandbits(4)
        return cls._pid_code

    @staticmethod
    def register(settings):
        """注册默认配置"""
        if 'epoch' in settings and isinstance(settings['epoch'], int):
            ObjectId._epoch = settings['epoch'] & TIMESTAMP_MASK
        if 'service_code' in settings and isinstance(settings['service_code'], int):
            ObjectId._service = Service(settings['service_code'] & SERVICE_CODE_MASK)

    @staticmethod
    def unpack(oid):
        oid_value = oid
        if isinstance(oid_value, int):
            pass
        else:
            if isinstance(oid_value, str) and len(oid_value) == OID_LEN:
                oid_value = int(oid, 16)
            else:
                raise TypeError(logger.error([5501, type(oid)]))
        service_code = oid_value >> SERVICE_CODE_BITS_SHIFT
        last_ts = (oid_value >> TIMESTAMP_BITS_SHIFT) & TIMESTAMP_MASK
        pid_code = (oid_value >> TIMESTAMP_BITS_SHIFT) & PID_CODE_MASK
        sequence = oid_value & SEQUENCE_MASK
        if last_ts <= 0:
            raise ValueError(logger.error([5502]))
        if Service.valid(service_code):
            return service_code, last_ts, pid_code, sequence
        else:
            raise ValueError(logger.error([5500, service_code, SERVICE_CODE_MIN, SERVICE_CODE_MAX]))

    @staticmethod
    def timestamp(ts: int):
        return EPOCH_MOMENT.add(ts + ObjectId._epoch - EPOCH_DEFAULT, 'ms')

    @staticmethod
    def pack(service_code: int, timestamp: moment = None, sn: int = None):
        ts = timestamp
        if ts is None:
            ts = moment()
        if ts.unix() < ObjectId._epoch:
            raise ValueError(logger.error([5502]))
        ts = (ts.unix() - ObjectId._epoch) * 1000 + ts.milliseconds()
        pid_code = ObjectId._generate_pid_code()
        sequence = sn
        with ObjectId._lock:
            if sn is None:
                if ts == ObjectId._last_ts:
                    ObjectId._sequence = (ObjectId._sequence + 1) & SEQUENCE_MASK
                    if ObjectId._sequence == 0:
                        ts = ts + 1
                else:
                    ObjectId._sequence = 0
                sequence = ObjectId._sequence
            else:
                ObjectId._sequence = sn
            ObjectId._last_ts = ts
        new_id = (service_code << SERVICE_CODE_BITS_SHIFT) | (ts << TIMESTAMP_BITS_SHIFT) | (pid_code << PID_CODE_BITS_SHIFT) | sequence
        return new_id

    def _generate(self, oid):
        service_code, last_ts, pid_code, sequence = ObjectId.unpack(oid)
        moment_last_ts = ObjectId.timestamp(last_ts)
        self._id = ObjectId.pack(service_code, moment_last_ts, sequence)

    @staticmethod
    def validate(oid):
        try:
            ObjectId.unpack(oid)
            return True
        except ValueError:
            return False

    @property
    def value(self):
        return self._id

    def __eq__(self, other):
        if isinstance(other, ObjectId):
            return self.value == other.value
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, ObjectId):
            return self.value != other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, ObjectId):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, ObjectId):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, ObjectId):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, ObjectId):
            return self.value >= other.value
        return NotImplemented


class DataDictionaryId(object):
    __slots__ = ('_id',)

    def __init__(self, **kwargs):
        if KEY_DDID in kwargs:
            ddid = kwargs[KEY_DDID]
            if isinstance(ddid, DataDictionaryId) or isinstance(ddid, str):
                self._generate(ddid)
            else:
                raise TypeError(logger.error([5600, type(ddid)]))
        else:
            if (KEY_OID in kwargs) and (KEY_DD_TYPE in kwargs):
                dd_type = kwargs[KEY_DD_TYPE]
                if isinstance(dd_type, str):
                    dd_type = int(dd_type, 16)
                self._id = DataDictionaryId.pack(dd_type, kwargs[KEY_OID])
            else:
                if KEY_DD_TYPE in kwargs:
                    sc = None
                    if KEY_SERVICE_CODE in kwargs:
                        sc = kwargs[KEY_SERVICE_CODE]
                    else:
                        if KEY_SERVICE_ID in kwargs:
                            sc = Service.to_service_code(kwargs[KEY_SERVICE_ID])
                    if sc is None:
                        raise ValueError(logger.error([5601, kwargs]))
                    else:
                        dd_type = kwargs[KEY_DD_TYPE]
                        if isinstance(dd_type, str) and dd_type in DD_TYPES:
                            dd_type = int(dd_type, 16)
                        else:
                            raise ValueError(logger.error([5601, kwargs]))
                        oid = ObjectId(service_code=sc)
                        self._id = DataDictionaryId.pack(dd_type, oid.value)
                else:
                    raise ValueError(logger.error([5601, kwargs]))

    def __str__(self):
        return hex_str(self._id, 17)

    def __repr__(self):
        dd_type, oid = DataDictionaryId.unpack(self._id)
        oid_str = hex_str(oid, 16)
        content = [str(self), dd_type, oid_str]
        return 'DataDictionaryId({0})\nData Dictionary Type: {1}\nObjectId: {2}\n'.format(*content)

    def _generate(self, ddid):
        dd_type, oid = DataDictionaryId.unpack(ddid)
        self._id = DataDictionaryId.pack(dd_type, oid)

    @staticmethod
    def unpack(ddid):
        ddid_value = ddid
        if isinstance(ddid, int):
            pass
        else:
            if isinstance(ddid, str) and len(ddid) == DDID_LEN:
                ddid_value = int(ddid, 16)
            else:
                if isinstance(ddid, DataDictionaryId):
                    ddid_value = ddid.value
                else:
                    raise TypeError(logger.error([5602]))
        dd_type = ddid_value >> DD_TYPE_BITS_SHIFT
        oid = ddid_value & OID_MASK
        if DataDictionaryId._validate(dd_type, oid):
            return dd_type, oid
        else:
            raise ValueError(logger.error([5603]))

    @staticmethod
    def pack(dd_type: int, oid: int):
        if DataDictionaryId._validate(dd_type, oid):
            ddid = (dd_type << DD_TYPE_BITS_SHIFT) | oid
            return ddid
        else:
            raise ValueError(logger.error([5603]))

    @staticmethod
    def _validate(dd_type: int, oid: int):
        if ObjectId.validate(oid) and hex_str(dd_type, 1) in DD_TYPES:
            return True
        else:
            return False

    @property
    def value(self):
        return self._id

    @property
    def dd_type(self):
        return hex_str(self._id >> DD_TYPE_BITS_SHIFT, 1)

    @property
    def oid(self):
        return hex_str(self.value, -16)

    def __eq__(self, other):
        if isinstance(other, DataDictionaryId):
            return self.value == other.value
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, DataDictionaryId):
            return self.value != other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, DataDictionaryId):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, DataDictionaryId):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, DataDictionaryId):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, DataDictionaryId):
            return self.value >= other.value
        return NotImplemented
