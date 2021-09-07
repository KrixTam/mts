# coding: utf-8

import sqlite3
from os import path, getpid
from config import config
import connectorx as cx
import threading
from abc import ABCMeta, abstractmethod
import logging
from moment import moment
from random import getrandbits

_SERVICE_CODE_BITS = 6
_TIMESTAMP_BITS = 42
_PID_CODE_BITS = 4
_SEQUENCE_BITS = 12

_SERVICE_CODE_BITS_SHIFT = _TIMESTAMP_BITS + _PID_CODE_BITS + _SEQUENCE_BITS
_TIMESTAMP_BITS_SHIFT = _PID_CODE_BITS + _SEQUENCE_BITS
_PID_CODE_BITS_SHIFT = _SEQUENCE_BITS

_SERVICE_CODE_MASK = -1 ^ (-1 << _SERVICE_CODE_BITS)
_TIMESTAMP_MASK = -1 ^ (-1 << _TIMESTAMP_BITS)
_PID_CODE_MASK = -1 ^ (-1 << _PID_CODE_BITS)
_SEQUENCE_MASK = -1 ^ (-1 << _SEQUENCE_BITS)


class ObjectId(object):
    _pid = getpid()
    _lock = threading.Lock()
    _epoch = 1608480000
    _service_code = 0
    _pid_code = getrandbits(4)
    _last_ts = None
    _sequence = 0
    __slots__ = ('_id',)

    def __init__(self, oid=None):
        if oid is None:
            self._generate()
        else:
            if isinstance(oid, str) and len(oid) == 16:
                self._generate(oid)
            else:
                if isinstance(oid, ObjectId):
                    self._generate(str(oid))
                else:
                    raise TypeError("id must be an instance of (str, ObjectId), not %s" % (type(oid),))

    def __str__(self):
        return "{0:0{1}x}".format(self._id, 16)

    def __repr__(self):
        service_code, ts, pid_code, sequence = ObjectId._unpack(self._id)
        now = moment()
        gap = (now.unix() - ObjectId._epoch) * 1000 + now.milliseconds() - ts
        content = "ObjectId('%s')\n" % (str(self),)
        content = content + "Service Code: %s\n" % (str(service_code))
        content = content + "Timestamp: %s\n" % (now.subtract(gap, 'ms').format())
        content = content + "PID Code: %s\n" % (str(pid_code))
        content = content + "Sequence No.: %s\n" % (str(sequence))
        return content

    @staticmethod
    def register(settings):
        if 'epoch' in settings and isinstance(settings['epoch'], int):
            ObjectId._epoch = settings['epoch'] & _TIMESTAMP_MASK
        if 'service_code' in settings and isinstance(settings['service_code'], int):
            ObjectId._service_code = settings['service_code'] & _SERVICE_CODE_MASK

    @classmethod
    def _unpack(cls, oid):
        service_code = oid >> _SERVICE_CODE_BITS_SHIFT
        last_ts = (oid >> _TIMESTAMP_BITS_SHIFT) & _TIMESTAMP_MASK
        pid_code = (oid >> _TIMESTAMP_BITS_SHIFT) & _PID_CODE_MASK
        sequence = oid & _SEQUENCE_MASK
        return service_code, last_ts, pid_code, sequence

    @classmethod
    def _generate_pid_code(cls):
        pid = getpid()
        if pid != cls._pid:
            cls._pid = pid
            cls._pid_code = getrandbits(4)
        return cls._pid_code

    def _generate(self, oid: str = None):
        if oid is None:
            ts = moment()
            assert ts.unix() >= ObjectId._epoch, "clock is moving backwards"
            ts = (ts.unix() - ObjectId._epoch) * 1000 + ts.milliseconds()
            pid_code = ObjectId._generate_pid_code()
            sequence = 0
            with ObjectId._lock:
                if ts == ObjectId._last_ts:
                    ObjectId._sequence = (ObjectId._sequence + 1) & _SEQUENCE_MASK
                    sequence = ObjectId._sequence
                    if ObjectId._sequence == 0:
                        ts = ts + 1
                else:
                    ObjectId._sequence = 0
                ObjectId._last_ts = ts
            new_id = (ObjectId._service_code << _SERVICE_CODE_BITS_SHIFT) | (ts << _TIMESTAMP_BITS_SHIFT) | (
                    pid_code << _PID_CODE_BITS_SHIFT) | sequence
            self._id = new_id
        else:
            self._validate(oid)

    def _validate(self, oid):
        oid_value = int(oid, 16)
        try:
            ObjectId._pid = getpid()
            ObjectId._service_code, ObjectId._last_ts, ObjectId._pid_code, ObjectId._sequence = ObjectId._unpack(oid_value)
            self._id = oid_value
        except ValueError:
            print('id is invalid. Program will generate a new Object ID.')
            self._generate()

    @property
    def value(self):
        return self._id

    def __eq__(self, other):
        if isinstance(other, ObjectId):
            return self._id == other.value
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, ObjectId):
            return self._id != other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, ObjectId):
            return self._id < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, ObjectId):
            return self._id <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, ObjectId):
            return self._id > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, ObjectId):
            return self._id >= other.value
        return NotImplemented


class DataUnitProcessor(object):
    def __init__(self, settings):
        self._config = config({
            'name': 'DataUnitProcessor',
            'default': {
                'db': 'sqlite:///path/to/db'
            },
            'schema': {
                'type': 'object',
                'properties': {
                    'db': {'type': 'string'}
                }
            }
        })
        for key, value in settings.items():
            if key in self._config:
                self._config[key] = value
        self._ds = {}

    def add_ds(self, ds):
        if isinstance(ds, DataUnitService):
            self._ds[ds.service_id()] = ds
        else:
            if isinstance(ds, dict):
                ds_object = DataUnitService(ds)
                self._ds[ds_object.service_id()] = ds_object
            else:
                raise TypeError('参数ds的类型应该为DataService或者dict')

    def db_path(self):
        return self._config['db'].split('://')[1]

    def init_table(self, service_id, cursor):
        if service_id in self._ds:
            ds = self._ds[service_id]
        else:
            logging.warning('不存在数据单元服务：[' + service_id + ']，未能进行初始化处理')

    def init_service(self, service_id):
        if service_id in self._ds:
            ds = self._ds[service_id]
            conn = sqlite3.connect(self.db_path())
            c = conn.cursor()
            # 初始化SDU数据
            sdu_data_filename = path.join(ds.ds_path(), service_id + '.sdu')
        else:
            logging.warning('不存在数据单元服务：[' + service_id + ']，未能进行初始化处理')


class DataUnitService(object):
    def __init__(self, settings):
        self._config = config({
            'name': 'DataUnitService',
            'default': {
                'service_id': 'service_id',
                'version': 0,
                'ds_path': 'ds',
                'tdu': {},
                'sdu': {}
            },
            'schema': {
                'type': 'object',
                'properties': {
                    'service_id': {'type': 'string'},
                    'version': {
                        'type': 'integer',
                        'minimum': 0
                    },
                    'ds_path': {'type': 'string'},
                    'tdu': {'type': 'object'},
                    'sdu': {'type': 'object'}
                }
            }
        })
        for key, value in settings.items():
            if key in self._config:
                self._config[key] = value
        self._tdu = TimeDataUnit(settings['tdu'])
        self._sdu = SpaceDataUnit(settings['sdu'])

    def service_id(self):
        return self._config['service_id']

    def owners(self):
        return self._tdu.owners()

    def metrics(self):
        return self._tdu.metrics()

    def ds_path(self):
        return self._config['ds_path']

    def init_tables(self, cursor):
        # 初始化SDU数据表
        sdu_table_name = self.service_id() + '_sdu'
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + sdu_table_name + "'")
        if cursor.fetchone()[0] == 1:
            cursor.execute("DROP TABLE " + sdu_table_name)
        cursor.execute()

    def import_data(self):
        sdu_data_filename = path.join(self.ds_path(), self.service_id() + '.sdu')


class DataUnit(metaclass=ABCMeta):
    __slots__ = ('_config',)

    def __init__(self, settings):
        self._config = None
        self.init_config()
        self.set_config(settings)

    @abstractmethod
    def init_config(self):
        pass

    def set_config(self, settings):
        for key, value in settings.items():
            if key in self._config:
                self._config[key] = value


class TimeDataUnit(DataUnit):
    def __init__(self, settings):
        super().__init__(settings)

    def init_config(self):
        self._config = config({
            'name': 'TimeDataUnit',
            'default': {
                'owners': ['owner_01'],
                'metrics': ['metric_01']
            },
            'schema': {
                'type': 'object',
                'properties': {
                    'owners': {
                        'type': 'array',
                        'items': {'type': 'string'},
                        'minItems': 1
                    },
                    'metrics': {
                        'type': 'array',
                        'items': {'type': 'string'},
                        'minItems': 1
                    }
                }
            }
        })

    def owners(self):
        return self._config['owners']

    def metrics(self):
        return self._config['metrics']


class SpaceDataUnit(DataUnit):
    def __init__(self, settings):
        super().__init__(settings)

    def init_config(self):
        self._config = config({
            'name': 'SpaceDataUnit',
            'default': {
                'tags': [
                    {
                        'name': 'tag_name',
                        'values': ['tag_value_01']
                    }
                ]
            },
            'schema': {
                'type': 'object',
                'properties': {
                    'tag': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'name': {'type': 'string'},
                                'value': {
                                    'type': 'array',
                                    'items': {'type': 'string'},
                                    'minItems': 1
                                }
                            }
                        },
                        'minItems': 1
                    }
                }
            }
        })
