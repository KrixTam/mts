# coding: utf-8

import csv
import sqlite3
from os import path, getpid
from config import config
import connectorx as cx
import threading
import logging
from moment import moment
from random import getrandbits
import pandas as pd

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

_DD_TYPE_BITS = 4
_OID_BITS = 64

_DD_TYPE_BITS_SHIFT = _OID_BITS

_DD_TYPE_MASK = -1 ^ (-1 << _DD_TYPE_BITS)
_OID_MASK = -1 ^ (-1 << _OID_BITS)

_DD_TYPE_OWNER = 'owner'
_DD_TYPE_METRIC = 'metric'
_DD_TYPE_TAG = 'tag'
_DD_TYPE_TAG_VALUE = 'tag_value'

_DD_TYPE = {
    _DD_TYPE_OWNER: 1,
    _DD_TYPE_METRIC: 2,
    _DD_TYPE_TAG: 3,
    _DD_TYPE_TAG_VALUE: 4
}

_DD_HEADERS = 'ddid, disc, mask'

_FILE_TYPE_DD = 'dd'
_FILE_TYPE_SDU = 'sdu'
_FILE_TYPE_TDU = 'tdu'

_TABLE_TYPE_DD = 'dd'
_TABLE_TYPE_SDU = 'sdu'
_TABLE_TYPE_TDU = 'tdu'

_TABLE_TYPE = [_TABLE_TYPE_DD, _TABLE_TYPE_SDU, _TABLE_TYPE_TDU]

_FILE_EXT = {
    _FILE_TYPE_DD: '.dd',
    _FILE_TYPE_SDU: '.sdu',
    _FILE_TYPE_TDU: '.tdu'
}

_FIELDS_DD = {'ddid': 'VARCHAR(17)', 'disc': 'VARCHAR(160)', 'mask': 'INT'}


class ObjectId(object):
    _pid = getpid()
    _lock = threading.Lock()
    _epoch = 1608480000
    _service_code = 0
    _pid_code = getrandbits(4)
    _last_ts = None
    _sequence = 0
    __slots__ = ('_id', '_sc',)

    def __init__(self, oid=None, service_code: int = None):
        self._sc = None
        if service_code is None:
            self._sc = ObjectId._service_code
        else:
            self._sc = service_code
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
        service_code, ts, pid_code, sequence = ObjectId.unpack(self._id)
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

    @staticmethod
    def unpack(oid):
        service_code = oid >> _SERVICE_CODE_BITS_SHIFT
        last_ts = (oid >> _TIMESTAMP_BITS_SHIFT) & _TIMESTAMP_MASK
        pid_code = (oid >> _TIMESTAMP_BITS_SHIFT) & _PID_CODE_MASK
        sequence = oid & _SEQUENCE_MASK
        if last_ts <= 0:
            raise ValueError('Invalid id. Time is moving backwards.')
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
            new_id = (self._sc << _SERVICE_CODE_BITS_SHIFT) | (ts << _TIMESTAMP_BITS_SHIFT) | (pid_code << _PID_CODE_BITS_SHIFT) | sequence
            self._id = new_id
        else:
            self._validate(oid)

    def _validate(self, oid):
        if len(oid) == 16:
            try:
                oid_value = int(oid, 16)
                ObjectId._pid = getpid()
                ObjectId._service_code, ObjectId._last_ts, ObjectId._pid_code, ObjectId._sequence = ObjectId.unpack(oid_value)
                self._id = oid_value
                self._sc = ObjectId._service_code
            except ValueError:
                print('id is invalid. Program will generate a new Object ID.')
                self._generate()
        else:
            print('id is invalid. Program will generate a new Object ID.')
            self._generate()

    @staticmethod
    def validate(oid):
        if isinstance(oid, int):
            try:
                ObjectId.unpack(oid)
                return True
            except ValueError:
                return False
        else:
            if isinstance(oid, str) and len(oid) == 16:
                try:
                    oid_value = int(oid, 16)
                    ObjectId.unpack(oid_value)
                    return True
                except ValueError:
                    return False
            else:
                return False

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


class DataDictionaryId(object):
    __slots__ = ('_id',)

    def __init__(self, **kwargs):
        if 'ddid' in kwargs:
            ddid = kwargs['ddid']
            if isinstance(ddid, DataDictionaryId):
                self._generate(ddid)
            else:
                if isinstance(ddid, str):
                    self._generate(ddid)
                else:
                    raise TypeError("ddid must be an instance of (str, DataDictionaryId), not %s" % (type(ddid),))
        else:
            if 'oid' in kwargs and 'dd_type' in kwargs:
                self._id = DataDictionaryId.pack(kwargs['dd_type'], kwargs['oid'])
            else:
                if 'dd_type' in kwargs and 'service_code' in kwargs:
                    oid = ObjectId(service_code=kwargs['service_code'])
                    self._id = DataDictionaryId.pack(kwargs['dd_type'], oid.value)
                else:
                    raise ValueError('Invalid parameters for DataDictionaryId.')

    def __str__(self):
        return "{0:0{1}x}".format(self._id, 17)

    def __repr__(self):
        dd_type, oid = DataDictionaryId.unpack(self._id)
        content = "DataDictionaryId('%s')\n" % (str(self),)
        content = content + "Data Dictionary Type: %s\n" % (dd_type,)
        oid_str = "{0:0{1}x}".format(oid, 16)
        content = content + "ObjectId: %s\n" % (oid_str,)
        return content

    def _generate(self, ddid):
        dd_type, oid = DataDictionaryId.unpack(ddid)
        self._id = DataDictionaryId.pack(dd_type, oid)

    @staticmethod
    def unpack(ddid):
        ddid_value = ddid
        if isinstance(ddid, int):
            pass
        else:
            if isinstance(ddid, str) and len(ddid) == 17:
                ddid_value = int(ddid, 16)
            else:
                raise TypeError("ddid must be an instance of str, which contains 17 characters, or an instance of DataDictionaryId.")
        dd_type = ddid_value >> _DD_TYPE_BITS_SHIFT
        oid = ddid_value & _OID_MASK
        if ObjectId.validate(oid) and dd_type in _DD_TYPE.values():
            return dd_type, oid
        else:
            raise ValueError('Invalid ddid.')

    @staticmethod
    def pack(dd_type: int, oid: int):
        ddid = (dd_type << _DD_TYPE_BITS_SHIFT) | oid
        return ddid

    @property
    def value(self):
        return self._id

    @property
    def dd_type(self):
        return self._id >> _DD_TYPE_BITS_SHIFT

    @property
    def oid(self):
        return self._id & _OID_MASK

    def __eq__(self, other):
        if isinstance(other, DataDictionaryId):
            return self._id == other.value
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, DataDictionaryId):
            return self._id != other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, DataDictionaryId):
            return self._id < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, DataDictionaryId):
            return self._id <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, DataDictionaryId):
            return self._id > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, DataDictionaryId):
            return self._id >= other.value
        return NotImplemented


class DBConnector(object):
    _db_url = None
    _connection = None

    def __init__(self):
        pass

    @staticmethod
    def register(db_url):
        DBConnector._db_url = db_url

    @staticmethod
    def query(table_name, fields: list = None, condition: str = None):
        sql = 'SELECT '
        if fields is None:
            sql = sql + '* from ' + table_name
        else:
            sql = sql + ', '.join(fields) + table_name
        if condition is not None:
            sql = sql + 'WHERE ' + condition
        return cx.read_sql(DBConnector._db_url, sql)

    @staticmethod
    def get_cursor():
        if DBConnector._connection is None:
            db_path = DBConnector._db_url.split('://')[1]
            DBConnector._connection = sqlite3.connect(db_path)
        cursor = DBConnector._connection.cursor()
        return cursor

    @staticmethod
    def disconnect():
        if DBConnector._connection is not None:
            DBConnector._connection.close()
            DBConnector._connection = None

    @staticmethod
    def init_table(table_name, fields):
        if DBConnector._db_url is None:
            raise ValueError('Please register db_url to DBConnector first.')
        else:
            cursor = DBConnector.get_cursor()
            # 如果存在table，先drop掉
            sql = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + table_name + "'"
            cursor.execute(sql)
            if cursor.fetchone()[0] == 1:
                sql = "DROP TABLE " + table_name
                cursor.execute(sql)
            # 创建table
            for key, value in fields.items():
                fields.append('[' + key + '] ' + value)
            sql = "CREATE TABLE IF NOT EXISTS " + table_name + "(" + ", ".join(fields) + ")"
            print(sql)
            cursor.execute(sql)
            cursor.close()

    @staticmethod
    def import_data(filename, table_name):
        cursor = DBConnector.get_cursor()
        with open(filename, 'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [tuple([row[field] for field in dr.fieldnames]) for row in dr]
            sql = 'INSERT INTO ' + table_name + ' ' + str(tuple(dr.fieldnames)) + ' VALUES (' + ', '.join(list('?' * len(dr.fieldnames))) + ');'
            cursor.executemany(sql, to_db)
        cursor.close()

    @staticmethod
    def get_table_name(service_id: str, table_type: str, owner_id: str = None):
        if table_type in _TABLE_TYPE:
            if table_type == _TABLE_TYPE_DD:
                return service_id + '_dd'
            else:
                if table_type == _TABLE_TYPE_SDU:
                    return service_id + '_sdu'
                else:
                    if owner_id is None:
                        raise ValueError('owner_id can not be None')
                    else:
                        return service_id + '_' + owner_id + '_tdu'
        else:
            raise ValueError('table_type should be one of %s' % (_TABLE_TYPE,))

    @staticmethod
    def get_dd(service_id: str, dd_type: str):
        if dd_type in _DD_TYPE:
            table_name = DBConnector.get_table_name(service_id, _TABLE_TYPE_DD)
            dd = DBConnector.query(table_name)
            return dd[dd['ddid'].str[0] == hex(_DD_TYPE[dd_type])[-1]]
        else:
            raise ValueError('Unknown dd_type "%s".' % (dd_type,))


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
        assert not self._config.is_default('db')
        DBConnector.register(self._config['db'])
        self._ds = {}

    def add_ds(self, ds):
        if isinstance(ds, DataUnitService):
            self._ds[ds.service_id] = ds
        else:
            if isinstance(ds, dict):
                ds_object = DataUnitService(ds)
                self._ds[ds_object.service_id] = ds_object
            else:
                raise TypeError("ds must be an instance of (DataService, dict), not %s" % (type(ds),))

    def init_service(self, service_id: str):
        if service_id in self._ds:
            ds = self._ds[service_id]
            ds.init_tables()
        else:
            logging.warning('不存在数据单元服务：[' + service_id + ']，未能进行初始化处理')


class DataUnitService(object):
    __slots__ = ('_dd', '_config',)

    def __init__(self, settings):
        self._config = config({
            'name': 'DataUnitService',
            'default': {
                'service_id': '00',
                'version': 0,
                'ds_path': 'ds',
                'owners': ['owner_01'],
                'metrics': ['metric_01'],
                'tags': [
                    {
                        'name': 'xxx',
                        'values': [
                            {
                                'disc': 'xxx',
                                'owners': []
                            }
                        ]
                    }
                ]

            },
            'schema': {
                'type': 'object',
                'properties': {
                    'service_id': {
                        'type': 'string',
                        'pattern': '[0-7]{2}'
                    },
                    'version': {
                        'type': 'integer',
                        'minimum': 0
                    },
                    'ds_path': {'type': 'string'},
                    'owners': {
                        'type': 'array',
                        'items': {'type': 'string'},
                        'minItems': 1
                    },
                    'metrics': {
                        'type': 'array',
                        'items': {'type': 'string'},
                        'minItems': 1
                    },
                    'tag': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'name': {'type': 'string'},
                                'values': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'object',
                                        'properties': {
                                            'disc': {'type': 'string'},
                                            'owners': {
                                                'type': 'array',
                                                'items': {'type': 'string'}
                                            }
                                        }
                                    },
                                    'minItems': 1
                                }
                            }
                        },
                        'minItems': 1
                    }
                }
            }
        })
        for key, value in settings.items():
            if key in self._config:
                self._config[key] = value
        self._dd = None
        self.load_dd()
        # TODO
        # TDU?SDU?初始化？
        # 直接load数据初始化的处理后续要考虑

    @property
    def service_id(self):
        return self._config['service_id']

    @property
    def service_code(self):
        return int(self.service_id, 8)

    @property
    def owners(self):
        data = self._dd[self._dd['ddid'].str[0] == hex(_DD_TYPE[_DD_TYPE_OWNER])[-1]]
        return data['ddid']

    @property
    def metrics(self):
        data = self._dd[self._dd['ddid'].str[0] == hex(_DD_TYPE[_DD_TYPE_METRIC])[-1]]
        return data['ddid']

    @property
    def tags(self):
        data = self._dd[self._dd['ddid'].str[0] == hex(_DD_TYPE[_DD_TYPE_TAG])[-1]]
        return data['ddid']

    def load_dd(self):
        self._dd = DBConnector.query(self._get_table_name(_TABLE_TYPE_DD))

    def _get_table_name(self, table_type: str, owner_id: str = None):
        return DBConnector.get_table_name(self.service_id, table_type, owner_id)

    def init_tables(self):
        # 建立DD数据表
        dd_table_name = self._get_table_name(_TABLE_TYPE_DD)
        DBConnector.init_table(dd_table_name, _FIELDS_DD)
        # 初始化ddid数据
        ddid_content = []
        for owner in self._config['owners']:
            line = str(DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_OWNER], service_code=self.service_code)) + ', ' + owner + ', 0'
            ddid_content.append(line)
        for metric in self._config['metrics']:
            line = str(DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_METRIC], service_code=self.service_code)) + ', ' + metric + ', 0'
            ddid_content.append(line)
        for tag in self._config['tags']:
            line = str(DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_TAG], service_code=self.service_code)) + ', ' + tag['name'] + ', 0'
            ddid_content.append(line)
            mask_bit = 0
            for tag_value in tag['values']:
                mask_bit = mask_bit + 1
                mask = -1 ^ (-1 << mask_bit)
                line = str(DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_TAG_VALUE], service_code=self.service_code)) + ', ' + tag_value['disc'] + ', ' + str(mask)
                ddid_content.append(line)
        dd_filename = self._get_filename(_FILE_TYPE_DD)
        DataUnitService.write_file(dd_filename, _DD_HEADERS, ddid_content)
        # 导入数据到数据库表
        DBConnector.import_data(dd_filename, dd_table_name)
        self.load_dd()
        # 建立SDU数据表
        sdu_table_name = self._get_table_name(_TABLE_TYPE_SDU)
        sdu_fields = {'owner': 'VARCHAR(16)'}
        for tag in self.tags:
            sdu_fields[tag] = 'INT'
        DBConnector.init_table(sdu_table_name, sdu_fields)
        # TODO
        # 初始化SDU数据
        # 建立TDU数据表
        tdu_fields = {'timestamp': 'VARCHAR(16)'}  # String of Unix Millisecond Timestamp
        for metric in self.metrics:
            tdu_fields[metric] = 'VARCHAR(16)'
        for owner in self.owners:
            tdu_table_name = self._get_table_name(_TABLE_TYPE_TDU, owner)
            DBConnector.init_table(tdu_table_name, tdu_fields)
        # TODO
        # 初始化TDU数据

    def _get_filename(self, file_type, *args):
        if file_type in _FILE_EXT:
            if file_type == _FILE_TYPE_DD or file_type == _FILE_TYPE_SDU:
                return path.join(self._config['ds_path'], self.service_id + _FILE_EXT[file_type])
            else:
                if isinstance(args[0], list):
                    filenames = []
                    for owner_id in args[0]:
                        filenames.append(path.join(self._config['ds_path'], self.service_id + '_' + owner_id + _FILE_EXT[file_type]))
                    return filenames
                else:
                    if isinstance(args[0], str):
                        return path.join(self._config['ds_path'], self.service_id + '_' + args[0] + _FILE_EXT[file_type])
                    else:
                        raise TypeError('File type "%s" should with parameter of a list of owner ids or one owner id.' % (file_type,))
        else:
            raise ValueError('Unknown file type to get the filename.')

    def import_data(self, file_type, table_name, *args):
        filename = self._get_filename(file_type, *args)
        DBConnector.import_data(filename, table_name)

    def export_data(self, output_filename):
        pass

    @staticmethod
    def write_file(output_filename, headers, content):
        with open(output_filename, 'w', newline='') as file_handler:
            wr = csv.writer(file_handler)
            if len(headers) > 0:
                wr.writerow(headers)
            for line in content:
                wr.writerow(line)


class DataUnit(object):
    pass


class TimeDataUnit(DataUnit):
    def __init__(self, owners, metrics):
        super().__init__()


class SpaceDataUnit(DataUnit):
    def __init__(self, service_id, tags=None):
        super().__init__()
        dd_tag = DBConnector.get_dd(service_id, _DD_TYPE_TAG)
        dd_tag_value = DBConnector.get_dd(service_id, _DD_TYPE_TAG_VALUE)
