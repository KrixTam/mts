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


def hex_str(num, bits):
    length = abs(bits)
    res = "{0:0{1}x}".format(num, length)
    if bits > 0:
        return res[:bits]
    else:
        return res[bits:]


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
    _DD_TYPE_OWNER: hex_str(1, 1),
    _DD_TYPE_METRIC: hex_str(2, 1),
    _DD_TYPE_TAG: hex_str(3, 1),
    _DD_TYPE_TAG_VALUE: hex_str(4, 1)
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

_FIELDS_DD = {'ddid': 'VARCHAR(17)', 'disc': 'VARCHAR(160)', 'oid_mask': 'VARCHAR(32)'}


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
                    raise TypeError("id 应为 (str, ObjectId)，而非 %s" % (type(oid),))

    def __str__(self):
        return hex_str(self._id, 16)

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
            raise ValueError('非法 id；时间逆流。')
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
            assert ts.unix() >= ObjectId._epoch, "异常：时间逆流。"
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
                print('非法 id；将自动生成一个新的 Object ID。')
                self._generate()
        else:
            print('非法 id；将自动生成一个新的 Object ID。')
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
                    raise TypeError("ddid 应为 (str, DataDictionaryId)，而非 %s" % (type(ddid),))
        else:
            if 'oid' in kwargs and 'dd_type' in kwargs:
                dd_type = kwargs['dd_type']
                if isinstance(dd_type, str):
                    dd_type = int(dd_type, 16)
                self._id = DataDictionaryId.pack(dd_type, kwargs['oid'])
            else:
                if 'dd_type' in kwargs and 'service_code' in kwargs:
                    dd_type = kwargs['dd_type']
                    if isinstance(dd_type, str):
                        dd_type = int(dd_type, 16)
                    oid = ObjectId(service_code=kwargs['service_code'])
                    self._id = DataDictionaryId.pack(dd_type, oid.value)
                else:
                    raise ValueError('构建 DataDictionaryId 时，遇到异常的参数。')

    def __str__(self):
        return hex_str(self._id, 17)

    def __repr__(self):
        dd_type, oid = DataDictionaryId.unpack(self._id)
        content = "DataDictionaryId('%s')\n" % (str(self),)
        content = content + "Data Dictionary Type: %s\n" % (dd_type,)
        oid_str = hex_str(oid, 16)
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
                raise TypeError("ddid 应为17位长度的字符串，或者是 DataDictionaryId 实例。")
        dd_type = ddid_value >> _DD_TYPE_BITS_SHIFT
        oid = ddid_value & _OID_MASK
        if ObjectId.validate(oid) and dd_type in _DD_TYPE.values():
            return dd_type, oid
        else:
            raise ValueError('异常：非法 ddid。')

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
        return hex_str(self._id, -16)

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
            raise ValueError('DBConnector 需要对 db_url 进行登记后方能使用。')
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
                        raise ValueError('owner_id 不能为 None。')
                    else:
                        return service_id + '_' + owner_id + '_tdu'
        else:
            raise ValueError('table_type 应为 (%s)' % (_TABLE_TYPE,))

    @staticmethod
    def get_dd(service_id: str, dd_type: str):
        if dd_type in _DD_TYPE:
            table_name = DBConnector.get_table_name(service_id, _TABLE_TYPE_DD)
            dd = DBConnector.query(table_name)
            return dd[dd['ddid'].str[0] == _DD_TYPE[dd_type]]
        else:
            raise ValueError('异常：未能识别的 dd_type "%s".' % (dd_type,))


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
                raise TypeError("ds 应为 (DataService, dict)，而非 %s" % (type(ds),))

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
        data = self._dd[self._dd['ddid'].str[0] == _DD_TYPE[_DD_TYPE_OWNER]]
        return data['ddid']

    @property
    def metrics(self):
        data = self._dd[self._dd['ddid'].str[0] == _DD_TYPE[_DD_TYPE_METRIC]]
        return data['ddid']

    @property
    def tags(self):
        data = self._dd[self._dd['ddid'].str[0] == _DD_TYPE[_DD_TYPE_TAG]]
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
        owner_mappings = {}
        tag_mappings = {}
        tag_value_mappings = {}
        for owner in self._config['owners']:
            owner_ddid = DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_OWNER], service_code=self.service_code)
            owner_mappings[owner] = owner_ddid.oid
            line = str(owner_ddid) + ',' + owner + ','
            ddid_content.append(line)
        for metric in self._config['metrics']:
            line = str(DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_METRIC], service_code=self.service_code)) + ',' + metric + ','
            ddid_content.append(line)
        for tag in self._config['tags']:
            tag_ddid = DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_TAG], service_code=self.service_code)
            tag_oid = tag_ddid.oid
            tag_mappings[tag['name']] = tag_ddid.oid
            line = str(tag_ddid) + ',' + tag['name'] + ','
            ddid_content.append(line)
            tag_value_mappings[tag_oid] = {}
            mask_bit = 0
            for tag_value in tag['values']:
                mask_bit = mask_bit + 1
                mask = -1 ^ (-1 << mask_bit)
                mask_str = tag_oid + hex_str(mask_bit, 16)
                line = str(DataDictionaryId(dd_type=_DD_TYPE[_DD_TYPE_TAG_VALUE], service_code=self.service_code)) + ',' + tag_value['disc'] + ',' + mask_str
                ddid_content.append(line)
                for owner in tag_value['owners']:
                    if owner in tag_value_mappings[tag_oid]:
                        tag_value_mappings[tag_oid][owner] = tag_value_mappings[tag_oid][owner] + mask
                    else:
                        tag_value_mappings[tag_oid][owner] = mask
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
        # 初始化SDU数据
        sdu_content = {}
        sdu_headers = ['owner']
        for owner, owner_oid in owner_mappings.items():
            sdu_content[owner_oid] = {}
            for tag, tag_oid in tag_mappings.items():
                sdu_content[owner_oid][tag_oid] = 0
        for tag_oid, value in tag_value_mappings.items():
            sdu_headers.append(tag_oid)
            for owner, tag_value in value.items():
                owner_oid = owner_mappings[owner]
                sdu_content[owner_oid][tag_oid] = tag_value
        sdu_filename = self._get_filename(_FILE_TYPE_SDU)
        DataUnitService.write_file(sdu_filename, sdu_headers, sdu_content)
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
                        raise TypeError('文件类型 "%s" 后必须跟有一个值为 owner id 的参数，或者一个存储着一系列 owner id 的 list' % (file_type,))
        else:
            raise ValueError('并非预先定义的 file_type，获取文件名失败')

    def import_data(self, file_type, table_name, *args):
        filename = self._get_filename(file_type, *args)
        DBConnector.import_data(filename, table_name)

    def export_data(self, output_filename):
        pass

    @staticmethod
    def write_file(output_filename, headers, content):
        with open(output_filename, 'w', newline='') as file_handler:
            if isinstance(content, list):
                wr = csv.writer(file_handler)
                if isinstance(headers, str):
                    if len(headers) > 0:
                        wr.writerow(headers)
                else:
                    if isinstance(headers, list):
                        wr.writerow(','.join(headers))
                    else:
                        raise TypeError('headers 的类型必须为包含","的字符串或字符串list')
                for line in content:
                    wr.writerow(line)
            else:
                if isinstance(content, dict):
                    if isinstance(headers, list):
                        writer = csv.DictWriter(file_handler, fieldnames=headers)
                        writer.writeheader()
                        first_field = headers[0]
                        other_fields = headers.copy()
                        del other_fields[0]
                        for first_field_value, items in content.items():
                            line = {first_field: first_field_value}
                            for field in other_fields:
                                line[field] = items[field]
                            writer.writerow(line)
                    else:
                        raise TypeError('当 content 为 dict 时，headers必须为字符串的 list')
                else:
                    raise TypeError('content 的类型必须为 list 或 dict')


class DataUnit(object):
    pass


class TimeDataUnit(DataUnit):
    def __init__(self, owners, metrics):
        super().__init__()


class SpaceDataUnit(DataUnit):
    def __init__(self, service_id, tag_definition_raw=None):
        super().__init__()
        dd_tag = DBConnector.get_dd(service_id, _DD_TYPE_TAG)
        dd_tag_value = DBConnector.get_dd(service_id, _DD_TYPE_TAG_VALUE)
        self._tag_definition = {}
        for index, row in dd_tag.iterrows():
            tag_oid = row['ddid'][1:]
            self._tag_definition[tag_oid] = {}
        for index, row in dd_tag_value.iterrows():
            tag_value_oid = row['ddid'][1:]
            tag_oid = row['oid_mask'][:16]
            tag_value_mask = row['oid_mask'][16:]
            self._tag_definition[tag_oid][tag_value_oid] = tag_value_mask

