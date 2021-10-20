import csv
import sqlite3
import threading
import connectorx as cx
import pandas as pd
from os import path, getpid
from abc import abstractmethod
from random import getrandbits
from cachetools import TTLCache
from datetime import datetime
from moment import moment
from ni.config import Config
from mts.const import *
from mts.utils import logger


class ObjectId(object):
    _pid = getpid()
    _lock = threading.Lock()
    _epoch = EPOCH_DEFAULT
    _service_code = SERVICE_CODE_MIN
    _pid_code = getrandbits(4)
    _last_ts = None
    _sequence = 0
    __slots__ = ('_id', '_sc',)

    def __init__(self, oid=None, service_code: int = None):
        self._sc = None
        if service_code is None:
            self._sc = ObjectId._service_code
        else:
            if (service_code >= SERVICE_CODE_MIN) and (service_code <= SERVICE_CODE_MAX):
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
        return hex_str(self._id, 16)

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
            ObjectId._service_code = settings['service_code'] & SERVICE_CODE_MASK

    @staticmethod
    def unpack(oid):
        oid_value = oid
        if isinstance(oid, int):
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
        if (service_code < SERVICE_CODE_MIN) or (service_code > SERVICE_CODE_MAX):
            raise ValueError(logger.error([5500, service_code, SERVICE_CODE_MIN, SERVICE_CODE_MAX]))
        return service_code, last_ts, pid_code, sequence

    @staticmethod
    def pack(service_code: int, timestamp: moment = None, sn: int = None):
        ts = timestamp
        if timestamp is None:
            ts = moment()
            if ts.unix() < ObjectId._epoch:
                raise ValueError(logger.error([5502]))
            ts = (ts.unix() - ObjectId._epoch) * 1000 + ts.milliseconds()
        pid_code = ObjectId._generate_pid_code()
        sequence = sn
        if sn is None:
            sequence = 0
            with ObjectId._lock:
                if ts == ObjectId._last_ts:
                    ObjectId._sequence = (ObjectId._sequence + 1) & SEQUENCE_MASK
                    sequence = ObjectId._sequence
                    if ObjectId._sequence == 0:
                        ts = ts + 1
                else:
                    ObjectId._sequence = 0
                ObjectId._last_ts = ts
        new_id = (service_code << SERVICE_CODE_BITS_SHIFT) | (ts << TIMESTAMP_BITS_SHIFT) | (
                    pid_code << PID_CODE_BITS_SHIFT) | sequence
        return new_id

    def _generate(self, oid):
        service_code, last_ts, pid_code, sequence = ObjectId.unpack(oid)
        self._id = ObjectId.pack(service_code, last_ts, sequence)

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
        if 'ddid' in kwargs:
            ddid = kwargs['ddid']
            if isinstance(ddid, DataDictionaryId) or isinstance(ddid, str):
                self._generate(ddid)
            else:
                raise TypeError(logger.error([5600, type(ddid)]))
        else:
            if ('oid' in kwargs) and ('dd_type' in kwargs):
                dd_type = kwargs['dd_type']
                if isinstance(dd_type, str):
                    dd_type = int(dd_type, 16)
                self._id = DataDictionaryId.pack(dd_type, kwargs['oid'])
            else:
                if 'dd_type' in kwargs and 'service_code' in kwargs:
                    dd_type = kwargs['dd_type']
                    if isinstance(dd_type, str) and dd_type in DD_TYPE:
                        dd_type = int(dd_type, 16)
                    oid = ObjectId(service_code=kwargs['service_code'])
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
        if ObjectId.validate(oid) and hex_str(dd_type, 1) in DD_TYPE:
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
    _db_url = None  # sqlite:///path/to/db
    _connection = None
    _mode = 0  # 0: cx; 1: sqlite

    def __init__(self, mode=0):
        DBConnector.set_mode(mode)

    @staticmethod
    def set_mode(mode:int):
        DBConnector._mode = mode

    @staticmethod
    def register(db_url):
        DBConnector._db_url = db_url

    @staticmethod
    def query(service_id: str, table_type: str, fields: list = None, condition: str = None, owner_id: str = None):
        table_name = DBConnector.get_table_name(service_id, table_type, owner_id)
        sql = 'SELECT '
        if fields is None:
            sql = sql + '* from ' + table_name
        else:
            sql = sql + ', '.join(fields) + ' from ' + table_name
        if condition is not None:
            sql = sql + 'WHERE ' + condition
        # TODO：待删除的临时打印信息
        logger.log(sql)
        if 0 == DBConnector._mode:
            return cx.read_sql(DBConnector._db_url, sql)
        else:
            return pd.read_sql_query(sql, DBConnector.connect())

    @staticmethod
    def connect():
        if DBConnector._connection is None:
            db_path = DBConnector._db_url.split('://')[1]
            DBConnector._connection = sqlite3.connect(db_path)
        return DBConnector._connection

    @staticmethod
    def get_cursor():
        cursor = DBConnector.connect().cursor()
        return cursor

    @staticmethod
    def commit():
        DBConnector.connect().commit()

    @staticmethod
    def disconnect():
        if DBConnector._connection is not None:
            DBConnector._connection.close()
            DBConnector._connection = None

    @staticmethod
    def init_table(table_name, fields):
        if DBConnector._db_url is None:
            raise ValueError(logger.error([5700]))
        else:
            cursor = DBConnector.get_cursor()
            # 如果存在table，先drop掉
            sql = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + table_name + "'"
            cursor.execute(sql)
            if cursor.fetchone()[0] == 1:
                sql = "DROP TABLE " + table_name
                cursor.execute(sql)
            # 创建table
            fields_def = []
            for key, value in fields.items():
                fields_def.append('[' + key + '] ' + value)
            sql = "CREATE TABLE IF NOT EXISTS " + table_name + "(" + ", ".join(fields_def) + ")"
            # TODO：待删除的临时打印信息
            logger.log(sql)
            cursor.execute(sql)
            DBConnector.commit()
            cursor.close()

    @staticmethod
    def import_data(filename, table_name):
        cursor = DBConnector.get_cursor()
        with open(filename, 'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [tuple([row[field] for field in dr.fieldnames]) for row in dr]
            # TODO：待删除的临时打印信息
            logger.log(to_db)
            sql = 'INSERT INTO ' + table_name + ' ' + str(tuple(dr.fieldnames)).replace("'", '') + ' VALUES (' + ', '.join(list('?' * len(dr.fieldnames))) + ');'
            # TODO：待删除的临时打印信息
            logger.log(sql)
            # sql = 'INSERT INTO ' + table_name + ' (' + ''.join(dr.fieldnames) + ') VALUES (' + ', '.join(
            #     list('?' * len(dr.fieldnames))) + ');'
            cursor.executemany(sql, to_db)
            DBConnector.commit()
        cursor.close()

    @staticmethod
    def get_table_name(service_id: str, table_type: str, owner_id: str = None):
        if table_type in TABLE_TYPE:
            if table_type == TABLE_TYPE_DD:
                return 'dd_' + service_id
            else:
                if table_type == TABLE_TYPE_SDU:
                    return 'sdu_' + service_id
                else:
                    if owner_id is None:
                        raise ValueError(logger.error([5702]))
                    else:
                        return 'tdu' + service_id + '_' + owner_id
        else:
            raise ValueError(logger.error([5701, TABLE_TYPE]))

    @staticmethod
    def get_dd(service_id: str, dd_type: str):
        if dd_type in DD_TYPE:
            dd = DBConnector.query(service_id, TABLE_TYPE_DD)
            return dd[dd['ddid'].str[0] == dd_type]
        else:
            raise ValueError(logger.error([5703, dd_type]))


class DataUnitProcessor(object):
    def __init__(self, settings):
        self._config = Config({
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
                raise TypeError(logger.error([1001, type(ds)]))

    def init_service(self, service_id: str):
        if service_id in self._ds:
            ds = self._ds[service_id]
            ds.init_tables()
        else:
            logger.warning([1000, service_id])


class DataUnitService(object):
    __slots__ = ('_dd', '_config', '_sdu', '_tdu')

    def __init__(self, settings, init_flag=False):
        self._config = Config({
            'name': 'DataUnitService',
            'default': {
                'service_id': '00',
                'version': 0,
                'ds_path': 'ds',
                'bak_path': 'bak',
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
                    'bak_path': {'type': 'string'},
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
        self._sdu = None
        self._tdu = None
        if self._config.is_default('bak_path'):
            self._config['bak_path'] = path.join(self._config['ds_path'], 'bak')
        if init_flag:
            if self._config.is_default() or self._config.is_default('service_id') or self._config.is_default('ds_path'):
                raise ValueError(logger.error([5001]))
            else:
                self.init_tables()
        else:
            if self._config.is_default('service_id') or self._config.is_default('ds_path'):
                raise ValueError(logger.error([5002]))
            else:
                self._load_dd()
                self._init_sdu()
                # TODO：待验证
                self._init_tdu()

    @property
    def service_id(self):
        return self._config['service_id']

    @property
    def service_code(self):
        return int(self.service_id, 8)

    def _get_attribute(self, dd_type):
        data = self._dd[self._dd['ddid'].str[0] == dd_type]
        res = data['ddid'].str[1:].tolist()
        res.sort()
        return res

    @property
    def owners(self):
        return self._tdu.owners

    @property
    def metrics(self):
        return self._tdu.metrics

    @property
    def tags(self):
        return self._sdu.tags

    def disc(self, ddid: str = None, dd_type: str = None, oid: str = None):
        data = self._dd
        if ddid is None:
            if dd_type is None:
                if oid is None:
                    pass
                else:
                    data = self._dd[self._dd['ddid'].str[1:] == oid]
            else:
                if oid is None:
                    data = self._dd[self._dd['ddid'].str[0] == dd_type]
                else:
                    ddid_val = dd_type + oid
                    data = self._dd[self._dd['ddid'] == ddid_val]
        else:
            data = self._dd[self._dd['ddid'] == ddid]
        res = data['disc'].values.tolist()
        res.sort()
        return res

    def _load_dd(self):
        self._dd = DBConnector.query(self.service_id, TABLE_TYPE_DD)

    def _init_sdu(self):
        self._sdu = SpaceDataUnit(self.service_id)

    def _init_tdu(self):
        # TODO：初始化TDU
        self._tdu = TDUProcessor(self.service_id)

    def _get_table_name(self, table_type: str, owner_id: str = None):
        return DBConnector.get_table_name(self.service_id, table_type, owner_id)

    def init_tables(self):
        # 建立DD数据表
        dd_table_name = self._get_table_name(TABLE_TYPE_DD)
        DBConnector.init_table(dd_table_name, FIELDS_DD)
        # 初始化ddid数据
        ddid_content = []
        owner_mappings = {}
        metric_mappings = {}
        tag_mappings = {}
        tag_value_mappings = {}
        for owner in self._config['owners']:
            owner_ddid = DataDictionaryId(dd_type=DD_TYPE_OWNER, service_code=self.service_code)
            owner_mappings[owner] = owner_ddid.oid
            line = str(owner_ddid) + ',' + owner + ','
            ddid_content.append(line)
        for metric in self._config['metrics']:
            metric_ddid = DataDictionaryId(dd_type=DD_TYPE_METRIC, service_code=self.service_code)
            metric_mappings[metric] = metric_ddid.oid
            line = str(metric_ddid) + ',' + metric + ','
            ddid_content.append(line)
        for tag in self._config['tags']:
            tag_ddid = DataDictionaryId(dd_type=DD_TYPE_TAG, service_code=self.service_code)
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
                line = str(DataDictionaryId(dd_type=DD_TYPE_TAG_VALUE, service_code=self.service_code)) + ',' + tag_value['disc'] + ',' + mask_str
                ddid_content.append(line)
                for owner in tag_value['owners']:
                    if owner in tag_value_mappings[tag_oid]:
                        tag_value_mappings[tag_oid][owner] = tag_value_mappings[tag_oid][owner] + mask
                    else:
                        tag_value_mappings[tag_oid][owner] = mask
        dd_filename = self._get_filename(FILE_TYPE_DD)
        DataUnitService.write_file(dd_filename, DD_HEADERS, ddid_content)
        # 导入数据到数据库表
        DBConnector.import_data(dd_filename, dd_table_name)
        # 初始化_dd
        self._load_dd()
        # 建立SDU数据表
        sdu_table_name = self._get_table_name(TABLE_TYPE_SDU)
        sdu_fields = {'owner': 'VARCHAR(16)'}
        tags = self._get_attribute(DD_TYPE_TAG)
        for tag in tags:
            # TODO：待删除的临时打印信息
            logger.log(tag)
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
        sdu_filename = self._get_filename(FILE_TYPE_SDU)
        DataUnitService.write_file(sdu_filename, sdu_headers, sdu_content)
        # 导入数据到数据库表
        DBConnector.import_data(sdu_filename, sdu_table_name)
        # 初始化_sdu
        self._init_sdu()
        # 建立TDU数据表
        tdu_fields = {'timestamp': 'VARCHAR(16)'}  # String of Unix Millisecond Timestamp
        metrics = self._get_attribute(DD_TYPE_METRIC)
        for metric in metrics:
            tdu_fields[metric] = 'VARCHAR(16)'
        for owner_disc, owner_oid in owner_mappings.items():
            tdu_table_name = self._get_table_name(TABLE_TYPE_TDU, owner_oid)
            DBConnector.init_table(tdu_table_name, tdu_fields)
            raw_data = self._read_tdu_raw_data(owner_disc)
            raw_data.rename(columns=metric_mappings, inplace=True)
            raw_data['timestamp'] = raw_data['timestamp'].apply(lambda x: moment(x).format('YYYYMMDD HHmmss.SSS ZZ')).astype('string')
            # pd.to_datetime(raw_data['timestamp'])
            tdu_filename = self._get_filename(FILE_TYPE_TDU, owner_oid)
            DataUnitService.write_file(tdu_filename, None, raw_data)
            # 导入数据到数据库表
            DBConnector.import_data(tdu_filename, tdu_table_name)
        # TODO：待验证
        self._init_tdu()

    def _read_tdu_raw_data(self, owner):
        filename = path.join(self._config['ds_path'], owner + FILE_EXT[FILE_TYPE_TDU_RAW])
        data = pd.read_csv(filename, dtype=str)
        return data

    def _get_filename(self, file_type, *args):
        if file_type in FILE_EXT:
            if file_type == FILE_TYPE_DD or file_type == FILE_TYPE_SDU:
                return path.join(self._config['bak_path'], self.service_id + FILE_EXT[file_type])
            else:
                if file_type == FILE_TYPE_TDU:
                    if isinstance(args[0], list):
                        filenames = []
                        for owner_id in args[0]:
                            filenames.append(path.join(self._config['bak_path'], self.service_id + '_' + owner_id + FILE_EXT[file_type]))
                        return filenames
                    else:
                        if isinstance(args[0], str):
                            return path.join(self._config['bak_path'], self.service_id + '_' + args[0] + FILE_EXT[file_type])
                        else:
                            raise TypeError(logger.error([5003]))
                else:
                    raise ValueError(logger.error([5004, file_type]))
        else:
            raise ValueError(logger.error([5004, file_type]))

    def import_data(self, file_type, table_name, *args):
        filename = self._get_filename(file_type, *args)
        DBConnector.import_data(filename, table_name)

    def export_data(self, output_filename):
        pass

    @staticmethod
    def write_file(output_filename, headers, content):
        if headers is None and isinstance(content, pd.DataFrame):
            content.to_csv(output_filename, index=False)
        else:
            with open(output_filename, 'w', newline='') as file_handler:
                if isinstance(content, list):
                    if isinstance(headers, str):
                        if len(headers) > 0:
                            file_handler.write(headers + '\n')
                    else:
                        if isinstance(headers, list):
                            file_handler.write(','.join(headers) + '\n')
                        else:
                            raise TypeError(logger.error([5005]))
                    for line in content:
                        file_handler.write(line + '\n')
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
                            raise TypeError(logger.error([5006]))
                    else:
                        raise TypeError(logger.error([5007]))


class TDUProcessor(object):
    __slots__ = ('_tdus', '_owners', '_metrics')

    def __init__(self, service_id, cache_size=CACHE_MAX_SIZE_DEFAULT, time_to_live=CACHE_TTL_DEFAULT):
        dd_owners = DBConnector.get_dd(service_id, DD_TYPE_OWNER)
        dd_metrics = DBConnector.get_dd(service_id, DD_TYPE_METRIC)
        self._tdus = TTLCache(maxsize=cache_size, ttl=time_to_live, timer=datetime.now)
        self._owners = dd_owners['ddid'].str[1:].tolist()
        self._owners.sort()
        self._metrics = dd_metrics['ddid'].str[1:].tolist()
        self._metrics.sort()

    @property
    def owners(self):
        return self._owners

    @property
    def metrics(self):
        return self._metrics


class DataUnit(object):

    def __init__(self, service_id):
        self._service_id = service_id

    @property
    def service_id(self):
        return self._service_id

    @abstractmethod
    def query(self, include: dict = None, exclude: dict = None, scope: dict = None):
        pass


class TimeDataUnit(DataUnit):

    def __init__(self, service_id, owner_id, tdu_processor):
        super().__init__(service_id)
        self._owner_id = owner_id
        self._processor = tdu_processor

    def query(self, include: dict = None, exclude: dict = None, scope: dict = None):
        # 参数校验
        if PV_TDU_QUERY_INCLUDE.validate('include', include):
            pass
        else:
            logger.warning([2000])


class SpaceDataUnit(DataUnit):

    def __init__(self, service_id):
        super().__init__(service_id)
        dd_tag = DBConnector.get_dd(service_id, DD_TYPE_TAG)
        dd_tag_value = DBConnector.get_dd(service_id, DD_TYPE_TAG_VALUE)
        self._tags = []
        self._tag_definition = {}
        for index, row in dd_tag.iterrows():
            tag_oid = row['ddid'][1:]
            self._tag_definition[tag_oid] = {}
            self._tags.append(tag_oid)
        for index, row in dd_tag_value.iterrows():
            tag_value_oid = row['ddid'][1:]
            tag_oid = row['oid_mask'][:16]
            tag_value_mask = row['oid_mask'][16:]
            self._tag_definition[tag_oid][tag_value_oid] = tag_value_mask
        self._tags.sort()

    @property
    def tags(self):
        return self._tags

    def query(self, include: dict = None, exclude: dict = None, scope: dict = None):
        pass
