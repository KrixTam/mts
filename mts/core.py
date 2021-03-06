import csv
import yaml
import sqlite3
import threading
import connectorx as cx
import pandas as pd
from os import path, getpid
from abc import abstractmethod
from random import getrandbits
from cachetools import TTLCache
from datetime import datetime
# from moment import moment
from ni.config import Config
from mts.const import *
from mts.utils import logger
from pandas.io.sql import DatabaseError
from .singleton import Singleton


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
        """??????????????????"""
        if 'epoch' in settings and isinstance(settings['epoch'], int):
            ObjectId._epoch = settings['epoch'] & TIMESTAMP_MASK
        if 'service_code' in settings and isinstance(settings['service_code'], int):
            ObjectId._service_code = settings['service_code'] & SERVICE_CODE_MASK

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
        if (service_code < SERVICE_CODE_MIN) or (service_code > SERVICE_CODE_MAX):
            raise ValueError(logger.error([5500, service_code, SERVICE_CODE_MIN, SERVICE_CODE_MAX]))
        return service_code, last_ts, pid_code, sequence

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
        new_id = (service_code << SERVICE_CODE_BITS_SHIFT) | (ts << TIMESTAMP_BITS_SHIFT) | (
                    pid_code << PID_CODE_BITS_SHIFT) | sequence
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
                    if 'service_code' in kwargs:
                        sc = kwargs['service_code']
                    else:
                        if 'service_id' in kwargs:
                            sc = int(kwargs['service_id'], 8)
                    if sc is None:
                        raise ValueError(logger.error([5601, kwargs]))
                    else:
                        dd_type = kwargs[KEY_DD_TYPE]
                        if isinstance(dd_type, str) and dd_type in DD_TYPES:
                            dd_type = int(dd_type, 16)
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


class DataFileHandler(object):

    @staticmethod
    def load_json(filename):
        obj_json = None
        if path.exists(filename):
            with open(filename, 'r') as f:
                obj_json = yaml.safe_load(f)
                logger.info([5900, filename])
        else:
            logger.warning([5901, filename])
        return obj_json


class DBHandler(Singleton):
    _db_url = None  # sqlite:///path/to/db
    _connection = None
    _tz = pd.Timedelta(DEFAULT_TZ)

    def __init__(self, db_path=None, tz=None):
        super().__init__()
        if tz is None:
            if self._tz is None:
                self.set_tz(DEFAULT_TZ)
        else:
            self.set_tz(tz)
        if db_path is not None:
            self.register(db_path)

    def set_tz(self, tz=DEFAULT_TZ):
        if PV_TZ.validate('tz', tz):
            self._tz = pd.Timedelta(tz)
            return True
        else:
            return False

    def tz(self):
        return self._tz

    def register(self, db_url: str):
        self._db_url = db_url
        self.disconnect()

    def query(self, service_id: str, table_type: str, fields: list = None, condition: str = None, owner_id: str = None):
        table_name = DBHandler.get_table_name(service_id, table_type, owner_id)
        sql = 'SELECT '
        if fields is None:
            sql = sql + '* from ' + table_name
        else:
            sql = sql + ', '.join(fields) + ' from ' + table_name
        if condition is not None:
            sql = sql + ' WHERE ' + condition
        result = None
        try:
            result = cx.read_sql(self._db_url, sql)
        except (ModuleNotFoundError, AttributeError, ValueError, RuntimeError) as e:
            result = pd.read_sql_query(sql, self.connect())
        return result

    def connect(self):
        if self._connection is None:
            db_path = self._db_url.split('://')[1]
            self._connection = sqlite3.connect(db_path)
        return self._connection

    def get_cursor(self):
        cursor = self.connect().cursor()
        return cursor

    def commit(self):
        self.connect().commit()

    def disconnect(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def get_fields(self, table_name: str):
        cursor = self.get_cursor()
        res = cursor.execute("PRAGMA table_info('" + table_name + "')").fetchall()
        fields = []
        for item in res:
            fields.append(item[1])
        fields.sort()
        return fields

    def get_tables(self):
        cursor = self.get_cursor()
        res = cursor.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").fetchall()
        tables = []
        for item in res:
            tables.append(item[0])
        tables.sort()
        return tables

    def exist_table(self, table_name: str):
        return table_name in self.get_tables()

    def init_table(self, table_name: str, fields: dict):
        if self._db_url is None:
            raise ValueError(logger.error([5700]))
        else:
            if PV_DB_DEFINITION.validate('field', fields):
                cursor = self.get_cursor()
                # ????????????table??????drop???
                sql = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(table_name)
                cursor.execute(sql)
                if cursor.fetchone()[0] == 1:
                    sql = "DROP TABLE {}".format(table_name)
                    cursor.execute(sql)
                # ??????table
                fields_def = []
                for key, value in fields.items():
                    fields_def.append('[' + key + '] ' + value)
                sql = "CREATE TABLE IF NOT EXISTS {}({})".format(table_name, ", ".join(fields_def))
                cursor.execute(sql)
                self.commit()
                cursor.close()
            else:
                raise ValueError(logger.error([5703]))

    def add(self, data: dict, table_name: str):
        cursor = self.get_cursor()
        keys = []
        values = []
        for key, value in data.items():
            keys.append(key)
            values.append(str(value))
        columns = ', '.join(keys)
        values = "', '".join(values)
        sql = "INSERT OR IGNORE INTO {} ({}) VALUES ('{}');".format(table_name, columns, values)
        # logger.log(sql)
        cursor.execute(sql)
        self.commit()
        cursor.close()

    def add_column(self, table_name: str, field_name: str, field_def: str):
        cursor = self.get_cursor()
        sql = "ALTER TABLE {} ADD COLUMN {} {};".format(table_name, field_name, field_def)
        cursor.execute(sql)
        self.commit()
        cursor.close()

    def import_data(self, filename: str, table_name: str):
        cursor = self.get_cursor()
        with open(filename, 'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [tuple([row[field] for field in dr.fieldnames]) for row in dr]
            sql = 'INSERT OR IGNORE INTO ' + table_name + ' ' + str(tuple(dr.fieldnames)).replace("'", '')
            sql = sql + ' VALUES (' + ', '.join(list('?' * len(dr.fieldnames))) + ');'
            cursor.executemany(sql, to_db)
            self.commit()
        cursor.close()

    def export_data(self, output_dir: str, service_id: str, table_type: str, owner_id: str = None):
        output_filename = None
        if table_type == TABLE_TYPE_DD or table_type == TABLE_TYPE_SDU:
            output_filename = service_id + FILE_EXT_BY_TABLE_TYPE[table_type]
        else:
            if table_type == TABLE_TYPE_TDU:
                if owner_id is None:
                    raise ValueError(logger.error([5707]))
                else:
                    output_filename = service_id + '_' + owner_id + FILE_EXT_BY_TABLE_TYPE[table_type]
            else:
                raise ValueError(logger.error([5705, table_type]))
        output_filename = path.join(output_dir, output_filename)
        df = self.query(service_id=service_id, table_type=table_type, owner_id=owner_id)
        df.to_csv(output_filename, index=False)

    @staticmethod
    def get_table_name(service_id: str, table_type: str, owner_id: str = None):
        if PV_SERVICE.validate(KEY_SERVICE_ID, service_id):
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
                            if PV_ID.validate(KEY_OID, owner_id):
                                return 'tdu_' + service_id + '_' + owner_id
                            else:
                                raise ValueError(logger.error([5706]))
            else:
                raise ValueError(logger.error([5701, TABLE_TYPE]))
        else:
            raise ValueError(logger.error([5704]))


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
        # TODO
        assert not self._config.is_default('db')
        DBHandler.register(self._config['db'])
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
    # TODO:
    # 1. SDU???TDU????????????????????????????????????DUS???
    #
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
                                'desc': 'xxx',
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
                                            'desc': {'type': 'string'},
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
            if self._config.is_default() or self._config.is_default(KEY_SERVICE_ID) or self._config.is_default('ds_path'):
                raise ValueError(logger.error([5001]))
            else:
                self.init_tables()
        else:
            if self._config.is_default(KEY_SERVICE_ID) or self._config.is_default('ds_path'):
                raise ValueError(logger.error([5002]))
            else:
                self._load_dd()
                self._init_sdu()
                # TODO????????????
                self._init_tdu()

    @property
    def service_id(self):
        return self._config[KEY_SERVICE_ID]

    @property
    def service_code(self):
        return int(self.service_id, 8)

    def _get_attribute(self, dd_type):
        data = self._dd[self._dd[KEY_DDID].str[0] == dd_type]
        res = data[KEY_DDID].str[1:].tolist()
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

    def desc(self, ddid: str = None, dd_type: str = None, oid: str = None):
        data = self._dd
        if ddid is None:
            if dd_type is None:
                if oid is None:
                    pass
                else:
                    data = self._dd[self._dd[KEY_DDID].str[1:] == oid]
            else:
                if oid is None:
                    data = self._dd[self._dd[KEY_DDID].str[0] == dd_type]
                else:
                    ddid_val = dd_type + oid
                    data = self._dd[self._dd[KEY_DDID] == ddid_val]
        else:
            data = self._dd[self._dd[KEY_DDID] == ddid]
        res = data['desc'].values.tolist()
        res.sort()
        return res

    def _load_dd(self):
        self._dd = DBHandler().query(self.service_id, TABLE_TYPE_DD)

    def _init_sdu(self):
        dd_tag = DataDictionary.query_dd(self.service_id, DD_TYPE_TAG)
        logger.log(dd_tag)
        dd_tag_value = DataDictionary.query_dd(self.service_id, DD_TYPE_TAG_VALUE)
        logger.log(dd_tag_value)
        self._sdu = SpaceDataUnit(self.service_id)

    def _init_tdu(self):
        # TODO????????????TDU
        self._tdu = TDUProcessor(self.service_id)

    def _get_table_name(self, table_type: str, owner_id: str = None):
        return DBHandler.get_table_name(self.service_id, table_type, owner_id)

    def init_tables(self):
        db = DBHandler()
        # ??????DD?????????
        dd_table_name = self._get_table_name(TABLE_TYPE_DD)
        db.init_table(dd_table_name, FIELDS_DD)
        # ?????????ddid??????
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
                line = str(DataDictionaryId(dd_type=DD_TYPE_TAG_VALUE, service_code=self.service_code)) + ',' + tag_value['desc'] + ',' + mask_str
                ddid_content.append(line)
                for owner in tag_value['owners']:
                    if owner in tag_value_mappings[tag_oid]:
                        tag_value_mappings[tag_oid][owner] = tag_value_mappings[tag_oid][owner] + mask
                    else:
                        tag_value_mappings[tag_oid][owner] = mask
        dd_filename = self._get_filename(FILE_TYPE_DD)
        DataUnitService.write_file(dd_filename, DD_HEADERS, ddid_content)
        # ???????????????????????????
        db.import_data(dd_filename, dd_table_name)
        # ?????????_dd
        self._load_dd()
        # ??????SDU?????????
        sdu_table_name = self._get_table_name(TABLE_TYPE_SDU)
        sdu_fields = {'owner': 'VARCHAR(16)'}
        tags = self._get_attribute(DD_TYPE_TAG)
        for tag in tags:
            # TODO?????????????????????????????????
            logger.log(tag)
            sdu_fields[tag] = 'INT'
        db.init_table(sdu_table_name, sdu_fields)
        # ?????????SDU??????
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
        # ???????????????????????????
        db.import_data(sdu_filename, sdu_table_name)
        # ?????????_sdu
        self._init_sdu()
        # ??????TDU?????????
        tdu_fields = {'timestamp': 'VARCHAR(16)'}  # String of Unix Millisecond Timestamp
        metrics = self._get_attribute(DD_TYPE_METRIC)
        for metric in metrics:
            tdu_fields[metric] = 'VARCHAR(16)'
        for owner_desc, owner_oid in owner_mappings.items():
            tdu_table_name = self._get_table_name(TABLE_TYPE_TDU, owner_oid)
            db.init_table(tdu_table_name, tdu_fields)
            raw_data = self._read_tdu_raw_data(owner_desc)
            raw_data.rename(columns=metric_mappings, inplace=True)
            raw_data['timestamp'] = raw_data['timestamp'].apply(lambda x: moment(x).format('X.SSS')).astype('string')
            # raw_data['timestamp'] = raw_data['timestamp'].apply(lambda x: moment(x).format(TIMESTAMP_FORMAT)).astype('string')
            # pd.to_datetime(raw_data['timestamp'])
            tdu_filename = self._get_filename(FILE_TYPE_TDU, owner_oid)
            DataUnitService.write_file(tdu_filename, None, raw_data)
            # ???????????????????????????
            db.import_data(tdu_filename, tdu_table_name)
        # TODO????????????
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
        db.import_data(filename, table_name)

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
    __slots__ = ('_cache', '_owners', '_metrics')

    def __init__(self, service_id, cache_size=CACHE_MAX_SIZE_DEFAULT, time_to_live=CACHE_TTL_DEFAULT):
        self._cache = TTLCache(maxsize=cache_size, ttl=time_to_live, timer=datetime.now)
        self._owners = DataDictionary.query_oid(service_id, DD_TYPE_OWNER)
        self._metrics = DataDictionary.query_oid(service_id, DD_TYPE_METRIC)

    @property
    def owners(self):
        return self._owners

    @property
    def metrics(self):
        return self._metrics


class DataUnit(object):

    def __init__(self, service_id: str):
        if PV_SERVICE.validate(KEY_SERVICE_ID, service_id):
            self._service_id = service_id
        else:
            raise ValueError(logger.error([4500]))
        self._db = DBHandler()

    @property
    def service_id(self):
        return self._service_id

    @abstractmethod
    def fields(self):
        pass

    @abstractmethod
    def query(self, **kwargs):
        pass

    @abstractmethod
    def add(self, **kwargs):
        pass

    @abstractmethod
    def remove(self, **kwargs):
        pass

    @abstractmethod
    def sync_db(self, filename, init_flag=False):
        pass

    @abstractmethod
    def import_data(self, filename, data_dict=None):
        pass

    @abstractmethod
    def export_data(self, output_dir):
        pass


class DataDictionary(DataUnit):

    def __init__(self, service_id: str):
        super().__init__(service_id)
        self._data = None
        if self._db.exist_table(DBHandler.get_table_name(service_id, TABLE_TYPE_DD)):
            pass
        else:
            self._db.init_table(DBHandler.get_table_name(service_id, TABLE_TYPE_DD), FIELDS_DD)
        self.reload()

    def reload(self):
        self._data = self._db.query(self._service_id, TABLE_TYPE_DD)

    def sync_db(self, filename, init_flag=False):
        table_name = DBHandler.get_table_name(self._service_id, TABLE_TYPE_DD)
        if init_flag:
            self._db.init_table(table_name, FIELDS_DD)
        self._db.import_data(filename, table_name)
        self.reload()

    def get_oid(self, dd_type: str):
        return self.query(True, dd_type=dd_type)

    def map_oid(self, desc: str, dd_type: str = None):
        if dd_type is None:
            res = self.query(True, desc=[desc])
        else:
            res = self.query(True, dd_type=dd_type, desc=[desc])
        if 1 == len(res):
            return res[0]
        else:
            return None

    def map_desc(self, oid: str, dd_type: str = None):
        if dd_type is None:
            res = self.query(oid=oid)[FIELD_DESC]
        else:
            ddid = dd_type + oid
            res = self.query(ddid=[ddid])[FIELD_DESC]
        if 1 == len(res.index):
            return res[0]
        else:
            return None

    @staticmethod
    def query_oid(service_id: str, dd_type: str):
        data = DataDictionary.query_dd(service_id, dd_type)
        res = data[FIELD_DDID].apply(lambda x: x[1:]).tolist()
        res.sort()
        return res

    @staticmethod
    def query_dd(service_id: str, dd_type: str):
        if PV_SERVICE.validate(KEY_SERVICE_ID, service_id):
            if dd_type in DD_TYPES:
                data = DBHandler().query(service_id, TABLE_TYPE_DD)
                res = data[data[FIELD_DDID].str[0] == dd_type]
                return res
            else:
                raise ValueError(logger.error([5801, dd_type]))
        else:
            raise ValueError(logger.error([5802]))

    def fields(self):
        return FIELDS_DD

    def query(self, oid_only=False, **kwargs):
        res = self._data.copy()
        if KEY_DD_TYPE in kwargs:
            if PV_DD_QUERY.validate(KEY_DD_TYPE, kwargs[KEY_DD_TYPE]):
                if kwargs[KEY_DD_TYPE] in DD_TYPES:
                    res = res[res[FIELD_DDID].str[0] == kwargs[KEY_DD_TYPE]]
                else:
                    raise ValueError(logger.error([5801, kwargs[KEY_DD_TYPE]]))
            else:
                raise ValueError(logger.error([5800, kwargs[KEY_DD_TYPE]]))
        if KEY_DESC in kwargs:
            if PV_DD_QUERY.validate(KEY_DESC, kwargs[KEY_DESC]):
                res = res[res['desc'].isin(kwargs[KEY_DESC])]
            else:
                raise ValueError(logger.error([5803]))
        if KEY_DDID in kwargs:
            if PV_DD_QUERY.validate(KEY_DDID, kwargs[KEY_DDID]):
                res = res[res[FIELD_DDID].isin(kwargs[KEY_DDID])]
            else:
                raise ValueError(logger.error([5807]))
        if KEY_OID in kwargs:
            if PV_DD_QUERY.validate(KEY_OID, kwargs[KEY_OID]):
                res = res[res[FIELD_DDID].str[1:] == kwargs[KEY_OID]]
            else:
                raise ValueError(logger.error([5808]))
        if oid_only:
            res = res[FIELD_DDID].apply(lambda x: x[1:]).tolist()
            res.sort()
        return res

    def add(self, **kwargs):
        if len(kwargs) > 0 and PV_DD_ADD.validates(kwargs):
            duplicated = True
            res = self.query(dd_type=kwargs[KEY_DD_TYPE], desc=[kwargs['desc']])
            if res.empty:
                duplicated = False
            if duplicated:
                logger.warning([5804, kwargs])
                return res[KEY_DDID][0]
            else:
                ddid = str(DataDictionaryId(dd_type=kwargs[KEY_DD_TYPE], service_id=self.service_id))
                data = {FIELD_DDID: ddid, FIELD_DESC: kwargs['desc'], FIELD_OID_MASK: kwargs['oid_mask']}
                dd_table_name = DBHandler.get_table_name(self.service_id, TABLE_TYPE_DD)
                self._db.add(data, dd_table_name)
                self.reload()
                return ddid
        else:
            raise ValueError(logger.error([5805, kwargs]))

    @staticmethod
    def append(**kwargs):
        if len(kwargs) > 0 and PV_DD_APPEND.validates(kwargs):
            sid = kwargs[KEY_SERVICE_ID]
            ddid = str(DataDictionaryId(dd_type=kwargs[KEY_DD_TYPE], service_id=sid))
            data = {FIELD_DDID: ddid, FIELD_DESC: kwargs['desc'], FIELD_OID_MASK: kwargs['oid_mask']}
            dd_table_name = DBHandler.get_table_name(sid, TABLE_TYPE_DD)
            DBHandler().add(data, dd_table_name)
            return ddid
        else:
            raise ValueError(logger.error([5806, kwargs]))

    def remove(self, **kwargs):
        # TODO ?????????
        pass

    def import_data(self, filename, data_dict=None):
        # TODO ?????????
        pass

    def export_data(self, output_dir):
        self._db.export_data(output_dir, self.service_id, TABLE_TYPE_DD)


class TimeDataUnit(DataUnit):

    def __init__(self, service_id, owner_id):
        super().__init__(service_id)
        if PV_ID.validate(KEY_OID, owner_id):
            self._owner_id = owner_id
            if self._db.exist_table(DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, owner_id)):
                self._metric = self._db.get_fields(DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, owner_id))
                self._metric.remove(FIELD_TIMESTAMP)
            else:
                self._metric = DataDictionary.query_oid(service_id, DD_TYPE_METRIC)
                tdu_fields = self.fields()
                self._db.init_table(DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, owner_id), tdu_fields)
            self._desc = {}
            data = DataDictionary.query_dd(service_id, DD_TYPE_METRIC)
            for index, row in data.iterrows():
                # logger.log(row)
                self._desc[row[FIELD_DESC]] = row[KEY_DDID][1:]
        else:
            raise ValueError(logger.error([2002]))

    @property
    def oid(self):
        return self._owner_id

    def fields(self):
        fields = {FIELD_TIMESTAMP: 'VARCHAR(16) PRIMARY KEY'}  # String of Unix Millisecond Timestamp
        for metric in self._metric:
            fields[metric] = 'INT'
        return fields

    def query(self, **kwargs):
        # ????????????
        if PV_TDU_QUERY.validates(kwargs):
            fields = None
            res = None
            if (KEY_METRIC in kwargs) and (len(kwargs[KEY_METRIC]) > 0):
                fields = kwargs[KEY_METRIC].copy()
                fields.append(FIELD_TIMESTAMP)
            condition = None
            if 'interval' in kwargs:
                c = FIELD_TIMESTAMP + " >= '{0}' AND " + FIELD_TIMESTAMP + " <= '{1}'"
                date_from = moment(kwargs['interval']['from']).format(MOMENT_FORMAT)
                date_to = moment(kwargs['interval']['to']).format(MOMENT_FORMAT)
                condition = c.format(*[date_from, date_to])
            else:
                if 'any' in kwargs:
                    claus = []
                    c = FIELD_TIMESTAMP + " = '{0}'"
                    for item in kwargs['any']:
                        claus.append(c.format(*[moment(item).format(MOMENT_FORMAT)]))
                    condition = ' OR '.join(claus)
            try:
                res = self._db.query(self._service_id, TABLE_TYPE_TDU, fields, condition, self.oid)
            except (DatabaseError, RuntimeError):
                logger.error([2003])
                res = pd.DataFrame(columns=self.fields().keys())
            finally:
                res[FIELD_TIMESTAMP] = TimeDataUnit.to_date(res[FIELD_TIMESTAMP])
                res.index = res[FIELD_TIMESTAMP]
                del res[FIELD_TIMESTAMP]
                return res
        else:
            raise ValueError(logger.error([2000]))

    @staticmethod
    def to_date(data):
        return pd.to_datetime(data, unit='s') + DBHandler().tz()

    def add(self, **kwargs):
        if len(kwargs) > 0 and PV_TDU_ADD.validates(kwargs):
            data = {}
            tdu_table_name = DBHandler.get_table_name(self.service_id, TABLE_TYPE_TDU, self.oid)
            for key, value in kwargs['data'].items():
                if key in self._desc:
                    data[self._desc[key]] = value
                else:
                    ddid = DataDictionary.append(service_id=self.service_id, dd_type=DD_TYPE_METRIC, desc=key, oid_mask='')
                    metric_id = ddid[1:]
                    self._db.add_column(tdu_table_name, metric_id, 'INT')
                    self._desc[key] = metric_id
                    self._metric.append(metric_id)
                    data[metric_id] = value
            data[FIELD_TIMESTAMP] = moment(kwargs['ts']).format('X.SSS')
            tdu_table_name = DBHandler.get_table_name(self.service_id, TABLE_TYPE_TDU, self.oid)
            self._db.add(data, tdu_table_name)
        else:
            raise ValueError(logger.warning([2001]))

    def remove(self, **kwargs):
        # TODO ?????????
        pass

    def sync_db(self, filename, init_flag=False):
        table_name = DBHandler.get_table_name(self._service_id, TABLE_TYPE_TDU, self.oid)
        if init_flag:
            tdu_fields = self.fields()
            self._db.init_table(table_name, tdu_fields)
        self._db.import_data(filename, table_name)

    def import_data(self, filename, data_dict=None):
        # TODO ?????????
        pass

    def export_data(self, output_dir):
        self._db.export_data(output_dir, self.service_id, TABLE_TYPE_TDU, self.oid)


class SpaceDataUnit(DataUnit):

    def __init__(self, service_id):
        super().__init__(service_id)
        dd_tag = DataDictionary.query_dd(service_id, DD_TYPE_TAG)
        dd_tag_value = DataDictionary.query_dd(service_id, DD_TYPE_TAG_VALUE)
        self._tags = []
        self._tag_definition = {}
        for index, row in dd_tag.iterrows():
            tag_oid = row[KEY_DDID][1:]
            self._tag_definition[tag_oid] = {}
            self._tags.append(tag_oid)
        for index, row in dd_tag_value.iterrows():
            tag_value_oid = row[KEY_DDID][1:]
            tag_oid = row[FIELD_OID_MASK][:16]
            tag_value_mask = row[FIELD_OID_MASK][16:]
            self._tag_definition[tag_oid][tag_value_oid] = int(tag_value_mask, 16)
        self._tags.sort()
        self._desc = {}
        data = DataDictionary.query_dd(service_id, DD_TYPE_OWNER)
        for index, row in data.iterrows():
            self._desc[row[FIELD_DESC]] = row[KEY_DDID][1:]
        data = DataDictionary.query_dd(service_id, DD_TYPE_TAG)
        for index, row in data.iterrows():
            self._desc[row[FIELD_DESC]] = row[KEY_DDID][1:]
        data = DataDictionary.query_dd(service_id, DD_TYPE_TAG_VALUE)
        for index, row in data.iterrows():
            self._desc[row[FIELD_DESC]] = row[KEY_DDID][1:]
        if self._db.exist_table(DBHandler.get_table_name(service_id, TABLE_TYPE_SDU)):
            pass
        else:
            self._db.init_table(DBHandler.get_table_name(service_id, TABLE_TYPE_SDU), self.fields())
        self._data = self._db.query(self.service_id, TABLE_TYPE_SDU)

    @property
    def tags(self):
        return self._tags

    def fields(self):
        fields = {FIELD_OWNER: 'VARCHAR(16) PRIMARY KEY'}
        for tag in self.tags:
            fields[tag] = 'INT DEFAULT 0'
        return fields

    def query(self, **kwargs):
        pass

    def add(self, **kwargs):
        if len(kwargs) > 0 and PV_SDU_ADD.validates(kwargs):
            data = {}
            sdu_table_name = DBHandler.get_table_name(self.service_id, TABLE_TYPE_SDU)
            for key, value in kwargs['data'].items():
                tag_oid = None
                if key in self._desc:
                    tag_oid = self._desc[key]
                else:
                    ddid = DataDictionary.append(service_id=self.service_id, dd_type=DD_TYPE_TAG, desc=key, oid_mask='')
                    tag_oid = ddid[1:]
                    DBHandler.add_column(sdu_table_name, tag_oid, 'INT DEFAULT 0')
                    self._desc[key] = tag_oid
                    self._tags.append(tag_oid)
                    self._tag_definition[tag_oid] = {}
                tag_value_all = 0
                for tag_value in value:
                    tag_value_oid = None
                    tag_value_mask = 0
                    if tag_value in self._desc:
                        tag_value_oid = self._desc[tag_value]
                        tag_value_mask = self._tag_definition[tag_oid][tag_value_oid]
                    else:
                        tag_value_mask = len(self._tag_definition[tag_oid]) + 1
                        oid_mask = tag_oid + hex_str(tag_value_mask, 16)
                        ddid = DataDictionary.append(service_id=self.service_id, dd_type=DD_TYPE_TAG_VALUE, desc=tag_value, oid_mask=oid_mask)
                        tag_value_oid = ddid[1:]
                    tag_value_all = tag_value_all + tag_value_mask
                data[tag_oid] = tag_value_all
            owner = kwargs['owner']
            if owner in self._desc:
                data[FIELD_OWNER] = self._desc[owner]
            else:
                ddid = DataDictionary.append(service_id=self.service_id, dd_type=DD_TYPE_OWNER, desc=owner, oid_mask='')
                owner_oid = ddid[1:]
                data[FIELD_OWNER] = owner_oid
            DBHandler.add(data, sdu_table_name)
        else:
            logger.error([2501])

    def remove(self, **kwargs):
        # TODO ?????????
        pass

    def sync_db(self, filename, init_flag=False):
        pass

    def import_data(self, filename, data_dict=None):
        # TODO ?????????
        '''
        data = DataFileHandler.load_json(filename)
        dd = data_dict
        if dd is None:
            dd = DataDictionary(self.service_id)
        if PV_SDU_DATA.validate('tag', data):
            records = {}
            for tag in data:
                tag_oid = dd.map_oid(desc=tag['name'])
                for value in tag['values']:
                    tag_value_oid = dd.map_oid(desc=value['desc'])
                    for owner_desc in value['owners']:
                        owner_oid = dd.map_oid(desc=owner_desc)
                        if owner_oid in records:
                            records[owner_oid][tag_oid] = records[owner_oid][tag_value_oid] + self._tag_definition[tag_oid][tag_value_oid]
                        else:
                            records[owner_oid] = {tag_oid: self._tag_definition[tag_oid][tag_value_oid]}
            for owner_oid, tags in records.items():

        else:
            raise ValueError(logger.error([2500, filename]))
        '''
        pass

    def export_data(self, output_dir):
        DBHandler.export_data(output_dir, self.service_id, TABLE_TYPE_SDU)
