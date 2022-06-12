from abc import abstractmethod
import pandas as pd
from mts.commons import logger
from mts.commons.const import *
from mts.core.handler import DBHandler
from mts.core.id import DataDictionaryId, Service


class DataUnit(object):

    def __init__(self, service_id: str):
        service_code = Service.to_service_code(service_id)
        if Service.valid(service_code):
            self._service = Service(service_id)
        else:
            raise ValueError(logger.error([4500]))
        self._db = DBHandler()
        self._table_name = BLANK
        self._set_table_name()
        if self._db.exist_table(self._table_name):
            pass
        else:
            self.init_db()

    @abstractmethod
    def _set_table_name(self):
        pass

    @property
    def sid(self):
        return self._service.id

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

    def import_data(self, filename):
        self._db.import_data(filename, self._table_name)

    def export_data(self, output_dir):
        self._db.export_data(output_dir, self._table_name)

    def sync_db(self, filename, init_flag=False):
        if init_flag:
            self.init_db()
        self.import_data(filename)
        self._after_sync()

    def _after_sync(self):
        pass  # pragma: no cover

    def init_db(self):
        self._db.init_table(self._table_name, self.fields())


class DataDictionary(DataUnit):

    def __init__(self, service_id: str):
        super().__init__(service_id)
        self._data = None
        self.load_data()

    def load_data(self):
        self._data = self._db.query(self._table_name)

    def _after_sync(self):
        self.load_data()

    def _set_table_name(self):
        self._table_name = '_'.join([TABLE_PREFIX_DD, self.sid])

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
        if (KEY_DD_TYPE in kwargs) and PV_DD_ADD.validate(KEY_DD_TYPE, kwargs[KEY_DD_TYPE]):
            if (KEY_DESC in kwargs) and PV_DD_ADD.validate(KEY_DESC, kwargs[KEY_DESC]):
                mask = MASK_DEFAULT
                ddid = None
                if (KEY_OID in kwargs) and PV_DD_ADD.validate(KEY_OID, kwargs[KEY_OID]):
                    ddid = DataDictionaryId(ddid=kwargs[KEY_DD_TYPE]+kwargs[KEY_OID])
                if (KEY_MASK in kwargs) and PV_DD_ADD.validate(KEY_MASK, kwargs[KEY_MASK]):
                    mask = kwargs[KEY_MASK]
                duplicated = True
                res = self.query(dd_type=kwargs[KEY_DD_TYPE], desc=[kwargs[KEY_DESC]])
                if res.empty:
                    duplicated = False
                if duplicated:
                    logger.warning([5804, kwargs])
                    return res[KEY_DDID][0]
                else:
                    if ddid is None:
                        ddid = DataDictionaryId(dd_type=kwargs[KEY_DD_TYPE], service_id=self.sid)
                    ddid_str = str(ddid)
                    oid_mask = ddid.oid + mask
                    data = {FIELD_DDID: ddid_str, FIELD_DESC: kwargs[KEY_DESC], FIELD_OID_MASK: oid_mask}
                    self._db.add(data, self._table_name)
                    self.load_data()
                    return ddid_str
            else:
                raise ValueError(logger.error([5805, kwargs]))
        else:
            raise ValueError(logger.error([5805, kwargs]))

    def remove(self, ddid: str):
        if PV_DD_REMOVE.validate(KEY_DDID, ddid):
            self._db.remove(self._table_name, FIELD_DDID + '="' + ddid + '"')
            self.load_data()
        else:
            raise ValueError(logger.error([5802]))

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
            res = self.query(oid=oid)[FIELD_DESC].tolist()
        else:
            ddid = dd_type + oid
            res = self.query(ddid=[ddid])[FIELD_DESC].tolist()
        if 1 == len(res):
            return res[0]
        else:
            return None


class DataFragments(object):
    def __init__(self, service_id: str):
        service_code = Service.to_service_code(service_id)
        if Service.valid(service_code):
            self._service = Service(service_id)
        else:
            raise ValueError(logger.error([6200]))
        self._data = {}
        self._labels = {}
        self._key_type = None
        self._value_type = None
        self.init_key_and_value()
        self.load_data()

    def load_data(self):
        dd = DataDictionary(self.sid)
        keys = dd.query(dd_type=self._key_type)
        values = dd.query(dd_type=self._value_type)
        label_01 = keys.copy()
        label_01[FIELD_OID] = label_01[FIELD_DDID].apply(lambda x: x[1:])
        label_01.drop(columns=[FIELD_DDID, FIELD_OID_MASK], inplace=True)
        label_01[FIELD_MASK] = 0
        label_02 = values.copy()
        label_02[FIELD_OID] = label_02[FIELD_OID_MASK].apply(lambda x: x[0:16])
        label_02[FIELD_MASK] = label_02[FIELD_OID_MASK].apply(lambda x: int(x[16:], 16))
        label_02.drop(columns=[FIELD_DDID, FIELD_OID_MASK], inplace=True)
        self._labels = pd.concat([label_01, label_02])
        self._labels.reset_index(drop=True, inplace=True)
        self._data = self._labels.sort_values(FIELD_MASK).drop_duplicates(FIELD_OID, keep='last').set_index(FIELD_OID).to_dict()[FIELD_MASK]

    def exists(self, desc: str, mask: int = 0):
        oid = self.oid(desc)
        if oid is None:
            return False
        else:
            res = self.desc(oid, mask)
            if BLANK == res:
                return False
            else:
                if res == desc:
                    return True
                else:
                    return False

    def exists_oid(self, oid: str):
        return oid in self._data

    def desc(self, oid: str, mask: int = 0):
        res = self._labels[(self._labels[FIELD_OID] == oid) & (self._labels[FIELD_MASK] == mask)][FIELD_DESC].tolist()
        if 1 == len(res):
            return res[0]
        else:
            return BLANK

    def oid(self, desc: str):
        res = self._labels[(self._labels[FIELD_DESC] == desc)].drop_duplicates(FIELD_OID, keep='last')[FIELD_OID].tolist()
        if 1 == len(res):
            return res[0]
        else:
            return None

    @abstractmethod
    def init_key_and_value(self):
        pass

    @property
    def sid(self):
        return self._service.id

    @property
    def value(self):
        res = list(self._data.keys())
        res.sort()
        return res


class Metrics(DataFragments):

    def __init__(self, service_id: str):
        super().__init__(service_id)

    def init_key_and_value(self):
        self._key_type = DD_TYPE_METRIC
        self._value_type = DD_TYPE_METRIC_VALUE


class Tags(DataFragments):

    def __init__(self, service_id: str):
        super().__init__(service_id)

    def init_key_and_value(self):
        self._key_type = DD_TYPE_TAG
        self._value_type = DD_TYPE_TAG_VALUE


class TimeDataUnit(DataUnit):

    _metrics = {}

    def __init__(self, ddid):
        ddid_obj = DataDictionaryId(ddid=ddid)
        if ddid_obj.dd_type == DD_TYPE_OWNER:
            self._ddid = ddid_obj
            TimeDataUnit.add_metrics(ddid_obj.sid)
            super().__init__(self._ddid.sid)
        else:
            raise ValueError(logger.warning([2000]))

    def _set_table_name(self):
        self._table_name = '_'.join([TABLE_PREFIX_TDU, self.sid, self.oid])

    @property
    def oid(self):
        return self._ddid.oid

    def fields(self):
        fields = {FIELD_TIMESTAMP: 'VARCHAR(16) PRIMARY KEY'}  # String of Unix Millisecond Timestamp
        for metric in self.metrics.value:
            fields[metric] = 'INT'
        return fields

    @property
    def metrics(self):
        return TimeDataUnit._metrics[self.sid]

    @classmethod
    def add_metrics(cls, service_id: str):
        cls._metrics[service_id] = Metrics(service_id)

    def reset_metrics(self, force: bool = False):
        if force:
            TimeDataUnit._metrics[self.sid] = Metrics(self.sid)
        else:
            if self.sid in TimeDataUnit._metrics:
                pass
            else:
                TimeDataUnit._metrics[self.sid] = Metrics(self.sid)

    def map_desc(self, oid: str, mask: int = 0):
        return self.metrics.desc(oid, mask)

    def map_oid(self, desc: str):
        return self.metrics.oid(desc)

    def _after_sync(self):
        self.reset_metrics()

    def query(self, **kwargs):
        fields = [FIELD_TIMESTAMP]
        if (KEY_METRIC in kwargs) and PV_TDU_QUERY.validate(KEY_METRIC, kwargs[KEY_METRIC]):
            for oid in kwargs[KEY_METRIC]:
                if self.metrics.exists_oid(oid):
                    fields.append(oid)
        if (KEY_DESC in kwargs) and PV_TDU_QUERY.validate(KEY_DESC, kwargs[KEY_DESC]):
            for desc in kwargs[KEY_DESC]:
                oid = self.map_oid(desc)
                if oid is not None:
                    fields.append(oid)
        condition = None
        if (KEY_INTERVAL in kwargs) and PV_TDU_QUERY.validate(KEY_INTERVAL, kwargs[KEY_INTERVAL]):
            c = FIELD_TIMESTAMP + " >= '{0}' AND " + FIELD_TIMESTAMP + " <= '{1}'"
            date_from = moment(kwargs[KEY_INTERVAL][KEY_FROM]).format(MOMENT_FORMAT)
            date_to = moment(kwargs[KEY_INTERVAL][KEY_TO]).format(MOMENT_FORMAT)
            condition = c.format(*[date_from, date_to])
        else:
            if (KEY_ANY in kwargs) and PV_TDU_QUERY.validate(KEY_ANY, kwargs[KEY_ANY]):
                claus = []
                c = FIELD_TIMESTAMP + " = '{0}'"
                for item in kwargs[KEY_ANY]:
                    claus.append(c.format(*[moment(item).format(MOMENT_FORMAT)]))
                condition = ' OR '.join(claus)
        if 1 == len(fields):
            fields = None
        res = self._db.query(self._table_name, fields, condition)
        res[FIELD_TIMESTAMP] = pd.to_datetime(res[FIELD_TIMESTAMP], unit='s') + self._db.timezone
        res.set_index(FIELD_TIMESTAMP, drop=True, inplace=True)
        res = res.apply(pd.to_numeric)
        return res

    def add(self, **kwargs):
        if (len(kwargs) > 0) and PV_TDU_ADD.validates(kwargs):
            data = {}
            dd = DataDictionary(self.sid)
            for key, value in kwargs['data'].items():
                if self.metrics.exists(key):
                    data[self.metrics.oid(key)] = value
                else:
                    ddid = dd.add(dd_type=DD_TYPE_METRIC, desc=key)
                    oid = ddid[1:]
                    self._db.add_column(self._table_name, oid, 'INT')
                    self.reset_metrics(True)
                    data[oid] = value
            data[FIELD_TIMESTAMP] = moment(kwargs[KEY_TS]).format(MOMENT_FORMAT)
            self._db.add(data, self._table_name)
        else:
            raise ValueError(logger.warning([2001]))

    def remove(self, ts: str):
        ts_value = moment(ts).format(MOMENT_FORMAT)
        self._db.remove(self._table_name, FIELD_TIMESTAMP + '="' + ts_value + '"')
