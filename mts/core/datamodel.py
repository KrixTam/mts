from abc import abstractmethod, ABC
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
        self._table_name = None
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
        if len(kwargs) > 0 and PV_DD_ADD.validates(kwargs):
            duplicated = True
            res = self.query(dd_type=kwargs[KEY_DD_TYPE], desc=[kwargs['desc']])
            if res.empty:
                duplicated = False
            if duplicated:
                logger.warning([5804, kwargs])
                return res[KEY_DDID][0]
            else:
                ddid = str(DataDictionaryId(dd_type=kwargs[KEY_DD_TYPE], service_id=self.sid))
                data = {FIELD_DDID: ddid, FIELD_DESC: kwargs['desc'], FIELD_OID_MASK: kwargs['oid_mask']}
                self._db.add(data, self._table_name)
                self.load_data()
                return ddid
        else:
            raise ValueError(logger.error([5805, kwargs]))

    def remove(self, ddid: str):
        if PV_DD_REMOVE.validate(KEY_DDID, ddid):
            self._db.remove(self._table_name, 'ddid="' + ddid + '"')
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
            res = self.query(oid=oid)[FIELD_DESC]
        else:
            ddid = dd_type + oid
            res = self.query(ddid=[ddid])[FIELD_DESC]
        if 1 == len(res.index):
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
        self._disp = {}
        self._key_type = None
        self._value_type = None
        self.init_key_and_value()
        self.load_data()

    def load_data(self):
        dd = DataDictionary(self.sid)
        keys = dd.query(dd_type=self._key_type)
        values = dd.query(dd_type=self._value_type)
        disp_01 = keys.copy()
        disp_01[FIELD_OID] = disp_01[FIELD_DDID].apply(lambda x: x[1:])
        disp_01.drop(columns=[FIELD_DDID, FIELD_OID_MASK], inplace=True)
        disp_01[FIELD_MASK] = 0
        disp_02 = values.copy()
        disp_02[FIELD_OID] = disp_02[FIELD_OID_MASK].apply(lambda x: x[0:16])
        disp_02[FIELD_MASK] = disp_02[FIELD_OID_MASK].apply(lambda x: int(x[16:], 16))
        disp_02.drop(columns=[FIELD_DDID, FIELD_OID_MASK], inplace=True)
        self._disp = pd.concat([disp_01, disp_02])
        self._disp.reset_index(drop=True, inplace=True)
        self._data = self._disp.sort_values(FIELD_MASK).drop_duplicates(FIELD_OID, keep='last').set_index(FIELD_OID).to_dict()[FIELD_MASK]

    def desc(self, oid: str, mask: int = 0):
        res = self._disp[(self._disp[FIELD_OID] == oid) & (self._disp[FIELD_MASK] == mask)][FIELD_DESC].tolist()
        if 1 == len(res):
            return res[0]
        else:
            return BLANK

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



# class TimeDataUnit(DataUnit):
#
#     def __init__(self, ddid):
#         self._ddid = DataDictionaryId(ddid=ddid)
#         super().__init__(self._ddid.sid)
#         if self._db.exist_table(DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, owner_id)):
#             self._metric = self._db.get_fields(DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, owner_id))
#             self._metric.remove(FIELD_TIMESTAMP)
#         else:
#             self._metric = DataDictionary.query_oid(service_id, DD_TYPE_METRIC)
#             tdu_fields = self.fields()
#             self._db.init_table(DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, owner_id), tdu_fields)
#         self._desc = {}
#         data = DataDictionary.query_dd(service_id, DD_TYPE_METRIC)
#         for index, row in data.iterrows():
#             # logger.log(row)
#             self._desc[row[FIELD_DESC]] = row[KEY_DDID][1:]
#
#     def _set_table_name(self):
#         self._table_name = '_'.join([TABLE_PREFIX_TDU, self.sid, self.oid])
#
#     @property
#     def oid(self):
#         return self._ddid.oid
#
#     def fields(self):
#         fields = {FIELD_TIMESTAMP: 'VARCHAR(16) PRIMARY KEY'}  # String of Unix Millisecond Timestamp
#         for metric in self._metric:
#             fields[metric] = 'INT'
#         return fields
#
#     def query(self, **kwargs):
#         # 参数校验
#         if PV_TDU_QUERY.validates(kwargs):
#             fields = None
#             res = None
#             if (KEY_METRIC in kwargs) and (len(kwargs[KEY_METRIC]) > 0):
#                 fields = kwargs[KEY_METRIC].copy()
#                 fields.append(FIELD_TIMESTAMP)
#             condition = None
#             if 'interval' in kwargs:
#                 c = FIELD_TIMESTAMP + " >= '{0}' AND " + FIELD_TIMESTAMP + " <= '{1}'"
#                 date_from = moment(kwargs['interval']['from']).format(MOMENT_FORMAT)
#                 date_to = moment(kwargs['interval']['to']).format(MOMENT_FORMAT)
#                 condition = c.format(*[date_from, date_to])
#             else:
#                 if 'any' in kwargs:
#                     claus = []
#                     c = FIELD_TIMESTAMP + " = '{0}'"
#                     for item in kwargs['any']:
#                         claus.append(c.format(*[moment(item).format(MOMENT_FORMAT)]))
#                     condition = ' OR '.join(claus)
#             try:
#                 res = self._db.query(self._service_id, TABLE_TYPE_TDU, fields, condition, self.oid)
#             except (DatabaseError, RuntimeError):
#                 logger.error([2003])
#                 res = pd.DataFrame(columns=self.fields().keys())
#             finally:
#                 res[FIELD_TIMESTAMP] = TimeDataUnit.to_date(res[FIELD_TIMESTAMP])
#                 res.index = res[FIELD_TIMESTAMP]
#                 del res[FIELD_TIMESTAMP]
#                 return res
#         else:
#             raise ValueError(logger.error([2000]))
#
#     @staticmethod
#     def to_date(data):
#         return pd.to_datetime(data, unit='s') + DBHandler().tz()
#
#     def add(self, **kwargs):
#         if len(kwargs) > 0 and PV_TDU_ADD.validates(kwargs):
#             data = {}
#             tdu_table_name = DBHandler.get_table_name(self.service_id, TABLE_TYPE_TDU, self.oid)
#             for key, value in kwargs['data'].items():
#                 if key in self._desc:
#                     data[self._desc[key]] = value
#                 else:
#                     ddid = DataDictionary.append(service_id=self.service_id, dd_type=DD_TYPE_METRIC, desc=key, oid_mask='')
#                     metric_id = ddid[1:]
#                     self._db.add_column(tdu_table_name, metric_id, 'INT')
#                     self._desc[key] = metric_id
#                     self._metric.append(metric_id)
#                     data[metric_id] = value
#             data[FIELD_TIMESTAMP] = moment(kwargs['ts']).format('X.SSS')
#             tdu_table_name = DBHandler.get_table_name(self.service_id, TABLE_TYPE_TDU, self.oid)
#             self._db.add(data, tdu_table_name)
#         else:
#             raise ValueError(logger.warning([2001]))
#
#     def remove(self, **kwargs):
#         # TODO 待实现
#         pass
#
#     def sync_db(self, filename, init_flag=False):
#         table_name = DBHandler.get_table_name(self._service_id, TABLE_TYPE_TDU, self.oid)
#         if init_flag:
#             tdu_fields = self.fields()
#             self._db.init_table(table_name, tdu_fields)
#         self._db.import_data(filename, table_name)
#
#     def import_data(self, filename, data_dict=None):
#         # TODO 待实现
#         pass
#
#     def export_data(self, output_dir):
#         self._db.export_data(output_dir, self.service_id, TABLE_TYPE_TDU, self.oid)
