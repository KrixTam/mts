from abc import abstractmethod, ABC
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
        if self._db.exist_table(self._table_name):
            pass
        else:
            self.init_db()
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
