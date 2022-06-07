import csv
import yaml
import sqlite3
import connectorx as cx
import pandas as pd
from os import path
import hashlib
from mts.commons import logger, Singleton
from mts.commons.const import *


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

    @staticmethod
    def checksum(filename):
        hash_md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()


class DBHandler(Singleton):
    _db_url = None  # sqlite:///path/to/db
    _connection = None
    timezone = None

    def __init__(self, db_url: str = None, timezone: str = DEFAULT_TZ):
        super().__init__()
        if db_url is not None:
            self.register(db_url)
        if self.set_timezone(timezone):
            pass
        else:
            raise ValueError(logger.warning([5708, timezone]))

    def set_timezone(self, timezone: str = DEFAULT_TZ):
        if PV_TZ.validate('timezone', timezone):
            self.timezone = pd.Timedelta(timezone + ':00')
            return True
        else:
            return False

    def register(self, db_url: str):
        self._db_url = db_url
        self.disconnect()

    def query(self, table_name: str, fields: list = None, condition: str = None):
        # table_name = DBHandler.get_table_name(service_id, table_type, owner_id)
        sql = 'SELECT '
        if fields is None:
            sql = sql + '* from ' + table_name
        else:
            sql = sql + ', '.join(fields) + ' from ' + table_name
        if condition is not None:
            sql = sql + ' WHERE ' + condition
        try:
            result = cx.read_sql(self._db_url, sql)
        except (ModuleNotFoundError, AttributeError, ValueError, TypeError, RuntimeError) as e:
            result = pd.read_sql_query(sql, self.connect())
        return result

    def connect(self):
        if self._connection is None:
            if self._db_url is None:
                raise ValueError(logger.error([5700]))
            db_path = self._db_url.split('://')[1]
            self._connection = sqlite3.connect(db_path)
        return self._connection

    def get_cursor(self):
        cursor = self.connect().cursor()
        return cursor

    def commit(self):
        self.connect().commit()

    def is_connect(self):
        return self._connection is not None

    def disconnect(self):
        if self.is_connect():
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
                # 如果存在table，先drop掉
                sql = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(table_name)
                cursor.execute(sql)
                if cursor.fetchone()[0] == 1:
                    sql = "DROP TABLE {}".format(table_name)
                    cursor.execute(sql)
                # 创建table
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

    def export_data(self, output_dir: str, table_name: str, owner_id: str = None):
        output_filename = table_name + '.csv'
        output_filename = path.join(output_dir, output_filename)
        df = self.query(table_name)
        df.to_csv(output_filename, index=False)
