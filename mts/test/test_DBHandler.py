import os
import unittest
import pandas as pd
from mts.core import DBHandler, DataDictionaryId
from mts.const import *

cwd = os.path.abspath(os.path.dirname(__file__))
output_dir = os.path.join(cwd, 'output')
db_file_name = os.path.join(output_dir, 'dbhandler')
db_url = 'sqlite://' + db_file_name


class TestDBHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if os.path.exists(db_file_name):
            os.remove(db_file_name)
        DBHandler.register(db_url)

    def test_constructor(self):
        DBHandler(DB_MODE_SD)
        self.assertEqual(DB_MODE_SD, DBHandler._mode)

    def test_all_cx(self):
        service_id = '51'
        DBHandler.set_mode(DB_MODE_CX)
        dd_table_name = DBHandler.get_table_name(service_id, TABLE_TYPE_DD)
        self.assertFalse(dd_table_name in DBHandler.get_tables())
        DBHandler.init_table(dd_table_name, FIELDS_DD)
        self.assertTrue(dd_table_name in DBHandler.get_tables())
        fields = DBHandler.get_fields(dd_table_name)
        fields.sort()
        fields_dd = list(FIELDS_DD.keys())
        fields_dd.sort()
        self.assertEqual(fields, fields_dd)
        df = DBHandler.query(service_id, TABLE_TYPE_DD)
        self.assertTrue(df.empty)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        DBHandler.import_data(dd_file_name, dd_table_name)
        df = DBHandler.query(service_id, TABLE_TYPE_DD)
        self.assertFalse(df.empty)
        df = DBHandler.query(service_id, TABLE_TYPE_DD, ['desc', 'ddid'], "desc = '颜色'")
        self.assertEqual(df['ddid'][0], '3a4059507fd30c005')
        DBHandler.set_mode(DB_MODE_SD)
        df_01 = DBHandler.query(service_id, TABLE_TYPE_DD, ['desc', 'ddid'], "desc = '颜色'")
        self.assertEqual(df['ddid'][0], df_01['ddid'][0])
        df_02 = DBHandler.query(service_id, TABLE_TYPE_DD, ['desc', 'ddid'], "desc = '苹果' OR desc = '香蕉'")
        self.assertEqual(len(df_02.index), 2)

    def test_init_table_and_get_fields_sd(self):
        DBHandler.set_mode(DB_MODE_SD)
        dd_table_name = DBHandler.get_table_name('52', TABLE_TYPE_DD)
        self.assertFalse(dd_table_name in DBHandler.get_tables())
        DBHandler.init_table(dd_table_name, FIELDS_DD)
        self.assertTrue(dd_table_name in DBHandler.get_tables())
        fields = DBHandler.get_fields(dd_table_name)
        fields.sort()
        fields_dd = list(FIELDS_DD.keys())
        fields_dd.sort()
        self.assertEqual(fields, fields_dd)
        self.assertTrue(DBHandler.exist_table(dd_table_name))
        self.assertFalse(DBHandler.exist_table('abc'))

    def test_error(self):
        with self.assertRaises(ValueError):
            DBHandler.get_table_name(52, TABLE_TYPE_DD)

    def test_add_01(self):
        DBHandler.set_mode(DB_MODE_SD)
        service_id = '53'
        sc = int(service_id, 8)
        ddid = DataDictionaryId(dd_type=DD_TYPE_METRIC, service_code=sc)
        data = {'ddid': str(ddid), 'desc': '测试项', 'oid_mask': ''}
        dd_table_name = DBHandler.get_table_name(service_id, TABLE_TYPE_DD)
        DBHandler.init_table(dd_table_name, FIELDS_DD)
        DBHandler.add(data, dd_table_name)
        res = DBHandler.query(service_id, TABLE_TYPE_DD)
        self.assertEqual(res['ddid'][0], str(ddid))

    def test_add_02(self):
        DBHandler.set_mode(DB_MODE_CX)
        service_id = '54'
        ddid = DataDictionaryId(dd_type=DD_TYPE_METRIC, service_id=service_id)
        data = {'ddid': str(ddid), 'desc': '测试项02', 'oid_mask': ''}
        dd_table_name = DBHandler.get_table_name(service_id, TABLE_TYPE_DD)
        DBHandler.init_table(dd_table_name, FIELDS_DD)
        DBHandler.add(data, dd_table_name)
        res = DBHandler.query(service_id, TABLE_TYPE_DD)
        self.assertEqual(res['ddid'][0], str(ddid))
        # 测试导出功能
        DBHandler.export_data(output_dir, service_id, TABLE_TYPE_DD)
        file_des = os.path.join(output_dir, '54.dd')
        df = pd.read_csv(file_des, dtype=str)
        self.assertEqual(1, len(df.index))
        self.assertEqual('测试项02', df['desc'][0])
        self.assertEqual(str(ddid), df['ddid'][0])

    def test_add_column(self):
        service_id = '54'
        dd_table_name = DBHandler.get_table_name(service_id, TABLE_TYPE_DD)
        DBHandler.init_table(dd_table_name, FIELDS_DD)
        self.assertFalse('abc' in DBHandler.get_fields(dd_table_name))
        DBHandler.add_column(dd_table_name, 'abc', 'REAL')
        self.assertTrue('abc' in DBHandler.get_fields(dd_table_name))

    def test_fail_set_tz(self):
        self.assertFalse(DBHandler.set_tz(8))

    def test_disconnect(self):
        DBHandler.connect()
        DBHandler.disconnect()
        self.assertTrue(True)

    def test_error_init_table_01(self):
        with self.assertRaises(ValueError):
            DBHandler.register(None)
            dd_table_name = DBHandler.get_table_name('52', TABLE_TYPE_DD)
            DBHandler.init_table(dd_table_name, FIELDS_DD)
        DBHandler.register(db_url)

    def test_error_init_table_02(self):
        with self.assertRaises(ValueError):
            dd_table_name = DBHandler.get_table_name('57', TABLE_TYPE_DD)
            DBHandler.init_table(dd_table_name, {'a123': 123})

    def test_error_export_data_01(self):
        with self.assertRaises(ValueError):
            service_id = '51'
            DBHandler.export_data(output_dir, service_id, TABLE_TYPE_TDU)

    def test_error_export_data_02(self):
        with self.assertRaises(ValueError):
            service_id = '51'
            DBHandler.export_data(output_dir, service_id, 'new_type')

    def test_error_get_table_name_01(self):
        with self.assertRaises(ValueError):
            service_id = '51'
            DBHandler.get_table_name(service_id, 'new_type')

    def test_error_get_table_name_02(self):
        with self.assertRaises(ValueError):
            service_id = '51'
            DBHandler.get_table_name(service_id, TABLE_TYPE_TDU)

    def test_error_get_table_name_03(self):
        with self.assertRaises(ValueError):
            service_id = '51'
            DBHandler.get_table_name(service_id, TABLE_TYPE_TDU, 'adfafdafdafda')


if __name__ == '__main__':
    unittest.main()
