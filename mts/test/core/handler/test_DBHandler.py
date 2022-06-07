import os
import unittest
from mts.commons.singleton import _Singleton
from mts.core.handler import DBHandler, DataFileHandler
from mts.core.id import DataDictionaryId, Service
from mts.commons.const import *

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]
output_dir = os.path.join(os.getcwd(), 'output')
db_file_name = os.path.join(output_dir, 'dbhandler')
db_url = 'sqlite://' + db_file_name


class TestDBHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if os.path.exists(db_file_name):
            os.remove(db_file_name)  # pragma: no cover

    def setUp(self):
        if DBHandler in _Singleton._instances:
            del _Singleton._instances[DBHandler]

    def test_init_01(self):
        db = DBHandler()
        with self.assertRaises(ValueError):
            db.connect()

    def test_init_02(self):
        with self.assertRaises(ValueError):
            db = DBHandler(db_url, '+24:00')

    def test_fail_set_timezone(self):
        db = DBHandler()
        self.assertFalse(db.set_timezone('+24:00'))

    def test_exist_table(self):
        db = DBHandler(db_url)
        self.assertFalse(db.exist_table('51_dd'))
        db.disconnect()
        self.assertFalse(db.is_connect())

    def test_basic(self):
        db = DBHandler(db_url)
        dd_table_name = 'dd_51'
        self.assertFalse(dd_table_name in db.get_tables())
        db.init_table(dd_table_name, FIELDS_DD)
        self.assertTrue(dd_table_name in db.get_tables())
        fields = db.get_fields(dd_table_name)
        fields.sort()
        fields_dd = list(FIELDS_DD.keys())
        fields_dd.sort()
        self.assertEqual(fields, fields_dd)
        # 导入测试
        df = db.query(dd_table_name)
        self.assertTrue(df.empty)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        db.import_data(dd_file_name, dd_table_name)
        df = db.query(dd_table_name)
        self.assertFalse(df.empty)
        # 查询测试
        df = db.query(dd_table_name, ['desc', 'ddid'], "desc = '颜色'")
        self.assertEqual(df['ddid'][0], '3a4059507fd30c005')
        df_01 = db.query(dd_table_name, ['desc', 'ddid'], "desc = '颜色'")
        self.assertEqual(df['ddid'][0], df_01['ddid'][0])
        df_02 = db.query(dd_table_name, ['desc', 'ddid'], "desc = '苹果' OR desc = '香蕉'")
        self.assertEqual(len(df_02.index), 2)
        # 测试导出功能
        db.export_data(output_dir, dd_table_name)
        output_filename = os.path.join(output_dir, dd_table_name + '.csv')
        self.assertEqual(DataFileHandler.checksum(dd_file_name), DataFileHandler.checksum(output_filename))
        # 测试重复初始化表格
        db.init_table(dd_table_name, FIELDS_DD)
        df = db.query(dd_table_name)
        self.assertTrue(df.empty)

    def test_error_init_table_01(self):
        db = DBHandler()
        with self.assertRaises(ValueError):
            dd_table_name = 'dd_52'
            db.init_table(dd_table_name, FIELDS_DD)

    def test_error_init_table_02(self):
        db = DBHandler(db_url)
        with self.assertRaises(ValueError):
            dd_table_name = 'dd_52'
            db.init_table(dd_table_name, {'a123': 123})

    def test_add(self):
        db = DBHandler(db_url)
        service_id = '53'
        sc = Service.to_service_code(service_id)
        ddid = DataDictionaryId(dd_type=DD_TYPE_METRIC, service_code=sc)
        data = {'ddid': str(ddid), 'desc': '测试项', 'oid_mask': ''}
        dd_table_name = 'dd_' + service_id
        db.init_table(dd_table_name, FIELDS_DD)
        db.add(data, dd_table_name)
        res = db.query(dd_table_name)
        self.assertEqual(res['ddid'][0], str(ddid))
        import sys
        if 'connectorx' in sys.modules:
            cx_module = sys.modules['connectorx']
            read_sql_ori = cx_module.read_sql
            cx_module.read_sql = 'abc'
            res = db.query(dd_table_name)
            self.assertEqual(res['ddid'][0], str(ddid))
            cx_module.read_sql = read_sql_ori

    def test_add_column(self):
        db = DBHandler(db_url)
        service_id = '54'
        dd_table_name = 'dd_' + service_id
        db.init_table(dd_table_name, FIELDS_DD)
        self.assertFalse('abc' in db.get_fields(dd_table_name))
        db.add_column(dd_table_name, 'abc', 'REAL')
        self.assertTrue('abc' in db.get_fields(dd_table_name))


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
