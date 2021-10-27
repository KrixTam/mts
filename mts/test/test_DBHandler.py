import os
import unittest
from mts.core import DBHandler, DataDictionaryId
from mts.const import *

output_dir = os.path.join(os.getcwd(), 'output')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
db_file_name = os.path.join(os.getcwd(), 'output', 'dbhandler')
db_url = 'sqlite://' + db_file_name
if os.path.exists(db_file_name):
    os.remove(db_file_name)
DBHandler.register(db_url)


class TestDBHandler(unittest.TestCase):
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
        dd_file_name = os.path.join(os.getcwd(), 'resources', 'ds', '51.dd')
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

    def test_add(self):
        # DBHandler.set_mode(DB_MODE_SD)
        service_id = '53'
        sc = int(service_id, 8)
        ddid = DataDictionaryId(dd_type=DD_TYPE_METRIC, service_code=sc)
        data = {'ddid': str(ddid), 'desc': '测试项', 'oid_mask': ''}
        dd_table_name = DBHandler.get_table_name(service_id, TABLE_TYPE_DD)
        DBHandler.init_table(dd_table_name, FIELDS_DD)
        DBHandler.add(data, dd_table_name)
        res = DBHandler.query(service_id, TABLE_TYPE_DD)
        self.assertEqual(res['ddid'][0], str(ddid))


if __name__ == '__main__':
    unittest.main()
