import os
import unittest
from mts.const import *
from mts.core import DataDictionary, DBHandler
from mts.utils import logger

output_dir = os.path.join(os.getcwd(), 'output')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
db_file_name = os.path.join(os.getcwd(), 'output', 'dd')
db_url = 'sqlite://' + db_file_name
if os.path.exists(db_file_name):
    os.remove(db_file_name)
DBHandler.register(db_url)


class TestDataDictionary(unittest.TestCase):
    def test_default(self):
        service_id = '51'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(os.getcwd(), 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)
        self.assertEqual(dd.get_oid(DD_TYPE_METRIC), ['a4059507fd30c003', 'a4059507fd30c004'])
        self.assertEqual(dd.get_oid(DD_TYPE_METRIC), DataDictionary.query_oid(service_id, DD_TYPE_METRIC))
        self.assertEqual(dd.get_oid(DD_TYPE_METRIC), dd.query(True, desc=['进货量/斤', '销量/斤']))

    def test_error_query_oid(self):
        with self.assertRaises(ValueError):
            DataDictionary.query_oid('52', 'bad')


if __name__ == '__main__':
    unittest.main()
