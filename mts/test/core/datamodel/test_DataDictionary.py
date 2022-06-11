import os
import unittest
from mts.commons.const import *
from mts.core.handler import DBHandler, DataFileHandler
from mts.core.datamodel import DataDictionary

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]
output_dir = os.path.join(os.getcwd(), 'output')


class TestDataDictionary(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # pragma: no cover
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'dd')
        db_url = 'sqlite://' + db_file_name
        if os.path.exists(db_file_name):
            os.remove(db_file_name)
        DBHandler(db_url)

    def test_default(self):
        service_id = '51'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)
        self.assertEqual(dd.query(True, dd_type=DD_TYPE_METRIC), ['a4059507fd30c003', 'a4059507fd30c004'])
        self.assertEqual(dd.query(True, dd_type=DD_TYPE_METRIC), dd.query(True, desc=['进货量/斤', '销量/斤']))
        self.assertEqual(dd.query(ddid=['1a4059507fd2fc000'])[KEY_DESC].tolist(), ['苹果'])
        self.assertEqual(dd.query(oid='a4059507fd30c001')[KEY_DESC].tolist(), ['山竹'])
        dd.export_data(output_dir)
        file_01 = os.path.join(cwd, 'resources', 'ds', '51.dd')
        file_02 = os.path.join(output_dir, 'dd_51.csv')
        self.assertEqual(DataFileHandler.checksum(file_01), DataFileHandler.checksum(file_02))
        dd_02 = DataDictionary(service_id)
        self.assertEqual(dd.query(True, dd_type=DD_TYPE_METRIC), dd_02.query(True, dd_type=DD_TYPE_METRIC))
        self.assertEqual(dd.map_oid('苹果', DD_TYPE_METRIC), None)
        self.assertEqual(dd.map_oid('苹果'), 'a4059507fd2fc000')
        self.assertEqual(dd.map_desc('a4059507fd2fc000', DD_TYPE_METRIC), None)
        self.assertEqual(dd.map_desc('a4059507fd2fc000'), '苹果')

    def test_error_init(self):
        with self.assertRaises(ValueError):
            DataDictionary(123)

    def test_error_query_01(self):
        service_id = '52'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.query(dd_type='bad')

    def test_error_query_02(self):
        service_id = '52'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.query(dd_type=123)

    def test_error_query_03(self):
        service_id = '52'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.query(desc=123)

    def test_error_query_04(self):
        service_id = '52'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.query(ddid=123)

    def test_error_query_05(self):
        service_id = '52'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.query(oid=123)

    def test_add_and_remove_01(self):
        service_id = '52'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)
        ddid = dd.add(dd_type=DD_TYPE_OWNER, desc='苹果')
        self.assertEqual(1, len(dd.query(True, desc=['苹果'])))
        self.assertEqual(ddid[1:], dd.map_oid(desc='苹果'))
        self.assertEqual('苹果', dd.map_desc(ddid[1:]))
        dd.remove(ddid)
        self.assertEqual(0, len(dd.query(True, ddid=[ddid])))

    def test_add_and_remove_02(self):
        service_id = '56'
        dd = DataDictionary(service_id)
        ddid = dd.add(dd_type=DD_TYPE_OWNER, desc='苹果')
        self.assertEqual(1, len(dd.query(True, desc=['苹果'])))
        self.assertEqual(ddid[1:], dd.map_oid(desc='苹果'))
        self.assertEqual('苹果', dd.map_desc(ddid[1:]))
        with self.assertRaises(ValueError):
            dd.remove(ddid=123)

    def test_add_03(self):
        service_id = '56'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.add()

    def test_add_04(self):
        service_id = '56'
        dd = DataDictionary(service_id)
        with self.assertRaises(ValueError):
            dd.add(dd_type=DD_TYPE_OWNER)

    def test_add_05(self):
        service_id = '56'
        dd = DataDictionary(service_id)
        ddid = dd.add(dd_type=DD_TYPE_TAG_VALUE, desc='黑', oid='a4059507fd30c005', mask='0000000000000007')
        self.assertEqual(1, len(dd.query(True, desc=['黑'])))
        self.assertEqual(ddid[1:], dd.map_oid(desc='黑'))
        self.assertEqual('黑', dd.map_desc(ddid[1:]))

    def test_field(self):
        service_id = '57'
        dd = DataDictionary(service_id)
        self.assertEqual(dd.fields(), FIELDS_DD)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
