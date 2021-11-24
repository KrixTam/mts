import os
import unittest
from mts.const import *
from mts.core import DataDictionary, DBHandler

cwd = os.path.abspath(os.path.dirname(__file__))


class TestDataDictionary(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        output_dir = os.path.join(cwd, 'output')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'dd')
        db_url = 'sqlite://' + db_file_name
        if os.path.exists(db_file_name):
            os.remove(db_file_name)
        DBHandler.register(db_url)

    def test_default(self):
        service_id = '51'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)
        self.assertEqual(dd.get_oid(DD_TYPE_METRIC), ['a4059507fd30c003', 'a4059507fd30c004'])
        self.assertEqual(dd.get_oid(DD_TYPE_METRIC), DataDictionary.query_oid(service_id, DD_TYPE_METRIC))
        self.assertEqual(dd.get_oid(DD_TYPE_METRIC), dd.query(True, desc=['进货量/斤', '销量/斤']))

    def test_error_query_oid(self):
        with self.assertRaises(ValueError):
            DataDictionary.query_oid('52', 'bad')

    def test_add_01(self):
        service_id = '51'
        dd = DataDictionary(service_id)
        ddid = dd.add(dd_type=DD_TYPE_OWNER, desc='苹果', oid_mask='')
        self.assertEqual(1, len(dd.query(True, desc=['苹果'])))
        self.assertEqual(ddid[1:], dd.map_oid(desc='苹果'))
        self.assertEqual('苹果', dd.map_desc(ddid[1:]))

    def test_add_02(self):
        service_id = '56'
        dd = DataDictionary(service_id)
        ddid = dd.add(dd_type=DD_TYPE_OWNER, desc='苹果', oid_mask='')
        self.assertEqual(1, len(dd.query(True, desc=['苹果'])))
        self.assertEqual(ddid[1:], dd.map_oid(desc='苹果'))
        self.assertEqual('苹果', dd.map_desc(ddid[1:]))

    def test_append(self):
        service_id = '55'
        dd = DataDictionary(service_id)
        self.assertTrue(dd.query().empty)
        DataDictionary.append(service_id=service_id, dd_type=DD_TYPE_OWNER, desc='苹果', oid_mask='')
        dd.reload()
        self.assertFalse(dd.query().empty)


if __name__ == '__main__':
    unittest.main()
