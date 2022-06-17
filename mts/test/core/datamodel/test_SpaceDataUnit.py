import os
import unittest
from mts.core.datamodel import SpaceDataUnit, DataDictionary
from mts.core.handler import DataFileHandler, DBHandler
from mts.commons.const import *

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]
output_dir = os.path.join(os.getcwd(), 'output')


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # pragma: no cover
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'tdu')
        db_url = 'sqlite://' + db_file_name
        if os.path.exists(db_file_name):
            os.remove(db_file_name)
        DBHandler(db_url)
        service_id = '51'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)

    def test_all(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        sdu.init_db()
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 32}]}})
        self.assertEqual(None, res)
        self.assertEqual('颜色', sdu.map_desc('a4059507fd30c005'))
        # sync_db测试
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 32}]}})
        self.assertEqual(1, len(res))
        self.assertEqual('山竹', dd.map_desc(res[0], DD_TYPE_OWNER))

    def test_init_data(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        sdu.init_db()
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'ne': 32}]}})
        self.assertEqual(None, res)
        sdu.init_data()
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'ne': 32}]}})
        self.assertEqual(7, len(res))

    def test_query_01(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        with self.assertRaises(ValueError):
            sdu.query()

    def test_query_02(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}, {'eq': 2}]}})
        self.assertEqual(1, len(res))
        self.assertEqual('香蕉', dd.map_desc(res[0], DD_TYPE_OWNER))
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}, {'eq': 4}]}})
        self.assertEqual(2, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[1], DD_TYPE_OWNER))

    def test_query_03(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}, {'ne': 2}]}})
        self.assertEqual(1, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))

    def test_query_04(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'or', 'data': {'a4059507fd30c005': [{'eq': 1}, {'ne': 4}]}})
        self.assertEqual(3, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('山竹', dd.map_desc(res[1], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[2], DD_TYPE_OWNER))

    def test_query_05(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'or', 'data': {'a4059507fd30c005': [{'eq': 1}, {'eq': 2}]}})
        self.assertEqual(3, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('梨', dd.map_desc(res[1], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[2], DD_TYPE_OWNER))

    def test_query_06(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(owner={'op': 'and', 'data': [{'eq': 'a4059507fd2fc001'}]},
                        tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}, {'ne': 2}]}})
        self.assertEqual(None, res)
        res = sdu.query(owner={'op': 'or', 'data': [{'eq': 'a4059507fd2fc001'}]},
                        tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}, {'ne': 2}]}})
        self.assertEqual(None, res)
        res = sdu.query(owner={'op': 'or', 'data': [{'eq': 'a4059507fd2fc000'}]},
                        tag={'op': 'or', 'data': {'a4059507fd30c005': [{'eq': 1}, {'eq': 2}]}})
        self.assertEqual(1, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        res = sdu.query(owner={'op': 'or', 'data': [{'eq': 'a4059507fd2fc000'}, {'eq': 'a4059507fd2fc002'}]},
                        tag={'op': 'or', 'data': {'a4059507fd30c005': [{'eq': 4}, {'ne': 2}]}})
        self.assertEqual(2, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('西瓜', dd.map_desc(res[1], DD_TYPE_OWNER))
        res = sdu.query(owner={'op': 'and', 'data': [{'ne': 'a4059507fd2fc000'}, {'ne': 'a4059507fd2fc002'}]},
                        tag={'op': 'or', 'data': {'a4059507fd30c005': [{'eq': 4}, {'ne': 2}]}})
        self.assertEqual(5, len(res))
        self.assertEqual('梨', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('橘子', dd.map_desc(res[1], DD_TYPE_OWNER))
        self.assertEqual('橙子', dd.map_desc(res[2], DD_TYPE_OWNER))
        self.assertEqual('山竹', dd.map_desc(res[3], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[4], DD_TYPE_OWNER))

    def test_query_07(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(owner={'op': 'and', 'data_desc': [{'eq': '梨'}]},
                        tag={'op': 'and', 'data_desc': {'颜色': [{'eq': '红'}, {'ne': '黄'}]}})
        self.assertEqual(None, res)
        res = sdu.query(owner={'op': 'or', 'data_desc': [{'eq': '梨'}]},
                        tag={'op': 'and', 'data_desc': {'颜色': [{'eq': '红'}, {'ne': '黄'}]}})
        self.assertEqual(None, res)
        res = sdu.query(owner={'op': 'or', 'data_desc': [{'eq': '苹果'}]},
                        tag={'op': 'or', 'data_desc': {'颜色': [{'eq': '红'}, {'eq': '黄'}]}})
        self.assertEqual(1, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        res = sdu.query(owner={'op': 'or', 'data_desc': [{'eq': '苹果'}, {'eq': '西瓜'}]},
                        tag={'op': 'or', 'data_desc': {'颜色': [{'eq': '绿'}, {'ne': '黄'}]}})
        self.assertEqual(2, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('西瓜', dd.map_desc(res[1], DD_TYPE_OWNER))
        res = sdu.query(owner={'op': 'and', 'data_desc': [{'ne': '苹果'}, {'ne': '西瓜'}]},
                        tag={'op': 'or', 'data_desc': {'颜色': [{'eq': '绿'}, {'ne': '黄'}]}})
        self.assertEqual(5, len(res))
        self.assertEqual('梨', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('橘子', dd.map_desc(res[1], DD_TYPE_OWNER))
        self.assertEqual('橙子', dd.map_desc(res[2], DD_TYPE_OWNER))
        self.assertEqual('山竹', dd.map_desc(res[3], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[4], DD_TYPE_OWNER))

    def test_query_08(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(oid_only=False, tag={'op': 'or', 'data': {'a4059507fd30c005': [{'eq': 1}, {'ne': 4}]}})
        self.assertEqual((3, 3), res.shape)
        self.assertEqual('苹果', dd.map_desc(res[FIELD_OWNER][0], DD_TYPE_OWNER))
        self.assertEqual('山竹', dd.map_desc(res[FIELD_OWNER][1], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[FIELD_OWNER][2], DD_TYPE_OWNER))

    def test_query_09(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c015': [{'eq': 1}]}})
        self.assertEqual(None, res)

    def test_query_10(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(owner={'op': 'and', 'data': [{'eq': 'a4059507fd2fc011'}]})
        self.assertEqual(None, res)

    def test_query_11(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(oid_only=False, owner={'op': 'and', 'data': [{'eq': 'a4059507fd2fc002'}]})
        self.assertEqual((1, 3), res.shape)
        self.assertEqual(4, res['a4059507fd30c005'][0])
        self.assertEqual(1, res['a4059507fd31c005'][0])

    def test_query_12(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(owner={'op': 'and', 'data': [{'eq': 'a4059507fd2fc002'}]})
        self.assertEqual(1, len(res))
        self.assertEqual('a4059507fd2fc002', res[0])

    def test_add_01(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}]}})
        self.assertEqual(2, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[1], DD_TYPE_OWNER))
        sdu.add(owner='a4059507fd2fc002', data={'a4059507fd30c005': 1})
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}]}})
        self.assertEqual(3, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('西瓜', dd.map_desc(res[1], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[2], DD_TYPE_OWNER))

    def test_add_02(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        dd = DataDictionary(service_id)
        filename = os.path.join(cwd, 'resources', 'ds', '51.sdu')
        sdu.sync_db(filename, True)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}]}})
        self.assertEqual(2, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[1], DD_TYPE_OWNER))
        sdu.add(owner='a4059507fd2fc002', data_desc={'颜色': '红'})
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}]}})
        self.assertEqual(3, len(res))
        self.assertEqual('苹果', dd.map_desc(res[0], DD_TYPE_OWNER))
        self.assertEqual('西瓜', dd.map_desc(res[1], DD_TYPE_OWNER))
        self.assertEqual('香蕉', dd.map_desc(res[2], DD_TYPE_OWNER))

    def test_add_03(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        with self.assertRaises(ValueError):
            sdu.add(owner='a4059507fd2fc011', data_desc={'颜色': '红'})

    def test_add_04(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        with self.assertRaises(ValueError):
            sdu.add(data_desc={'颜色': '红'})

    def test_add_05(self):
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        sdu._db.remove(sdu._table_name)
        dd = DataDictionary(service_id)
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}]}})
        self.assertEqual(None, res)
        sdu.add(owner='a4059507fd2fc002', data_desc={'颜色': '红'})
        res = sdu.query(tag={'op': 'and', 'data': {'a4059507fd30c005': [{'eq': 1}]}})
        self.assertEqual(1, len(res))
        self.assertEqual('西瓜', dd.map_desc(res[0], DD_TYPE_OWNER))


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
