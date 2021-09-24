import unittest
import os
from mts.core import DataUnitService, DBConnector
from mts.const import *


class TestDataUnitService(unittest.TestCase):
    def test_constructor_01(self):
        settings = {
            'service_id': '51',
            'ds_path': os.path.join('output'),
            'owners': ['苹果', '梨', '西瓜', '橘子', '橙子', '山竹', '香蕉'],
            'metrics': ['进货量/斤', '销量/斤'],
            'tags': [
                {
                    'name': '颜色',
                    'values': [
                        {'disc': '红', 'owners': ['苹果', '西瓜']},
                        {'disc': '黄', 'owners': ['梨', '香蕉']},
                        {'disc': '绿', 'owners': ['梨', '苹果', '橘子', '橙子', '西瓜', '香蕉']},
                        {'disc': '橙', 'owners': ['橘子', '橙子']},
                        {'disc': '白', 'owners': []},
                        {'disc': '紫', 'owners': ['山竹']}
                    ]
                },
                {
                    'name': '货源',
                    'values': [
                        {'disc': '国产', 'owners': ['苹果', '梨', '西瓜', '橘子', '橙子']},
                        {'disc': '进口', 'owners': ['山竹', '香蕉']}
                    ]
                },
            ]
        }
        db_url = 'sqlite://' + os.path.join(os.getcwd(), 'output', 'mtsdb')
        print(db_url)
        DBConnector.register(db_url)
        ds = DataUnitService(settings, True)
        tags = []
        owners = settings['owners']
        owners.sort()
        self.assertEqual(ds.disc(dd_type=DD_TYPE_OWNER), owners)
        metrics = settings['metrics']
        metrics.sort()
        self.assertEqual(ds.disc(dd_type=DD_TYPE_METRIC), metrics)
        for tag in settings['tags']:
            tags.append(tag['name'])
        tags.sort()
        self.assertEqual(ds.disc(dd_type=DD_TYPE_TAG), tags)
        sdu_table_name = DBConnector.get_table_name(ds.service_id, TABLE_TYPE_SDU)
        owner_ids = DBConnector.query(sdu_table_name, ['owner'])['owner'].tolist()
        owner_ids.sort()
        self.assertEqual(ds.owners, owner_ids)

    def test_constructor_02(self):
        settings = {
            'service_id': '51',
            'ds_path': os.path.join('resources', 'ds')
        }
        db_url = 'sqlite://' + os.path.join(os.getcwd(), 'output', 'mtsdb')
        DBConnector.register(db_url)
        ds = DataUnitService(settings)
        owners = ['苹果', '梨', '西瓜', '橘子', '橙子', '山竹', '香蕉']
        owners.sort()
        self.assertEqual(ds.disc(dd_type=DD_TYPE_OWNER), owners)
        metrics = ['进货量/斤', '销量/斤']
        metrics.sort()
        self.assertEqual(ds.disc(dd_type=DD_TYPE_METRIC), metrics)
        tags = ['颜色', '货源']
        tags.sort()
        self.assertEqual(ds.disc(dd_type=DD_TYPE_TAG), tags)
        sdu_table_name = DBConnector.get_table_name(ds.service_id, TABLE_TYPE_SDU)
        owner_ids = DBConnector.query(sdu_table_name, ['owner'])['owner'].tolist()
        owner_ids.sort()
        self.assertEqual(ds.owners, owner_ids)


if __name__ == '__main__':
    unittest.main()
