import os
import unittest
from mts.const import *
from mts.core import DataUnitService, DBHandler
from mts.utils import logger


class TestDataUnitService(unittest.TestCase):
    def test_constructor_01(self):
        settings = {
            'service_id': '51',
            'ds_path': os.path.join('resources', 'ds'),
            'bak_path': os.path.join('output'),
            'owners': ['苹果', '梨', '西瓜', '橘子', '橙子', '山竹', '香蕉'],
            'metrics': ['进货量/斤', '销量/斤'],
            'tags': [
                {
                    'name': '颜色',
                    'values': [
                        {'desc': '红', 'owners': ['苹果', '西瓜']},
                        {'desc': '黄', 'owners': ['梨', '香蕉']},
                        {'desc': '绿', 'owners': ['梨', '苹果', '橘子', '橙子', '西瓜', '香蕉']},
                        {'desc': '橙', 'owners': ['橘子', '橙子']},
                        {'desc': '白', 'owners': []},
                        {'desc': '紫', 'owners': ['山竹']}
                    ]
                },
                {
                    'name': '货源',
                    'values': [
                        {'desc': '国产', 'owners': ['苹果', '梨', '西瓜', '橘子', '橙子']},
                        {'desc': '进口', 'owners': ['山竹', '香蕉']}
                    ]
                },
            ]
        }
        db_url = 'sqlite://' + os.path.join(os.getcwd(), 'output', 'mtsdb')
        logger.log(db_url)
        DBHandler.register(db_url)
        ds = DataUnitService(settings, True)
        tags = []
        owners = settings['owners']
        owners.sort()
        self.assertEqual(ds.desc(dd_type=DD_TYPE_OWNER), owners)
        metrics = settings['metrics']
        metrics.sort()
        self.assertEqual(ds.desc(dd_type=DD_TYPE_METRIC), metrics)
        for tag in settings['tags']:
            tags.append(tag['name'])
        tags.sort()
        self.assertEqual(ds.desc(dd_type=DD_TYPE_TAG), tags)
        owner_ids = DBHandler.query(ds.service_id, TABLE_TYPE_SDU, ['owner'])['owner'].tolist()
        owner_ids.sort()
        self.assertEqual(ds.owners, owner_ids)

    def test_constructor_02(self):
        settings = {
            'service_id': '51',
            'ds_path': os.path.join('resources', 'ds')
        }
        db_url = 'sqlite://' + os.path.join(os.getcwd(), 'resources', 'ds', 'mtsdb')
        DBHandler.register(db_url)
        ds = DataUnitService(settings)
        owners = ['苹果', '梨', '西瓜', '橘子', '橙子', '山竹', '香蕉']
        owners.sort()
        self.assertEqual(ds.desc(dd_type=DD_TYPE_OWNER), owners)
        metrics = ['进货量/斤', '销量/斤']
        metrics.sort()
        self.assertEqual(ds.desc(dd_type=DD_TYPE_METRIC), metrics)
        tags = ['颜色', '货源']
        tags.sort()
        self.assertEqual(ds.desc(dd_type=DD_TYPE_TAG), tags)
        owner_ids = DBHandler.query(ds.service_id, TABLE_TYPE_SDU, ['owner'])['owner'].tolist()
        owner_ids.sort()
        self.assertEqual(ds.owners, owner_ids)


if __name__ == '__main__':
    unittest.main()
