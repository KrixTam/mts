import unittest
import os
from mts.core import DataUnitService, DBConnector
from mts.const import *


class TestDataUnitService(unittest.TestCase):
    def test_constructor(self):
        settings = {
            'service_id': '01',
            'ds_path': os.path.join('resources', 'ds'),
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
        self.assertEqual(ds.disc(dd_type=DD_TYPE_METRIC), settings['metrics'].sort())


if __name__ == '__main__':
    unittest.main()
