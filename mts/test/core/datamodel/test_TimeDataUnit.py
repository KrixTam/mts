import os
import unittest
import numpy as np
from mts.core.datamodel import TimeDataUnit, DataDictionary
from mts.core.handler import DataFileHandler, DBHandler

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]
output_dir = os.path.join(os.getcwd(), 'output')


class TestTimeDataUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # pragma: no cover
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'tdu')
        db_url = 'sqlite://' + db_file_name
        if os.path.exists(db_file_name):
            os.remove(db_file_name)
        DBHandler(db_url)

    def setUp(self):
        service_id = '51'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)

    def test_basic(self):
        # 创建tdu
        tdu = TimeDataUnit('1a4059507fd2fc000')
        tdu.init_db()
        df = tdu.query()
        self.assertTrue(df.empty)
        # sync_db测试
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        # 重复sync测试
        tdu.sync_db(filename)
        tdu.export_data(output_dir)
        file_01 = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        file_02 = os.path.join(output_dir, 'tdu_51_a4059507fd2fc000.csv')
        self.assertEqual(DataFileHandler.checksum(file_01), DataFileHandler.checksum(file_02))
        res = tdu.query(metric=['a405ac45493b2000'])
        self.assertFalse(res.empty)
        self.assertEqual(res[res['a4059507fd30c003'] == 150]['a4059507fd30c004'].tolist(), [180])
        self.assertEqual(tdu.map_desc('a4059507fd30c004'), '销量/斤')

    def test_error_init(self):
        with self.assertRaises(ValueError):
            TimeDataUnit('2a4059507fd30c003')

    def test_query_01(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        res = tdu.query(metric=['a4059507fd30c004'])
        self.assertEqual(res.shape, (4, 1))
        na_flags = res['a4059507fd30c004'].isna().values.tolist()
        self.assertTrue(na_flags[2])
        self.assertEqual(180, res['a4059507fd30c004'][1])

    def test_query_02(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        res_01 = tdu.query(metric=['a4059507fd30c004'])
        res_02 = tdu.query(desc=['销量/斤'])
        self.assertTrue(res_01.equals(res_02))

    def test_query_03(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        res_01 = tdu.query(interval={'from': '2021-01-01', 'to': '2021-01-31'})
        self.assertEqual(res_01.shape, (1, 2))
        self.assertEqual(100, res_01['a4059507fd30c004'][0])
        res_02 = tdu.query(any=['2021-01-01', '2021-01-31'])
        self.assertTrue(res_01.equals(res_02))

    def test_add_01(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        with self.assertRaises(ValueError):
            tdu.add()

    def test_add_02(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        df = tdu.query(interval={'from': '2022-1-1', 'to': '2023-1-1'})
        self.assertTrue(df.empty)
        tdu.add(ts='2022-6-12', data={'进货量/斤': 55, '销量/斤': 38})
        df = tdu.query(interval={'from': '2022-1-1', 'to': '2023-1-1'})
        self.assertEqual(df.shape, (1, 2))
        self.assertEqual(38, df['a4059507fd30c004'][0])

    def test_add_03(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        df = tdu.query(interval={'from': '2022-1-1', 'to': '2023-1-1'})
        self.assertTrue(df.empty)
        tdu.add(ts='2022-6-12', data={'进货量/斤': 55, '销量/斤': 38, '存量': 188})
        df = tdu.query(interval={'from': '2022-1-1', 'to': '2023-1-1'})
        self.assertEqual(df.shape, (1, 3))
        metric = tdu.metrics.value
        metric.remove('a4059507fd30c003')
        metric.remove('a4059507fd30c004')
        self.assertEqual(188, df[metric[0]][0])

    def test_reset_metrics(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        del TimeDataUnit._metrics['51']
        with self.assertRaises(KeyError):
            tdu.query(metric=['a4059507fd30c004'])
        tdu.reset_metrics()
        res = tdu.query(metric=['a4059507fd30c004'])
        self.assertEqual(res.shape, (4, 1))
        na_flags = res['a4059507fd30c004'].isna().values.tolist()
        self.assertTrue(na_flags[2])
        self.assertEqual(180, res['a4059507fd30c004'][1])

    def test_remove(self):
        tdu = TimeDataUnit('1a4059507fd2fc000')
        filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename, True)
        res = tdu.query(metric=['a4059507fd30c004'])
        self.assertEqual(180, res['a4059507fd30c004'][1])
        tdu.remove('2021-02-28')
        res = tdu.query(metric=['a4059507fd30c004'])
        self.assertTrue(np.isnan(res['a4059507fd30c004'][1]))


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
