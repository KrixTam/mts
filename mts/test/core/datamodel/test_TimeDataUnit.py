import os
import unittest
from mts.commons.const import *
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
        service_id = '51'
        dd = DataDictionary(service_id)
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)

    def test_all_01(self):
        # 创建tdu
        service_id = '51'
        tdu = TimeDataUnit('1a4059507fd2fc000')
        # logger.log(tdu._metric)
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

    # def test_02(self):
    #     # 创建tdu
    #     service_id = '51'
    #     tdu = TimeDataUnit(service_id, 'a405ac45493b2000')
    #     # 常规无条件限制下的query测试
    #     df_01 = tdu.query()
    #     if df_01.empty:
    #         filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
    #         tdu.sync_db(filename)
    #         df_01 = tdu.query()
    #     a = TimeDataUnit.to_date('1617120000.000')
    #     self.assertEqual(df_01.loc[a]['a4059507fd30c003':][0], 80)
    #     # 只查询个别metric
    #     df_02 = tdu.query(metric=['a4059507fd30c003'])
    #     self.assertEqual(4, df_02.shape[0])
    #     self.assertEqual(1, len(df_02.columns))
    #     self.assertEqual(df_01['a4059507fd30c003'].loc[a], df_02['a4059507fd30c003'].loc[a])
    #     # query中带有any的测试
    #     df_03 = tdu.query(any=['2021-03-31'])
    #     self.assertEqual(1, len(df_03.index))
    #     self.assertEqual(df_03['a4059507fd30c003'].loc[a], df_02['a4059507fd30c003'].loc[a])
    #     # query中带有interval的测试
    #     df_04 = tdu.query(interval={'from': '2021-02-15', 'to': '2021-04-15'})
    #     self.assertEqual(2, len(df_04.index))
    #     self.assertEqual(df_03['a4059507fd30c003'].loc[a], df_04['a4059507fd30c003'].loc[a])
    #
    # def test_03(self):
    #     # 创建tdu
    #     service_id = '51'
    #     tdu = TimeDataUnit(service_id, 'a405ac45493b2000')
    #     dd = DataDictionary(service_id)
    #     # add一个记录
    #     tdu.add(ts='2021-3-17', data={'进货量/斤': 123, '转售额/斤': 40})
    #     dd.reload()
    #     df_05 = tdu.query(interval={'from': '2021-02-15', 'to': '2021-04-15'})
    #     if 1 == df_05.shape[0]:
    #         filename = os.path.join(cwd, 'resources', 'ds', '51_a4059507fd2fc000.tdu')
    #         tdu.sync_db(filename)
    #         df_05 = tdu.query(interval={'from': '2021-02-15', 'to': '2021-04-15'})
    #     self.assertEqual(3, len(df_05.index))
    #     df_06 = tdu.query(any=['2021-03-17'])
    #     # logger.log(df_06)
    #     self.assertEqual(1, len(df_06.index))
    #     self.assertEqual(123, df_06['a4059507fd30c003'][0])
    #     new_metric = dd.query(True, desc=['转售额/斤'])[0]
    #     self.assertEqual(40, df_06[new_metric][0])
    #
    # def test_04(self):
    #     with self.assertRaises(ValueError):
    #         TimeDataUnit('service_id', 'a405ac45493b2000')
    #
    # def test_05(self):
    #     with self.assertRaises(ValueError):
    #         TimeDataUnit('51', 'a405ac45493b200')
    #
    # def test_06(self):
    #     with self.assertRaises(ValueError):
    #         tdu = TimeDataUnit('51', 'a405ac45493b2000')
    #         tdu.add()


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
