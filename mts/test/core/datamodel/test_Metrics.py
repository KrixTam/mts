import os
import unittest
from mts.core.handler import DBHandler
from mts.commons.const import *
from mts.core.datamodel import Metrics, DataDictionary

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]
output_dir = os.path.join(os.getcwd(), 'output')


class TestMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # pragma: no cover
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'metrics')
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
        m = Metrics(service_id)
        self.assertEqual(m.desc('a4059507fd30c003'), '进货量/斤')
        self.assertEqual(m.value, ['a4059507fd30c003', 'a4059507fd30c004'])
        self.assertEqual(BLANK, m.desc('123'))

    def test_err(self):
        with self.assertRaises(ValueError):
            Metrics(12)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
