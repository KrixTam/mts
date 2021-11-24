import os
import unittest

from mts.core import DBHandler, SpaceDataUnit, DataDictionary
from mts.utils import logger

cwd = os.path.abspath(os.path.dirname(__file__))


class TestSpaceDataUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        output_dir = os.path.join(cwd, 'output')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'sdu')
        db_url = 'sqlite://' + db_file_name
        if os.path.exists(db_file_name):
            os.remove(db_file_name)
        DBHandler.register(db_url)
        dd = DataDictionary('51')
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        dd.sync_db(dd_file_name, True)

    def test_01(self):
        # 创建tdu
        service_id = '51'
        sdu = SpaceDataUnit(service_id)
        logger.log(sdu.tags)
        logger.log(sdu._tag_definition)
        logger.log(sdu._data)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
