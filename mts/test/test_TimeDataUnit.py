import os
import unittest
from mts.const import *
from mts.core import TimeDataUnit, DBHandler
from mts.utils import logger

output_dir = os.path.join(os.getcwd(), 'output')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
db_file_name = os.path.join(os.getcwd(), 'output', 'tdu')
db_url = 'sqlite://' + db_file_name
if os.path.exists(db_file_name):
    os.remove(db_file_name)
DBHandler.register(db_url)


class TestTimeDataUnit(unittest.TestCase):
    def test_default(self):
        logger.log(DBHandler.get_tables())
        tdu = TimeDataUnit('51', 'a405ac45493b2000')
        logger.log(tdu._metric)
        df = tdu.query()
        self.assertTrue(df.empty)
        logger.log(df)
        filename = os.path.join(os.getcwd(), 'resources', 'ds', '51_a4059507fd2fc000.tdu')
        tdu.sync_db(filename)


if __name__ == '__main__':
    unittest.main()
