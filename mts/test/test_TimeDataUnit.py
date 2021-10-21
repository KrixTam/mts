import unittest
import os
from mts.core import TimeDataUnit, DBHandler
from mts.utils import logger
db_url = 'sqlite://' + os.path.join(os.getcwd(), 'resources', 'ds', 'mtsdb')
DBHandler.register(db_url)


class TestTimeDataUnit(unittest.TestCase):
    def test_default(self):
        logger.log(DBHandler.get_tables())
        tdu = TimeDataUnit('51', 'a405ac45493b2000')
        res = tdu.query()
        logger.log(res)


if __name__ == '__main__':
    unittest.main()
