import os
import unittest
from mts.core.handler import DBHandler
from mts.commons.const import *
from mts.core.datamodel import Tags, DataDictionary

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]
output_dir = os.path.join(os.getcwd(), 'output')


class TestTags(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # pragma: no cover
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        db_file_name = os.path.join(output_dir, 'tags')
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
        t = Tags(service_id)
        self.assertEqual(t.desc('a4059507fd31c005'), '货源')
        self.assertEqual(t.desc('a4059507fd30c005', 1), '红')
        self.assertEqual(t.value, ['a4059507fd30c005', 'a4059507fd31c005'])
        self.assertEqual(BLANK, t.desc('123'))

    def test_err(self):
        with self.assertRaises(ValueError):
            Tags(12)

    def test_exists(self):
        service_id = '51'
        t = Tags(service_id)
        self.assertFalse(t.exists('红'))
        self.assertFalse(t.exists('黑'))
        self.assertFalse(t.exists('红', 2))
        self.assertFalse(t.exists('红', 64))

    def test_enum_value(self):
        service_id = '51'
        t = Tags(service_id)
        self.assertEqual(4, t.enum_value('绿'))
        self.assertEqual(None, t.enum_value('黑'))


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
