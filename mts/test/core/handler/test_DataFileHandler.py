import unittest
import os
from mts.core.handler import DataFileHandler

cwd = os.path.abspath(os.path.dirname(__file__)).split('core')[0]


class TestDataFileHandler(unittest.TestCase):

    def test_checksum(self):
        dd_file_name = os.path.join(cwd, 'resources', 'ds', '51.dd')
        self.assertEqual(DataFileHandler.checksum(dd_file_name), 'c4b33d31c5cc4cab7bce6c503ad7b031')

    def test_load_json(self):
        file_name = os.path.join(cwd, 'resources', 'ds', 'test.json')
        data = DataFileHandler.load_json(file_name)
        self.assertEqual(len(data), 2)
        self.assertEqual(data['abc'], 123)

    def test_error_load_json(self):
        file_name = os.path.join(cwd, 'resources', 'ds', 'test123.json')
        self.assertEqual(None, DataFileHandler.load_json(file_name))


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
