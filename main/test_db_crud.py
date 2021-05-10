import unittest
import sqlite3
import json
import os
from db_crud import CRUD


class CrudTest(unittest.TestCase):
    
    def setUp(self):
        self.db = sqlite3.connect(f"{os.getcwd()}/db/Grants.db")
        self.cursor = self.db.cursor()

    def test_read_file(self):
        records = CRUD.read_file(self,file_path='/input/neh-grants-2010-2019-csv-1.csv')
        self.assertEqual(len(records),7615)

    def tearDown(self):
        self.db.close()


if __name__ == '__main__':
    unittest.main()