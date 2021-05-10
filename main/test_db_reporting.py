import pytest
import unittest
import sqlite3
import json
import os
from db_reporting import Reporting


class ReportingTest(unittest.TestCase):
    
    def setUp(self):
        self.db = sqlite3.connect(f"{os.getcwd()}/db/Grants.db")
        self.cursor = self.db.cursor()

    def test_participants(self):
        _ , jdata = Reporting.participants(self, state=['NY'], participants="[Co Project Director]")
        self.assertEqual(jdata["InstState"],['NY'])  

    def test_supplements(self):
        supplements, _ = Reporting.supplements(self)
        self.assertEqual(len(supplements), 10)
        
    def test_projects(self):
        _ , jdata= Reporting.projects(self)
        self.assertEqual(len(list(jdata.keys())), 58)  

    def tearDown(self):
        self.db.close()


if __name__ == '__main__':
    unittest.main()