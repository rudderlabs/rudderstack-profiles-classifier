import unittest
from utils import *

class TestUtils(unittest.TestCase):
    def test_get_label_date_ref_case1(self):
        feature_date = '2022-01-01'
        self.assertEqual(get_label_date_ref(feature_date, 1), '2022-01-02')
        self.assertEqual(get_label_date_ref(feature_date, 0), '2022-01-01')
        self.assertEqual(get_label_date_ref(feature_date, -1), '2021-12-31')

if __name__ == '__main__':
    unittest.main()
