import unittest
from utils import *

class TestUtils(unittest.TestCase):
    def test_get_label_date_ref_case1(self):
        feature_date = '2022-01-01'
        self.assertEqual(get_label_date_ref(feature_date, 1), '2022-01-02')
        self.assertEqual(get_label_date_ref(feature_date, 0), '2022-01-01')
        self.assertEqual(get_label_date_ref(feature_date, -1), '2021-12-31')

    def test_combine_config(self):
        self.assertEqual(combine_config({"a":2, "b":3}, {"a":1}), {"a":1, "b":3})
        self.assertEqual(combine_config({"a":{"c":2},}, {"a":{"b":1}}), {"a":{"b":1,"c":2}})
        self.assertEqual(combine_config({"b":3}, {"a":1}), {"a":1, "b":3})
        self.assertEqual(combine_config({"a":2, "b":3}, None), {"a":2, "b":3})
        self.assertEqual(combine_config({"a":2, "b":3}, {}), {"a":2, "b":3})

        self.assertEqual(combine_config({"a":5, "b":{"x": {"p":1, "q":6}}, "c": {"m":8}, "d":9}, {"a":1, "b":{"x": None, "y":2}, "c": {}}), {"a":1, "b":{"x": {"p":1, "q":6}, "y":2}, "c": {"m":8}, "d":9})

if __name__ == '__main__':
    unittest.main()
