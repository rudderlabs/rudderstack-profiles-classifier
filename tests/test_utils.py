import unittest
from utils import *

class TestUtils(unittest.TestCase):

    def test_combine_config(self):
        self.assertEqual(combine_config({"a":2, "b":3}, {"a":1}), {"a":1, "b":3})
        self.assertEqual(combine_config({"a":{"c":2},}, {"a":{"b":1}}), {"a":{"b":1,"c":2}})
        self.assertEqual(combine_config({"b":3}, {"a":1}), {"a":1, "b":3})
        self.assertEqual(combine_config({"a":2, "b":3}, None), {"a":2, "b":3})
        self.assertEqual(combine_config({"a":2, "b":3}, {}), {"a":2, "b":3})

        self.assertEqual(combine_config({"a":5, "b":{"x": {"p":1, "q":6}}, "c": {"m":8}, "d":9}, {"a":1, "b":{"x": None, "y":2}, "c": {}}), {"a":1, "b":{"x": {"p":1, "q":6}, "y":2}, "c": {"m":8}, "d":9})
        
    # Should return a string with the expected format when given valid inputs
    def test_valid_inputs(self):
        material_table_prefix = "material_"
        model_name = "shopify_user_features"
        model_hash = "fa138b1a"
        seq_no = "785"
        expected_result = "material_shopify_user_features_fa138b1a_785"

        result = generate_material_name(material_table_prefix, model_name, model_hash, seq_no)
        self.assertEqual(result, expected_result)

