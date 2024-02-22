import sys
import unittest

sys.path.append("../..")

from src.utils.utils import replace_seq_no_in_query, split_material_table


class TestReplaceSeqNoInQuery(unittest.TestCase):
    def test_replaces_seq_no_correctly(self):
        query = "SELECT * FROM material_user_var_table_123"
        seq_no = 567
        expected_result = "SELECT * FROM material_user_var_table_567"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_empty_query(self):
        query = ""
        seq_no = 5
        expected_result = "_5"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_missing_seq_no(self):
        query = "SELECT * FROM material_user_var_table_"
        seq_no = 33
        expected_result = "SELECT * FROM material_user_var_table_33"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_non_string_input(self):
        with self.assertRaises(AttributeError):
            replace_seq_no_in_query(123, "456")


class TestSplitMaterialTable(unittest.TestCase):
    def test_valid_table_name(self):
        table_name = "Material_user_var_table_54ddc22a_383"
        expected_result = ("user_var_table", "54ddc22a", 383)
        actual_result = split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_missing_prefix(self):
        table_name = "user_var_table_54ddc22a_383"
        expected_result = (None, None, None)
        actual_result = split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_missing_seq_no(self):
        table_name = "Material_user_var_table_54ddc22a"
        expected_result = (None, None, None)
        actual_result = split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_invalid_seq_no(self):
        table_name = "Material_user_var_table_54ddc22a_foo"
        expected_result = (None, None, None)
        actual_result = split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_invalid_table_name(self):
        table_name = "user_var_table_54ddc22a_foo"
        expected_result = (None, None, None)
        actual_result = split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_material_query(self):
        table_name = "SELECT * FROM SCHEMA.Material_user_var_table_54ddc22a_383"
        expected_result = ("user_var_table", "54ddc22a", 383)
        actual_result = split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)
