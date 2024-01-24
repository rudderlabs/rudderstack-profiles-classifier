import sys
import unittest

sys.path.append("../..")

from utils import replace_seq_no_in_query


class TestReplaceSeqNoInQuery(unittest.TestCase):
    def test_replaces_seq_no_correctly(self):
        query = "SELECT * FROM material_user_var_table_123"
        seq_no = "567"
        expected_result = "SELECT * FROM material_user_var_table_567"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_empty_query(self):
        query = ""
        seq_no = "5"
        expected_result = "_5"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_missing_seq_no(self):
        query = "SELECT * FROM material_user_var_table_"
        seq_no = "33"
        expected_result = "SELECT * FROM material_user_var_table_33"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_non_string_input(self):
        with self.assertRaises(AttributeError):
            replace_seq_no_in_query(123, "456")
