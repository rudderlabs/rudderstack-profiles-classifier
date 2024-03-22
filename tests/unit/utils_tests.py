import unittest
from unittest.mock import patch
from src.predictions.rudderstack_predictions.wht.pythonWHT import PythonWHT

from src.predictions.rudderstack_predictions.utils.utils import (
    dates_proximity_check,
    datetime_to_date_string,
    generate_new_training_dates,
    replace_seq_no_in_query,
    get_abs_date_diff,
)


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
        expected_result = f"Couldn't find an integer seq_no in the input query: {query}"
        with self.assertRaises(Exception) as context:
            replace_seq_no_in_query(query, seq_no)
        self.assertIn(
            str(context.exception),
            expected_result,
        )

    def test_handles_missing_seq_no(self):
        query = "SELECT * FROM material_user_var_table_"
        seq_no = 33
        expected_result = f"Couldn't find an integer seq_no in the input query: {query}"
        with self.assertRaises(Exception) as context:
            replace_seq_no_in_query(query, seq_no)
        self.assertIn(
            str(context.exception),
            expected_result,
        )

    def test_replaces_seq_no_correctly_with_bigquery_input(self):
        query = "SELECT * FROM `schema`.`material_user_var_table_123`"
        seq_no = 567
        expected_result = "SELECT * FROM `schema`.`material_user_var_table_567`"
        actual_result = replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)


class TestSplitMaterialTable(unittest.TestCase):
    def test_valid_table_name(self):
        table_name = "Material_user_var_table_54ddc22a_383"
        expected_result = {
            "model_name": "user_var_table",
            "model_hash": "54ddc22a",
            "seq_no": 383,
        }
        actual_result = PythonWHT().split_material_name(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_missing_prefix(self):
        table_name = "user_var_table_54ddc22a_383"
        expected_result = {
            "model_name": "user_var_table",
            "model_hash": "54ddc22a",
            "seq_no": 383,
        }
        actual_result = PythonWHT().split_material_name(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_missing_seq_no(self):
        table_name = "Material_user_var_table_54ddc22a"
        with self.assertRaises(Exception):
            PythonWHT().split_material_name(table_name)

    def test_invalid_seq_no(self):
        table_name = "Material_user_var_table_54ddc22a_foo"
        with self.assertRaises(Exception):
            PythonWHT().split_material_name(table_name)

    def test_invalid_table_name(self):
        table_name = "user_var_table_54ddc22a_foo"
        with self.assertRaises(Exception):
            PythonWHT().split_material_name(table_name)

    def test_material_query(self):
        table_name = "SELECT * FROM SCHEMA.Material_user_var_table_54ddc22a_383"
        expected_result = {
            "model_name": "user_var_table",
            "model_hash": "54ddc22a",
            "seq_no": 383,
        }
        actual_result = PythonWHT().split_material_name(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_material_query_with_snowflake_input(self):
        table_name = "SELECT * FROM Material_user_var_table_54ddc22a_383"
        expected_result = {
            "model_name": "user_var_table",
            "model_hash": "54ddc22a",
            "seq_no": 383,
        }
        actual_result = PythonWHT().split_material_name(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_material_query_with_redshift_input(self):
        table_name = "SELECT * FROM material_user_var_table_54ddc22a_383"
        expected_result = {
            "model_name": "user_var_table",
            "model_hash": "54ddc22a",
            "seq_no": 383,
        }
        actual_result = PythonWHT().split_material_name(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_material_query_with_bigquery_input(self):
        table_name = "SELECT * FROM `SCHEMA`.`Material_user_var_table_54ddc22a_383`"
        expected_result = {
            "model_name": "user_var_table",
            "model_hash": "54ddc22a",
            "seq_no": 383,
        }
        actual_result = PythonWHT().split_material_name(table_name)
        self.assertEqual(actual_result, expected_result)


class TestDateDiff(unittest.TestCase):
    def test_same_date(self):
        """Test for same dates"""
        result = get_abs_date_diff("2023-11-21", "2023-11-21")
        self.assertEqual(result, 0)

    def test_different_dates(self):
        """Test for different dates"""
        result = get_abs_date_diff("2023-11-20", "2023-11-22")
        self.assertEqual(result, 2)

    def test_date_order(self):
        """Test for different date order"""
        result = get_abs_date_diff("2024-01-15", "2023-12-25")
        self.assertEqual(result, 21)

    def test_invalid_date_format(self):
        """Test for invalid date format"""
        with self.assertRaises(ValueError):
            get_abs_date_diff("2023/11/21", "2023-11-22")

    def test_invalid_date_values(self):
        """Test for invalid date values"""
        with self.assertRaises(ValueError):
            get_abs_date_diff("2023-11-31", "2023-11-22")


class TestDatesProximityCheck(unittest.TestCase):
    @patch(
        "src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff"
    )  # Mock the date_diff function
    def test_no_date_within_distance(self, mock_date_diff):
        """Test when no date is within the specified distance"""
        mock_date_diff.side_effect = [10, 15, 20]  # Mock return values for date_diff
        result = dates_proximity_check(
            "2023-11-21", ["2023-11-11", "2023-11-06", "2023-10-21"], 5
        )
        self.assertTrue(result)

    @patch("src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff")
    def test_date_within_distance(self, mock_date_diff):
        """Test when a date is within the specified distance"""
        mock_date_diff.side_effect = [4, 15, 20]
        result = dates_proximity_check(
            "2023-11-21", ["2023-11-17", "2023-11-06", "2023-10-21"], 5
        )
        self.assertFalse(result)

    @patch("src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff")
    def test_empty_date_list(self, mock_date_diff):
        """Test with an empty date list"""
        mock_date_diff.side_effect = []  # No calls to date_diff expected
        result = dates_proximity_check("2023-11-21", [], 5)
        self.assertTrue(result)


class TestGenerateNewTrainingDates(unittest.TestCase):
    @patch("src.predictions.rudderstack_predictions.utils.utils.date_add")
    @patch("src.predictions.rudderstack_predictions.utils.utils.dates_proximity_check")
    @patch("src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff")
    def test_no_training_dates(
        self, mock_get_abs_date_diff, mock_dates_proximity_check, mock_date_add
    ):
        """Test with no training dates"""
        max_feature_date = "2024-02-26"
        min_feature_date = "2024-02-7"
        prediction_horizon_days = 7
        feature_data_min_date_diff = 10

        mock_get_abs_date_diff.return_value = 19
        mock_date_add.side_effect = ["2024-02-16", "2024-02-23"]
        mock_dates_proximity_check.return_value = True

        result = generate_new_training_dates(
            max_feature_date,
            min_feature_date,
            [],
            prediction_horizon_days,
            feature_data_min_date_diff,
        )

        self.assertEqual(
            result,
            ("2024-02-16", "2024-02-23"),
        )

    @patch("src.predictions.rudderstack_predictions.utils.utils.date_add")
    @patch("src.predictions.rudderstack_predictions.utils.utils.dates_proximity_check")
    @patch("src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff")
    def test_find_valid_feature_date(
        self, mock_get_abs_date_diff, mock_dates_proximity_check, mock_date_add
    ):
        """Test finding a valid feature date"""
        max_feature_date = "2024-02-26"
        min_feature_date = "2024-02-18"
        training_dates = ["2024-02-26", "2024-02-18"]
        prediction_horizon_days = 7
        feature_data_min_date_diff = 5

        mock_get_abs_date_diff.return_value = 8
        mock_dates_proximity_check.side_effect = [
            False,
            False,
            True,
        ]  # Fourth call returns True

        mock_date_add.side_effect = [
            "2024-02-21",
            "2024-02-16",
            "2024-02-11",
            "2024-02-18",
        ]

        result = generate_new_training_dates(
            max_feature_date,
            min_feature_date,
            training_dates,
            prediction_horizon_days,
            feature_data_min_date_diff,
        )

        self.assertEqual(result, ("2024-02-11", "2024-02-18"))

    @patch("src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff")
    def test_invalid_max_feature_date(self, mock_date_diff):
        """Test with invalid max_feature_date format"""
        max_feature_date = "invalid_date_format"
        min_feature_date = "2024-02-15"
        training_dates = ["2024-02-20", "2024-02-15"]
        prediction_horizon_days = 7
        feature_data_min_date_diff = 5

        mock_date_diff.side_effect = ValueError  # Exception raised by date_diff
        with self.assertRaises(ValueError):
            generate_new_training_dates(
                max_feature_date,
                min_feature_date,
                training_dates,
                prediction_horizon_days,
                feature_data_min_date_diff,
            )


class TestDatetimeToDateString(unittest.TestCase):
    def test_datetime_with_time(self):
        """Test with a datetime string with time components"""
        datetime_str = "2024-02-26 15:30:45"
        result = datetime_to_date_string(datetime_str)
        self.assertEqual(result, "2024-02-26")

    def test_datetime_with_timezone(self):
        """Test with a datetime string with time components"""
        datetime_str = "2024-02-26 15:30:45+05:30"
        result = datetime_to_date_string(datetime_str)
        self.assertEqual(result, "2024-02-26")

    def test_date_only_string(self):
        """Test with a date-only string"""
        datetime_str = "2024-02-26"
        result = datetime_to_date_string(datetime_str)
        self.assertEqual(result, "2024-02-26")

    def test_invalid_format(self):
        """Test with an invalid datetime format"""
        datetime_str = "2024/02/26"
        result = datetime_to_date_string(datetime_str)
        self.assertEqual(result, "")
