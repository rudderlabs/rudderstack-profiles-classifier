import unittest
import src.utils.utils as utils

from unittest.mock import patch
from src.wht.pb import getPB


class TestReplaceSeqNoInQuery(unittest.TestCase):
    def test_replaces_seq_no_correctly(self):
        query = "SELECT * FROM material_user_var_table_123"
        seq_no = 567
        expected_result = "SELECT * FROM material_user_var_table_567"
        actual_result = utils.replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_empty_query(self):
        query = ""
        seq_no = 5
        expected_result = "_5"
        actual_result = utils.replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_missing_seq_no(self):
        query = "SELECT * FROM material_user_var_table_"
        seq_no = 33
        expected_result = "SELECT * FROM material_user_var_table_33"
        actual_result = utils.replace_seq_no_in_query(query, seq_no)
        self.assertEqual(expected_result, actual_result)

    def test_handles_non_string_input(self):
        with self.assertRaises(AttributeError):
            utils.replace_seq_no_in_query(123, "456")


class TestSplitMaterialTable(unittest.TestCase):
    def test_valid_table_name(self):
        table_name = "Material_user_var_table_54ddc22a_383"
        expected_result = ("user_var_table", "54ddc22a", 383)
        actual_result = getPB().split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_missing_prefix(self):
        table_name = "user_var_table_54ddc22a_383"
        expected_result = (None, None, None)
        actual_result = getPB().split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_missing_seq_no(self):
        table_name = "Material_user_var_table_54ddc22a"
        expected_result = (None, None, None)
        actual_result = getPB().split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_invalid_seq_no(self):
        table_name = "Material_user_var_table_54ddc22a_foo"
        expected_result = (None, None, None)
        actual_result = getPB().split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_invalid_table_name(self):
        table_name = "user_var_table_54ddc22a_foo"
        expected_result = (None, None, None)
        actual_result = getPB().split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)

    def test_material_query(self):
        table_name = "SELECT * FROM SCHEMA.Material_user_var_table_54ddc22a_383"
        expected_result = ("user_var_table", "54ddc22a", 383)
        actual_result = getPB().split_material_table(table_name)
        self.assertEqual(actual_result, expected_result)


class TestDateDiff(unittest.TestCase):
    def test_same_date(self):
        """Test for same dates"""
        result = utils.date_diff("2023-11-21", "2023-11-21")
        self.assertEqual(result, 0)

    def test_different_dates(self):
        """Test for different dates"""
        result = utils.date_diff("2023-11-20", "2023-11-22")
        self.assertEqual(result, 2)

    def test_date_order(self):
        """Test for different date order"""
        result = utils.date_diff("2024-01-15", "2023-12-25")
        self.assertEqual(result, 21)

    def test_invalid_date_format(self):
        """Test for invalid date format"""
        with self.assertRaises(ValueError):
            utils.date_diff("2023/11/21", "2023-11-22")

    def test_invalid_date_values(self):
        """Test for invalid date values"""
        with self.assertRaises(ValueError):
            utils.date_diff("2023-11-31", "2023-11-22")


class TestDatesProximityCheck(unittest.TestCase):
    @patch("src.utils.utils.date_diff")  # Mock the date_diff function
    def test_no_date_within_distance(self, mock_date_diff):
        """Test when no date is within the specified distance"""
        mock_date_diff.side_effect = [10, 15, 20]  # Mock return values for date_diff
        result = utils.dates_proximity_check(
            "2023-11-21", ["2023-11-11", "2023-11-06", "2023-10-21"], 5
        )
        self.assertTrue(result)

    @patch("src.utils.utils.date_diff")
    def test_date_within_distance(self, mock_date_diff):
        """Test when a date is within the specified distance"""
        mock_date_diff.side_effect = [4, 15, 20]
        result = utils.dates_proximity_check(
            "2023-11-21", ["2023-11-17", "2023-11-06", "2023-10-21"], 5
        )
        self.assertFalse(result)

    @patch("src.utils.utils.date_diff")
    def test_empty_date_list(self, mock_date_diff):
        """Test with an empty date list"""
        mock_date_diff.side_effect = []  # No calls to date_diff expected
        result = utils.dates_proximity_check("2023-11-21", [], 5)
        self.assertTrue(result)


class TestGenerateNewTrainingDates(unittest.TestCase):
    @patch.object(utils, "date_add")
    @patch.object(utils, "dates_proximity_check")
    def test_no_training_dates(self, mock_dates_proximity_check, mock_date_add):
        """Test with no training dates"""
        max_feature_date = "2024-02-26"
        prediction_horizon_days = 7
        feature_data_min_date_diff = 10

        mock_date_add.side_effect = ["2024-02-16", "2024-02-23"]
        mock_dates_proximity_check.return_value = True

        result = utils.generate_new_training_dates(
            max_feature_date, [], prediction_horizon_days, feature_data_min_date_diff
        )

        self.assertEqual(
            result,
            ("2024-02-16", "2024-02-23"),
        )

    @patch.object(utils, "date_add")
    @patch.object(utils, "dates_proximity_check")
    def test_find_valid_feature_date(self, mock_dates_proximity_check, moc_date_add):
        """Test finding a valid feature date"""
        max_feature_date = "2024-02-26"
        training_dates = ["2024-02-26", "2024-02-18"]
        prediction_horizon_days = 7
        feature_data_min_date_diff = 5

        mock_dates_proximity_check.side_effect = [
            False,
            False,
            False,
            True,
        ]  # Fourth call returns True

        moc_date_add.side_effect = [
            "2024-02-25",
            "2024-02-20",
            "2024-02-15",
            "2024-02-10",
            "2024-02-17",
        ]

        result = utils.generate_new_training_dates(
            max_feature_date,
            training_dates,
            prediction_horizon_days,
            feature_data_min_date_diff,
        )

        self.assertEqual(result, ("2024-02-10", "2024-02-17"))

    @patch.object(utils, "date_diff")
    def test_invalid_max_feature_date(self, mock_date_diff):
        """Test with invalid max_feature_date format"""
        max_feature_date = "invalid_date_format"
        training_dates = ["2024-02-20", "2024-02-15"]
        prediction_horizon_days = 7
        feature_data_min_date_diff = 5

        mock_date_diff.side_effect = ValueError  # Exception raised by date_diff
        with self.assertRaises(ValueError):
            utils.generate_new_training_dates(
                max_feature_date,
                training_dates,
                prediction_horizon_days,
                feature_data_min_date_diff,
            )


class TestGetMaxDateString(unittest.TestCase):
    def test_single_date(self):
        """Test with a single date"""
        dates = ["2024-02-26"]
        result = utils.get_max_date_string(dates)
        self.assertEqual(result, "2024-02-26")

    def test_multiple_dates(self):
        """Test with multiple dates"""
        dates = ["2024-02-22", "2024-02-26", "2024-02-23"]
        result = utils.get_max_date_string(dates)
        self.assertEqual(result, "2024-02-26")

    def test_empty_list(self):
        """Test with an empty list of dates"""
        dates = []
        result = utils.get_max_date_string(dates)
        self.assertEqual(result, "")

    def test_invalid_date_format(self):
        """Test with invalid date format"""
        dates = ["2024/02/26", "2024-02-twenty-six"]
        result = utils.get_max_date_string(dates)
        self.assertEqual(result, "")


class TestDatetimeToDateString(unittest.TestCase):
    def test_datetime_with_time(self):
        """Test with a datetime string with time components"""
        datetime_str = "2024-02-26 15:30:45"
        result = utils.datetime_to_date_string(datetime_str)
        self.assertEqual(result, "2024-02-26")

    def test_date_only_string(self):
        """Test with a date-only string"""
        datetime_str = "2024-02-26"
        result = utils.datetime_to_date_string(datetime_str)
        self.assertEqual(result, "")

    def test_invalid_format(self):
        """Test with an invalid datetime format"""
        datetime_str = "2024/02/26"
        result = utils.datetime_to_date_string(datetime_str)
        self.assertEqual(result, "")
