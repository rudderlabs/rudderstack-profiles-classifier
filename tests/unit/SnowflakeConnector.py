# Generated by CodiumAI
from src.predictions.rudderstack_predictions.connectors.SnowflakeConnector import (
    SnowflakeConnector,
)
import pandas as pd
from unittest.mock import Mock, patch
import unittest
from snowflake.snowpark import Session
import snowflake.snowpark.types as T


class TestLabelTable(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.config("local_testing", True).create()
        self.connector = SnowflakeConnector()
        df = pd.DataFrame.from_dict(
            {
                "COL1": ["2021-01-01 00:00:00", "2021-01-01 00:00:00"],
                "COL2": ["value1", "value2"],
                "COL3": ["2021-01-01", "2021-01-01"],
                "COL4": ["2021-01-01 00:00:00+00:00", "2021-01-01 00:00:00+00:00"],
            }
        )
        self.table = self.session.create_dataframe(df)
        self.connector.get_table = Mock(return_value=self.table)
        self.label_column = "COL2"
        self.entity_column = "COL3"

    def test_label_table_returns_only_required_cols(self):
        positive_class = "value1"
        actual = self.connector.label_table(
            self.session, None, self.label_column, self.entity_column, positive_class
        )
        self.assertListEqual(actual.columns, [self.entity_column, self.label_column])

    def test_label_table_changes_label_value_for_classification(self):
        positive_class = "value1"
        actual = self.connector.label_table(
            self.session, None, self.label_column, self.entity_column, positive_class
        )
        actual_label_col_vals = [
            v.as_dict()[self.label_column] for v in actual.collect()
        ]
        expected_label_col_vals = [1, 0]
        self.assertListEqual(actual_label_col_vals, expected_label_col_vals)

    def test_label_table_does_not_change_label_value_for_regression(self):
        actual = self.connector.label_table(
            self.session, None, self.label_column, self.entity_column, None
        )
        actual_label_col_vals = [
            v.as_dict()[self.label_column] for v in actual.collect()
        ]
        expected_label_col_vals = ["value1", "value2"]
        self.assertListEqual(actual_label_col_vals, expected_label_col_vals)


class TestGenerateTypeHint(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.config("local_testing", True).create()
        self.connector = SnowflakeConnector()

    # Returns a list of type hints for given pandas DataFrame's fields
    def test_returns_type_hints(self):
        df = pd.DataFrame.from_dict(
            {
                "COL1": ["a", "b"],
                "COL2": [1, 2],
                "COL3": [1.1, 2.2],
                "COL4": ["a1", "b1"],
            }
        )
        table = self.session.create_dataframe(df)
        column_types = {
            "categorical": ["COL1", "COL4"],
            "numeric": ["COL2", "COL3"],
        }
        input_column_types_map = {
            "numeric": {"COL2": "IntegerType", "COL3": "FloatType"},
            "categorical": {"COL1": "StringType", "COL4": "StringType"},
        }

        type_hints = self.connector.generate_type_hint(
            table, column_types, input_column_types_map
        )
        self.assertEqual(
            type_hints, [T.StringType(), T.IntegerType(), T.FloatType(), T.StringType()]
        )

    # Handles DataFrame with single row and column
    def test_handles_single_row_and_column(self):
        df = pd.DataFrame({"COL1": [1]})
        table = self.session.create_dataframe(df)
        column_types = {
            "categorical": {},
            "numeric": ["COL1"],
        }
        input_column_types_map = {"numeric": {"COL1": "IntegerType"}}

        type_hints = self.connector.generate_type_hint(
            table, column_types, input_column_types_map
        )
        self.assertEqual(type_hints, [T.IntegerType()])


class TestSelectRelevantColumns(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.config("local_testing", True).create()
        self.connector = SnowflakeConnector()
        df = pd.DataFrame.from_dict(
            {
                "COL1": ["a", "b"],
                "COL2": [1, 2],
                "COL3": [None, None],
                "COL4": ["a1", "b1"],
            }
        )
        self.table = self.session.create_dataframe(df)

    # Returns a pandas DataFrame with only the columns specified in the training_features_columns dictionary.
    def test_relevant_columns_only(self):
        training_features_columns = ["COL3", "COL2", "COL1"]
        relevant_columns = self.connector.select_relevant_columns(
            self.table, training_features_columns
        )
        expected_columns = ["COL1", "COL2", "COL3"]
        self.assertEqual(list(relevant_columns.columns), expected_columns)

    # Throws an exception that the expected column is not found
    def test_relevant_columns_not_found(self):
        training_features_columns = ["COL1", "COL2", "COL5"]
        with self.assertRaises(Exception) as context:
            self.connector.select_relevant_columns(
                self.table, training_features_columns
            )
        self.assertIn(
            "Expected feature column COL5 not found in the predictions input table",
            str(context.exception),
            [],
        )


class TestValidations(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.config("local_testing", True).create()
        self.connector = SnowflakeConnector()
        df = pd.DataFrame.from_dict(
            {
                "COL1": ["a", "a"],
                "COL2": [1, 2],
                "COL3": [None, None],
                "COL4": ["a1", "b1"],
            }
        )
        self.table = self.session.create_dataframe(df)

    # Checks for assertion error if label column is not present in the feature table.
    def test_label_column_not_present(self):
        label_column = "label"
        with self.assertRaises(Exception) as context:
            self.connector.validate_columns_are_present(self.table, label_column)
        self.assertIn(
            f"Label column {label_column} is not present in the feature table.",
            str(context.exception),
            [],
        )

    # Checks if no:of columns in the feature table is less than 3, then it raises an exception.
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION",
        new=1.0,
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MAX_LABEL_PROPORTION",
        new=0.0,
    )
    def test_expects_error_if_label_ratios_are_bad_classification(self):
        label_column = "COL2"
        with self.assertRaises(Exception) as context:
            self.connector.validate_class_proportions(
                self.table.select("COL1", "COL2", "COL3"),
                label_column,
            )
        error_msg = "1 - user count:  1 (50.00%)\n\t2 - user count:  1 (50.00%)"
        self.assertIn(
            error_msg,
            str(context.exception),
            [],
        )

    def test_expects_error_if_label_count_is_low_regression(self):
        label_column = "COL1"
        with self.assertRaises(Exception) as context:
            self.connector.validate_label_distinct_values(self.table, label_column)
        self.assertIn(
            f"Label column {label_column} has invalid number of distinct values.",
            str(context.exception),
            [],
        )

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION",
        new=0.05,
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MAX_LABEL_PROPORTION",
        new=0.95,
    )
    def test_passes_for_good_data_classification(self):
        df = pd.DataFrame.from_dict(
            {
                "COL1": ["a", "a", "b"],
                "COL2": [1, 2, 3],
                "COL3": [None, None, None],
                "COL4": ["a1", "b1", "c1"],
            }
        )
        table = self.session.create_dataframe(df)
        self.assertTrue(self.connector.validate_columns_are_present(table, "COL1"))
        self.assertTrue(self.connector.validate_class_proportions(table, "COL1"))

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES",
        new=3,
    )
    def test_passes_for_good_data_regression(self):
        df = pd.DataFrame.from_dict(
            {
                "COL1": [1, 2, 3, 4],
                "COL2": [1, 2, 3, 4],
                "COL3": [None, None, None, None],
                "COL4": ["a1", "b1", "c1", "d1"],
            }
        )
        table = self.session.create_dataframe(df)
        self.assertTrue(self.connector.validate_columns_are_present(table, "COL1"))
        self.assertTrue(self.connector.validate_label_distinct_values(table, "COL1"))
