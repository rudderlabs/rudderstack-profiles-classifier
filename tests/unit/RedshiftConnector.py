# Generated by CodiumAI
from pandas.core.api import DataFrame as DataFrame
from redshift_connector.cursor import Cursor
from src.connectors.RedshiftConnector import RedshiftConnector
import pandas as pd
from unittest.mock import Mock, patch
from src.utils.constants import TrainTablesInfo
import unittest


class TestGetMaterialRegistryTable(unittest.TestCase):
    # Returns a filtered material registry table containing only the successfully materialized data.
    def test_returns_filtered_material_registry_table(self):
        class MockRedshiftConnector(RedshiftConnector):
            def __init__(self, folder_path):
                self.folder_path = folder_path

            def get_table_as_dataframe(
                self, cursor: Cursor, table_name: str, **kwargs
            ) -> DataFrame:
                material_registry_table = pd.DataFrame.from_dict(
                    {
                        "seq_no": [1, 2, 3, 4, 5],
                        "metadata": [
                            '{"complete": {"status": 1}}',
                            '{"complete": {"status": 2}}',
                            None,
                            "null",
                            "{}",
                        ],
                    }
                )
                return material_registry_table

        redshift_connector = MockRedshiftConnector(folder_path="data")
        material_registry_table = redshift_connector.get_material_registry_table(
            cursor=None, material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict(
            {"seq_no": [2], "metadata": ['{"complete": {"status": 2}}'], "status": [2]}
        )
        self.assertEqual(
            material_registry_table.values.all(), expected_registry_table.values.all()
        )

    def test_returns_filtered_material_registry_table_empty_resp(self):
        class MockRedshiftConnector(RedshiftConnector):
            def __init__(self, folder_path):
                self.folder_path = folder_path

            def get_table_as_dataframe(
                self, cursor: Cursor, table_name: str, **kwargs
            ) -> DataFrame:
                material_registry_table = pd.DataFrame.from_dict(
                    {
                        "seq_no": [1, 2, 3, 4, 5],
                        "metadata": [
                            '{"complete": {"status": 1}}',
                            '{"complete": {"status": 1}}',
                            None,
                            "null",
                            "{}",
                        ],
                    }
                )
                return material_registry_table

        redshift_connector = MockRedshiftConnector(folder_path="data")
        material_registry_table = redshift_connector.get_material_registry_table(
            cursor=None, material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict({})
        self.assertEqual(
            material_registry_table.values.all(), expected_registry_table.values.all()
        )

    def test_runs_on_empty_material_registry_table(self):
        class MockRedshiftConnector(RedshiftConnector):
            def __init__(self, folder_path):
                self.folder_path = folder_path

            def get_table_as_dataframe(
                self, cursor: Cursor, table_name: str, **kwargs
            ) -> DataFrame:
                material_registry_table = pd.DataFrame.from_dict(
                    {"seq_no": [], "metadata": []}
                )
                return material_registry_table

        redshift_connector = MockRedshiftConnector(folder_path="data")

        # Call the get_material_registry_table method
        material_registry_table = redshift_connector.get_material_registry_table(
            cursor=None, material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict({})
        self.assertEqual(
            material_registry_table.values.all(), expected_registry_table.values.all()
        )


class TestGetMaterialNames(unittest.TestCase):
    def setUp(self) -> None:
        self.session_mock = Mock()
        self.connector = RedshiftConnector("data")
        self.material_table = "material_table"
        self.start_date = "2022-01-01"
        self.end_date = "2022-01-31"
        self.features_profiles_model = "model_name"
        self.model_hash = "model_hash"
        self.material_table_prefix = "material_prefix"
        self.prediction_horizon_days = 7
        self.output_filename = "output_file.csv"
        self.site_config_path = "siteconfig.yaml"
        self.project_folder = "project_folder"
        self.input_models = ["model1.yaml", "model2.yaml"]
        self.inputs = ["""select * from material_user_var_736465_0"""]

    # Retrieves material names and training dates when materialized data is available within the specified date range
    def test_retrieves_material_names_within_date_range(self):
        # Set up the expected input and output
        input_materials = [("feature_table_name", "label_table_name")]
        input_training_dates = [("feature_table_dt", "label_table_dt")]
        feature_label_joined_table = None
        expected_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            )
        ]

        # Mock the internal method get_material_names_
        self.connector.get_material_names_ = Mock(
            return_value=(input_materials, input_training_dates, feature_label_joined_table)
        )
        # Invoke the method under test
        materials = self.connector.get_material_names(
            self.session_mock,
            self.material_table,
            self.start_date,
            self.end_date,
            self.features_profiles_model,
            self.model_hash,
            self.material_table_prefix,
            self.prediction_horizon_days,
            self.output_filename,
            self.site_config_path,
            self.project_folder,
            self.input_models,
            self.inputs,
        )
        # Assert the result
        self.assertEqual(materials, expected_materials)

    # Materializes feature and label data if no materialized data is found within the specified date range and retrieves material names and training dates
    def test_materializes_data_if_not_found_within_date_range(self):
        # Set up the expected input and output
        input_materials = [("feature_table_name", "label_table_name")]
        input_training_dates = [("feature_table_dt", "label_table_dt")]
        feature_label_joined_table = None
        expected_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            )
        ]
        # Mock the internal methods get_material_names_ and generate_training_materials
        self.connector.get_material_names_ = Mock(
            side_effect=[([], [], None), (input_materials, input_training_dates, feature_label_joined_table)]
        )
        self.connector.generate_training_materials = self.session_mock()

        # Invoke the method under test
        materials = self.connector.get_material_names(
            self.session_mock,
            self.material_table,
            self.start_date,
            self.end_date,
            self.features_profiles_model,
            self.model_hash,
            self.material_table_prefix,
            self.prediction_horizon_days,
            self.output_filename,
            self.site_config_path,
            self.project_folder,
            self.input_models,
            self.inputs,
        )

        # Assert the result
        self.assertEqual(materials, expected_materials)
        self.connector.generate_training_materials.assert_called_once_with(
            self.session_mock,
            feature_label_joined_table,
            self.start_date,
            self.features_profiles_model,
            self.model_hash,
            self.prediction_horizon_days,
            self.output_filename,
            self.site_config_path,
            self.project_folder,
            self.input_models,
        )

    def test_materializes_data_once_even_if_it_cant_find_right_materials(self):
        # Mock the internal methods get_material_names_ and generate_training_materials
        feature_label_joined_table = None
        self.connector.get_material_names_ = Mock(return_value=([], [], feature_label_joined_table))
        self.connector.generate_training_materials = Mock()

        # Invoke the method under test and assert exception
        with self.assertRaises(Exception) as context:
            self.connector.get_material_names(
                self.session_mock,
                self.material_table,
                self.start_date,
                self.end_date,
                self.features_profiles_model,
                self.model_hash,
                self.material_table_prefix,
                self.prediction_horizon_days,
                self.output_filename,
                self.site_config_path,
                self.project_folder,
                self.input_models,
                self.inputs,
            )
        # Check the exception message
        self.assertIn(
            "Tried to materialise past data but no materialized data found",
            str(context.exception),
        )

        # Assert generate_training_materials called once
        self.connector.generate_training_materials.assert_called_once_with(
            self.session_mock,
            feature_label_joined_table,
            self.start_date,
            self.features_profiles_model,
            self.model_hash,
            self.prediction_horizon_days,
            self.output_filename,
            self.site_config_path,
            self.project_folder,
            self.input_models,
        )


class TestSelectRelevantColumns(unittest.TestCase):
    # Returns a pandas DataFrame with only the columns specified in the training_features_columns dictionary.
    def test_relevant_columns_only(self):
        table = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": [4, 5, 6],
                "col3": [7, 8, 9],
                "col4": [10, 11, 12],
            }
        )
        training_features_columns = ["COL1", "COL2", "COL3"]
        redshift_connector = RedshiftConnector("data")
        relevant_columns = redshift_connector.select_relevant_columns(
            table, training_features_columns
        )
        expected_columns = ["col1", "col2", "col3"]
        self.assertEqual(list(relevant_columns.columns), expected_columns)

    def test_relevant_columns_only_handling_case_sensitivity(self):
        table = pd.DataFrame(
            {
                "COL1": [1, 2, 3],
                "col2": [4, 5, 6],
                "col3": [7, 8, 9],
                "col4": [10, 11, 12],
            }
        )
        training_features_columns = ["COL1", "COL2", "COL3"]
        redshift_connector = RedshiftConnector("data")
        relevant_columns = redshift_connector.select_relevant_columns(
            table, training_features_columns
        )
        expected_columns = ["COL1", "col2", "col3"]
        self.assertEqual(list(relevant_columns.columns), expected_columns)

    # Throws an exception that the expected column is not found
    def test_relevant_columns_not_found(self):
        table = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": [4, 5, 6],
                "col3": [7, 8, 9],
                "col4": [10, 11, 12],
            }
        )
        training_features_columns = ["COL1", "COL2", "COL3", "COL5"]
        redshift_connector = RedshiftConnector("data")
        with self.assertRaises(Exception) as context:
            redshift_connector.select_relevant_columns(table, training_features_columns)
        self.assertIn(
            f"Expected columns {training_features_columns} not found in table ['COL1', 'COL2', 'COL3']",
            str(context.exception),
        )


class TestGenerateTypeHint(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = RedshiftConnector("data")
        self.column_types = {
            "categorical_columns": ["col2"],
            "numeric_columns": ["col1"],
        }
        return super().setUp()

    # Returns a list of type hints for given pandas DataFrame's fields
    def test_returns_type_hints(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        type_hints = self.connector.generate_type_hint(df, self.column_types)
        self.assertEqual(type_hints, [float, str])

    # Handles empty DataFrame
    def test_handles_empty_dataframe(self):
        df = pd.DataFrame()
        type_hints = self.connector.generate_type_hint(df, self.column_types)
        self.assertEqual(type_hints, [])

    # Handles DataFrame with single row and column
    def test_handles_single_row_and_column(self):
        df = pd.DataFrame({"col1": [1]})
        type_hints = self.connector.generate_type_hint(df, self.column_types)
        self.assertEqual(type_hints, [float])


class TestValidations(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = RedshiftConnector("data")
        df = pd.DataFrame.from_dict(
            {
                "COL1": ["a", "a"],
                "COL2": [1, 2],
                "COL3": [None, None],
                "COL4": ["a1", "b1"],
            }
        )
        self.table = df

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
    def test_expects_error_if_label_ratios_are_bad_classification(self):
        label_column = "COL1"
        with self.assertRaises(Exception) as context:
            self.connector.validate_class_proportions(
                self.table[["COL1", "COL2", "COL3"]],
                label_column,
            )
        self.assertIn(
            f"Label column {label_column} has invalid proportions.",
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

    @patch("src.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION", new=0.05)
    @patch("src.utils.constants.CLASSIFIER_MAX_LABEL_PROPORTION", new=0.95)
    def test_passes_for_good_data_classification(self):
        table = pd.DataFrame.from_dict(
            {
                "COL1": ["a", "b", "a"],
                "COL2": [1, 2, 3],
                "COL3": [None, None, None],
                "COL4": ["a1", "b1", "c1"],
            }
        )
        self.assertTrue(self.connector.validate_columns_are_present(table, "COL1"))
        self.assertTrue(self.connector.validate_class_proportions(table, "COL1"))

    @patch("src.utils.constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES", new=3)
    def test_passes_for_good_data_regression(self):
        table = pd.DataFrame.from_dict(
            {
                "COL1": [1, 2, 3, 4],
                "COL2": [1, 2, 3, 4],
                "COL3": [None, None, None, None],
                "COL4": ["a1", "b1", "c1", "d1"],
            }
        )
        self.assertTrue(self.connector.validate_columns_are_present(table, "COL1"))
        self.assertTrue(self.connector.validate_label_distinct_values(table, "COL1"))
