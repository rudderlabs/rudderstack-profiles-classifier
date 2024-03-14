# Generated by CodiumAI
import pandas as pd
from datetime import datetime
import unittest
from unittest.mock import Mock, patch, MagicMock, call
from redshift_connector.cursor import Cursor
from pandas.core.api import DataFrame as DataFrame
from src.predictions.rudderstack_predictions.trainers.MLTrainer import (
    ClassificationTrainer,
)
from src.predictions.rudderstack_predictions.wht.pythonWHT import PythonWHT

import src.predictions.rudderstack_predictions.utils.utils as utils
from src.predictions.rudderstack_predictions.utils.constants import TrainTablesInfo
from src.predictions.rudderstack_predictions.connectors.RedshiftConnector import (
    RedshiftConnector,
)
from tests.unit.MLTrainer import build_trainer_config


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
            session=None, material_registry_table_name=None
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
            session=None, material_registry_table_name=None
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
            session=None, material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict({})
        self.assertEqual(
            material_registry_table.values.all(), expected_registry_table.values.all()
        )


class TestGetMaterialNames(unittest.TestCase):
    def setUp(self) -> None:
        self.session_mock = Mock()
        self.connector = RedshiftConnector("data")
        self.start_date = "2022-01-01"
        self.end_date = "2022-01-31"
        self.features_profiles_model = "model_name"
        self.model_hash = "model_hash"
        self.prediction_horizon_days = 7
        self.input_models = ["model1.yaml", "model2.yaml"]
        self.inputs = ["""select * from material_user_var_736465_0"""]
        self.whtService = PythonWHT()
        self.whtService.init(
            self.connector, self.session_mock, "siteconfig.yaml", "project_folder"
        )

    def test_fetch_filtered_table(self):
        # Set up the expected input and output
        convert_to_timestamp = lambda date_string: datetime.strptime(
            date_string, "%Y-%m-%d %H:%M:%S"
        )
        input_table = pd.DataFrame(
            {
                "model_name": [
                    self.features_profiles_model,
                    self.features_profiles_model,
                    self.features_profiles_model,
                    f"NOT_{self.features_profiles_model}",
                ],
                "model_hash": [
                    self.model_hash,
                    self.model_hash,
                    self.model_hash,
                    f"NOT_{self.model_hash}",
                ],
                "end_ts": [
                    convert_to_timestamp("2022-01-01 07:29:25"),
                    convert_to_timestamp("2022-01-01 00:00:00"),
                    convert_to_timestamp("2022-01-31 00:00:00"),
                    convert_to_timestamp("2022-01-31 08:29:25"),
                ],
                "seq_no": [13, 14, 15, 16],
            }
        )
        expected_result = pd.DataFrame(
            {
                "FEATURE_SEQ_NO": [13, 14, 15],
                "FEATURE_END_TS": [
                    convert_to_timestamp("2022-01-01 07:29:25"),
                    convert_to_timestamp("2022-01-01 00:00:00"),
                    convert_to_timestamp("2022-01-31 00:00:00"),
                ],
            }
        )

        # Invoke the method under test
        result = self.connector.fetch_filtered_table(
            input_table,
            self.features_profiles_model,
            self.model_hash,
            self.start_date,
            self.end_date,
            columns={"seq_no": "FEATURE_SEQ_NO", "end_ts": "FEATURE_END_TS"},
        )

        # Assert the result
        self.assertEqual(result.to_dict(), expected_result.to_dict())

    def test_get_valid_feature_label_dates_with_both_materials(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            ),
        ]
        material_info = input_materials[0]

        # Mock the internal method is_valid_table
        self.connector.is_valid_table = Mock(return_value=True)

        # Invoke the method under test
        with self.assertRaises(Exception) as context:
            _ = self.whtService._get_valid_feature_label_dates(
                input_materials,
                self.start_date,
                self.prediction_horizon_days,
            )
        # Check the exception message
        self.assertIn(
            f"We don't need to fetch feature_date and label_date to materialise new datasets because Tables {material_info.feature_table_name} and {material_info.label_table_name} already exist. Please check generated materials for discrepancies.",
            str(context.exception),
        )

    def test_get_valid_feature_label_dates_with_feature_materials(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2022-01-10",
                label_table_name=None,
                label_table_date=None,
            ),
        ]
        expected_date = (None, "2022-01-17")

        # Mock the internal method is_valid_table
        self.connector.is_valid_table = Mock(return_value=True)

        # Invoke the method under test
        dates = self.whtService._get_valid_feature_label_dates(
            input_materials,
            self.start_date,
            self.prediction_horizon_days,
        )
        # Assert the result
        self.assertEqual(dates, expected_date)

    def test_get_valid_feature_label_dates_with_label_materials(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name=None,
                feature_table_date=None,
                label_table_name="label_table_name",
                label_table_date="2022-01-20",
            ),
        ]
        expected_date = ("2022-01-13", None)

        # Mock the internal method is_valid_table
        self.connector.is_valid_table = Mock(return_value=True)

        # Invoke the method under test
        dates = self.whtService._get_valid_feature_label_dates(
            input_materials,
            self.start_date,
            self.prediction_horizon_days,
        )
        # Assert the result
        self.assertEqual(dates, expected_date)

    def test_get_valid_feature_label_dates_with_no_materials(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name=None,
                feature_table_date=None,
                label_table_name=None,
                label_table_date=None,
            ),
        ]
        expected_date = ("2022-01-08", "2022-01-15")

        # Mock the internal method is_valid_table
        self.connector.is_valid_table = Mock(return_value=True)

        # Invoke the method under test
        dates = self.whtService._get_valid_feature_label_dates(
            input_materials,
            self.start_date,
            self.prediction_horizon_days,
        )
        # Assert the result
        self.assertEqual(dates, expected_date)

    def test_generate_training_materials_with_only_feature_material(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name=None,
                label_table_date=None,
            ),
        ]
        expected_date = (None, "2000-01-01")

        # Mock the internal method get_valid_feature_label_dates
        self.whtService._get_valid_feature_label_dates = Mock(
            return_value=expected_date
        )
        utils.subprocess_run = Mock()

        # Invoke the method under test
        self.whtService._generate_training_materials(
            input_materials,
            self.start_date,
            self.prediction_horizon_days,
            self.input_models,
        )
        utils.subprocess_run.assert_called_once_with(
            [
                "pb",
                "run",
                "-p",
                "project_folder",
                "-m",
                "model1.yaml,model2.yaml",
                "--migrate_on_load=True",
                "--end_time",
                "946684800",
                "-c",
                "siteconfig.yaml",
            ]
        )

    # Retrieves material names and training dates when materialized data is available within the specified date range
    def test_retrieves_material_names_within_date_range(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            ),
        ]
        expected_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            )
        ]

        # Mock the internal method get_material_names_
        self.whtService._get_material_names = Mock(return_value=input_materials)
        # Invoke the method under test
        materials = self.whtService.get_material_names(
            self.start_date,
            self.end_date,
            self.features_profiles_model,
            self.model_hash,
            self.prediction_horizon_days,
            self.input_models,
            self.inputs,
        )
        # Assert the result
        self.assertEqual(materials, expected_materials)

    # Materializes feature and label data if no materialized data is found within the specified date range and retrieves material names and training dates
    def test_materializes_data_if_not_found_within_date_range(self):
        # Set up the expected input and output
        input_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            ),
        ]
        expected_materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="feature_table_dt",
                label_table_name="label_table_name",
                label_table_date="label_table_dt",
            )
        ]
        # Mock the internal methods get_material_names_ and generate_training_materials
        self.whtService._get_material_names = Mock(side_effect=[[], input_materials])
        utils.subprocess_run = Mock()

        # Invoke the method under test
        materials = self.whtService.get_material_names(
            self.start_date,
            self.end_date,
            self.features_profiles_model,
            self.model_hash,
            self.prediction_horizon_days,
            self.input_models,
            self.inputs,
        )

        # Assert the result
        self.assertEqual(materials, expected_materials)
        utils.subprocess_run.assert_called_with(
            [
                "pb",
                "run",
                "-p",
                "project_folder",
                "-m",
                "model1.yaml,model2.yaml",
                "--migrate_on_load=True",
                "--end_time",
                "1642204800",
                "-c",
                "siteconfig.yaml",
            ]
        )

    def test_materializes_data_once_even_if_it_cant_find_right_materials(self):
        # Mock the internal methods get_material_names_ and generate_training_materials
        self.whtService._get_material_names = Mock(return_value=[])
        self.whtService._generate_training_materials = Mock()

        # Invoke the method under test and assert exception
        with self.assertRaises(Exception) as context:
            self.whtService.get_material_names(
                self.start_date,
                self.end_date,
                self.features_profiles_model,
                self.model_hash,
                self.prediction_horizon_days,
                self.input_models,
                self.inputs,
            )
        # Check the exception message
        self.assertIn(
            "Tried to materialise past data but no materialized data found",
            str(context.exception),
        )

        # Assert generate_training_materials called once
        self.whtService._generate_training_materials.assert_called_once_with(
            [],
            self.start_date,
            self.prediction_horizon_days,
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

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION",
        new=0.05,
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MAX_LABEL_PROPORTION",
        new=0.95,
    )
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

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES",
        new=3,
    )
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


class TestCheckForClassificationDataRequirement(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = RedshiftConnector("data")

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.MIN_NUM_OF_SAMPLES",
        new=90,
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION",
        new=0.01,
    )
    def test_enough_negative_and_total_samples(self):
        """Test when there are enough negative samples"""
        cursor = MagicMock()
        materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]
        label_column = "label"

        self.connector.run_query = Mock(side_effect=[([15],), ([100],)])
        result = self.connector.check_for_classification_data_requirement(
            cursor, materials, label_column, 1
        )

        self.assertTrue(result)
        self.connector.run_query.assert_any_call(
            cursor,
            """SELECT COUNT(*) as count
                FROM label_table_name
                WHERE label != 1""",
            response=True,
        )

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.MIN_NUM_OF_SAMPLES",
        new=90,
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION",
        new=0.01,
    )
    def test_insufficient_negative_and_total_samples(self):
        """Test when there are not enough negative samples"""
        cursor = MagicMock()
        materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]
        label_column = "label"

        self.connector.run_query = Mock(side_effect=[([9],), ([80],)])
        result = self.connector.check_for_classification_data_requirement(
            cursor, materials, label_column, 1
        )

        self.assertFalse(result)
        self.connector.run_query.assert_any_call(
            cursor,
            """SELECT COUNT(*) as count
                FROM label_table_name""",
            response=True,
        )

    def test_invalid_query_result(self):
        """Test with invalid query result format"""
        cursor = MagicMock()
        materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]
        label_column = "label"

        self.connector.run_query = Mock(return_value={"invalid_key": "invalid_value"})

        with self.assertRaises(KeyError):
            self.connector.check_for_classification_data_requirement(
                cursor, materials, label_column, 1
            )

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.MIN_NUM_OF_SAMPLES",
        new=100,
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.CLASSIFIER_MIN_LABEL_PROPORTION",
        new=0.01,
    )
    def test_empty_materials(self):
        """Test with empty materials list"""
        cursor = MagicMock()
        materials = []
        label_column = "label"

        self.connector.run_query = Mock()
        self.connector.run_query.assert_not_called()
        result = self.connector.check_for_classification_data_requirement(
            cursor, materials, label_column, 1
        )
        self.assertFalse(result)  # No materials, so considered sufficient


class TestCheckForRegressionDataRequirement(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = RedshiftConnector("data")

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.MIN_NUM_OF_SAMPLES",
        new=90,
    )
    def test_enough_total_samples(self):
        """Test when there are enough negative samples"""
        cursor = MagicMock()
        materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]

        self.connector.run_query = Mock(return_value=([100],))
        result = self.connector.check_for_regression_data_requirement(cursor, materials)

        self.assertTrue(result)
        self.connector.run_query.assert_called_once_with(
            cursor,
            """SELECT COUNT(*) as count
                FROM feature_table_name""",
            response=True,
        )

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.MIN_NUM_OF_SAMPLES",
        new=90,
    )
    def test_insufficient_total_samples(self):
        """Test when there are not enough negative samples"""
        cursor = MagicMock()
        materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]

        self.connector.run_query = Mock(return_value=([49],))
        result = self.connector.check_for_regression_data_requirement(cursor, materials)

        self.assertFalse(result)
        self.connector.run_query.assert_called_once_with(
            cursor,
            """SELECT COUNT(*) as count
                FROM feature_table_name""",
            response=True,
        )

    def test_invalid_query_result(self):
        """Test with invalid query result format"""
        cursor = MagicMock()
        materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]

        self.connector.run_query = Mock(return_value={"invalid_key": "invalid_value"})

        with self.assertRaises(KeyError):
            self.connector.check_for_regression_data_requirement(cursor, materials)

    @patch(
        "src.predictions.rudderstack_predictions.utils.constants.MIN_NUM_OF_SAMPLES",
        new=100,
    )
    def test_empty_materials(self):
        """Test with empty materials list"""
        cursor = MagicMock()
        materials = []

        self.connector.run_query = Mock()
        self.connector.run_query.assert_not_called()
        result = self.connector.check_for_regression_data_requirement(cursor, materials)
        self.assertFalse(result)  # No materials, so considered sufficient


class TestCheckAndGenerateMoreMaterials(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = RedshiftConnector("data")
        self.materials = [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-20 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-27 00:00:00",
            ),
        ]
        self.session = MagicMock()
        self.whtService = PythonWHT()
        self.whtService.init(
            self.connector, self.session, "siteconfig.yaml", "project_folder"
        )
        trainer_input = build_trainer_config()
        self.trainer = ClassificationTrainer(**trainer_input)
        self.trainer.materialisation_strategy = "auto"
        self.trainer.materialisation_max_no_dates = 2
        self.prediction_horizon_days = 7
        self.trainer.prediction_horizon_days = self.prediction_horizon_days
        self.feature_data_min_date_diff = 0
        self.trainer.feature_data_min_date_diff = self.feature_data_min_date_diff
        self.trainer.materialisation_dates = []
        self.input_models = "model_name"

    @patch("src.predictions.rudderstack_predictions.utils.utils.date_add")
    @patch("src.predictions.rudderstack_predictions.utils.utils.dates_proximity_check")
    @patch("src.predictions.rudderstack_predictions.utils.utils.get_abs_date_diff")
    @patch(
        "src.predictions.rudderstack_predictions.utils.utils.get_feature_package_path"
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.utils.datetime_to_date_string"
    )
    @patch("src.predictions.rudderstack_predictions.wht.rudderPB.RudderPB.run")
    def test_generate_new_materials_auto_strategy(
        self,
        mock_rudderpb_run,
        mock_datetime_to_date_string,
        mock_get_feature_package_path,
        mock_get_abs_date_diff,
        mock_dates_proximity_check,
        mock_date_add,
    ):
        # Mock data
        mock_rudderpb_run.return_value = True
        mock_datetime_to_date_string.side_effect = ["2024-02-20", "2024-02-20"]
        mock_get_feature_package_path.return_value = "feature_package_path"
        new_materials = self.materials + [
            TrainTablesInfo(
                feature_table_name="feature_table_name",
                feature_table_date="2024-02-06 00:00:00",
                label_table_name="label_table_name",
                label_table_date="2024-02-13 00:00:00",
            ),
        ]

        mock_get_material_func = Mock(return_value=new_materials)

        mock_date_add.side_effect = [
            "2024-02-06",
            "2024-02-13",
            "2024-02-06",
            "2024-02-13",
        ]

        # Call the function
        self.trainer.check_min_data_requirement = Mock(side_effect=[False, True])
        result = self.trainer.check_and_generate_more_materials(
            mock_get_material_func,
            materials=self.materials,
            input_models=self.input_models,
            whtService=self.whtService,
            connector=self.connector,
            session=self.session,
        )

        # Assertions
        self.assertEqual(len(result), 2)  # Two materials generated

        # Verify calls to mock functions
        mock_get_feature_package_path.assert_called_once_with(self.input_models)
        mock_datetime_to_date_string.assert_called()  # Called multiple times
        mock_rudderpb_run.assert_called()  # Called twice

        mock_dates_proximity_check.assert_called_once_with(
            "2024-02-06", ["2024-02-20"], self.feature_data_min_date_diff
        )
        mock_date_add.assert_has_calls(
            [
                call("2024-02-20", -1 * self.feature_data_min_date_diff),
                call("2024-02-06", self.prediction_horizon_days),
            ]
        )
        mock_datetime_to_date_string.assert_called_once()
        mock_get_abs_date_diff.assert_called_once()

        # Test early termination due to materialisation failure
        mock_rudderpb_run.side_effect = ValueError

        self.trainer.check_min_data_requirement = Mock(return_value=False)
        result = self.trainer.check_and_generate_more_materials(
            mock_get_material_func,
            materials=self.materials,
            input_models=self.input_models,
            whtService=self.whtService,
            connector=self.connector,
            session=self.session,
        )

        self.assertEqual(len(result), 1)  # Only one material generated

    @patch(
        "src.predictions.rudderstack_predictions.utils.utils.get_feature_package_path"
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.utils.datetime_to_date_string"
    )
    @patch(
        "src.predictions.rudderstack_predictions.utils.utils.generate_new_training_dates"
    )
    @patch("src.predictions.rudderstack_predictions.wht.rudderPB.RudderPB.run")
    def test_generate_new_materials_manual_strategy(
        self,
        mock_rudderpb_run,
        mock_generate_new_training_dates,
        mock_datetime_to_date_string,
        mock_get_feature_package_path,
    ):
        materials_1 = self.materials + [
            TrainTablesInfo(
                feature_table_name="feature_table_name_1",
                feature_table_date="2024-01-01 00:00:00",
                label_table_name="label_table_name_1",
                label_table_date="2024-01-08 00:00:00",
            ),
        ]

        materials_2 = materials_1 + [
            TrainTablesInfo(
                feature_table_name="feature_table_name_2",
                feature_table_date="2024-02-01 00:00:00",
                label_table_name="label_table_name_2",
                label_table_date="2024-02-08 00:00:00",
            ),
        ]

        mock_rudderpb_run.side_effect = [True, True, True, True]
        mock_get_material_func = Mock(side_effect=[materials_1, materials_2])
        mock_get_feature_package_path.return_value = "feature_package_path"

        self.trainer.materialisation_dates = [
            "2024-01-01,2024-01-08",
            "2024-02-01,2024-02-08",
        ]
        self.trainer.check_min_data_requirement = Mock(side_effect=[False, False, True])
        self.trainer.materialisation_strategy = "manual"
        result = self.trainer.check_and_generate_more_materials(
            mock_get_material_func,
            materials=self.materials,
            input_models=self.input_models,
            whtService=self.whtService,
            connector=self.connector,
            session=self.session,
        )

        # Assertions
        self.assertEqual(len(result), 3)  # Three materials generated

        # Verify calls to mock functions
        mock_get_feature_package_path.assert_called_once_with(self.input_models)
        mock_datetime_to_date_string.assert_not_called()  # Not called
        mock_generate_new_training_dates.assert_not_called()  # Not called
        mock_rudderpb_run.assert_called()

        # Test case where data requirement is not met after materialisation
        mock_rudderpb_run.side_effect = ValueError
        self.trainer.check_min_data_requirement = Mock(return_value=False)

        result = self.trainer.check_and_generate_more_materials(
            mock_get_material_func,
            materials=self.materials,
            input_models=self.input_models,
            whtService=self.whtService,
            connector=self.connector,
            session=self.session,
        )

        self.assertEqual(len(result), 1)  # Only one material generated


class TestValidateHistoricalMaterialsHash(unittest.TestCase):
    def setUp(self) -> None:
        self.session_mock = Mock()
        self.connector = RedshiftConnector("data")
        self.material_table = "material_table"
        self.start_date = "2022-01-01"
        self.end_date = "2022-01-31"
        self.features_profiles_model = "model_name"
        self.model_hash = "model_hash"
        self.prediction_horizon_days = 7
        self.site_config_path = "siteconfig.yaml"
        self.project_folder = "project_folder"
        self.input_models = ["model1.yaml", "model2.yaml"]
        self.inputs = ["""select * from material_user_var_736465_0"""]
        self.whtService = PythonWHT()
        self.whtService.init(self.connector, self.session_mock, "", "")
        self.connector.get_tables_by_prefix = Mock(return_value=["material_table_1"])

    # The method is called with valid arguments and all tables exist in the warehouse registry.
    def test_valid_arguments_all_tables_exist(self):
        self.connector.check_table_entry_in_material_registry = Mock(return_value=True)
        result = self.whtService._validate_historical_materials_hash(
            "SELECT * FROM material_table_3", 1, 2
        )
        self.assertTrue(result)

    def test_valid_arguments_some_tables_dont_exist(self):
        self.connector.check_table_entry_in_material_registry = Mock(
            side_effect=[True, False]
        )
        result = self.whtService._validate_historical_materials_hash(
            "SELECT * FROM material_table_3", 1, 2
        )
        self.assertFalse(result)
