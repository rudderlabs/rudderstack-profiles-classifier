
# Generated by CodiumAI
from pandas.core.api import DataFrame as DataFrame
from redshift_connector.cursor import Cursor
from src.connectors.RedshiftConnector import RedshiftConnector
import pandas as pd
from unittest.mock import Mock
from src.constants.constants import TrainTablesInfo
import unittest


    
class TestGetMaterialRegistryTable(unittest.TestCase):

    # Returns a filtered material registry table containing only the successfully materialized data.
    def test_returns_filtered_material_registry_table(self):
        class MockRedshiftConnector(RedshiftConnector):
            def __init__(self, folder_path):
                self.folder_path = folder_path
            def get_table_as_dataframe(self, cursor: Cursor, table_name: str, **kwargs) -> DataFrame:
                material_registry_table = pd.DataFrame.from_dict({"seq_no":[1,2,3,4, 5], "metadata":['{"complete": {"status": 1}}', '{"complete": {"status": 2}}', None, "null", "{}" ]})
                return material_registry_table
        redshift_connector = MockRedshiftConnector(folder_path='data')
        material_registry_table = redshift_connector.get_material_registry_table(
            cursor=None,
            material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict({"seq_no":[2], "metadata":['{"complete": {"status": 2}}' ], "status": [2]})
        self.assertEqual(material_registry_table.values.all(), expected_registry_table.values.all())
    def test_returns_filtered_material_registry_table_empty_resp(self):
        class MockRedshiftConnector(RedshiftConnector):
            def __init__(self, folder_path):
                self.folder_path = folder_path
            def get_table_as_dataframe(self, cursor: Cursor, table_name: str, **kwargs) -> DataFrame:
                material_registry_table = pd.DataFrame.from_dict({"seq_no":[1,2,3,4, 5], "metadata":['{"complete": {"status": 1}}', '{"complete": {"status": 1}}', None, "null", "{}" ]})
                return material_registry_table
        redshift_connector = MockRedshiftConnector(folder_path='data')
        material_registry_table = redshift_connector.get_material_registry_table(
            cursor=None,
            material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict({})
        self.assertEqual(material_registry_table.values.all(), expected_registry_table.values.all())
    def test_runs_on_empty_material_registry_table(self):
        class MockRedshiftConnector(RedshiftConnector):
            def __init__(self, folder_path):
                self.folder_path = folder_path
            def get_table_as_dataframe(self, cursor: Cursor, table_name: str, **kwargs) -> DataFrame:
                material_registry_table = pd.DataFrame.from_dict({"seq_no":[], "metadata":[]})
                return material_registry_table
        redshift_connector = MockRedshiftConnector(folder_path='data')

        # Call the get_material_registry_table method
        material_registry_table = redshift_connector.get_material_registry_table(
            cursor=None,
            material_registry_table_name=None
        )
        expected_registry_table = pd.DataFrame.from_dict({})
        self.assertEqual(material_registry_table.values.all(), expected_registry_table.values.all())

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
        expected_materials = [TrainTablesInfo(feature_table_name="feature_table_name", 
                                              feature_table_date="feature_table_dt", 
                                              label_table_name="label_table_name", 
                                              label_table_date="label_table_dt")]

        # Mock the internal method get_material_names_
        self.connector.get_material_names_ = Mock(return_value=(input_materials, input_training_dates))
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
            self.inputs
        )
        # Assert the result
        self.assertEqual(materials, expected_materials)

    # Materializes feature and label data if no materialized data is found within the specified date range and retrieves material names and training dates
    def test_materializes_data_if_not_found_within_date_range(self):
        # Set up the expected input and output
        input_materials = [("feature_table_name", "label_table_name")]
        input_training_dates = [("feature_table_dt", "label_table_dt")]
        expected_materials = [TrainTablesInfo(feature_table_name="feature_table_name", 
                                        feature_table_date="feature_table_dt", 
                                        label_table_name="label_table_name", 
                                        label_table_date="label_table_dt")]
        # Mock the internal methods get_material_names_ and generate_training_materials
        self.connector.get_material_names_ = Mock(side_effect=[([], []), (input_materials, input_training_dates)])
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
            self.inputs
        )

        # Assert the result
        self.assertEqual(materials, expected_materials)
        self.connector.generate_training_materials.assert_called_once_with(
            self.start_date,
            self.prediction_horizon_days,
            self.output_filename,
            self.site_config_path,
            self.project_folder,
            self.input_models
        )
        
    def test_materializes_data_once_even_if_it_cant_find_right_materials(self):
        # Mock the internal methods get_material_names_ and generate_training_materials
        self.connector.get_material_names_ = Mock(return_value=([], []))
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
                self.inputs
            )
        # Check the exception message
        self.assertIn("Tried to materialise past data but no materialized data found", str(context.exception))

        # Assert generate_training_materials called once
        self.connector.generate_training_materials.assert_called_once_with(
            self.start_date,
            self.prediction_horizon_days,
            self.output_filename,
            self.site_config_path,
            self.project_folder,
            self.input_models
        )