
# Generated by CodiumAI
from pandas.core.api import DataFrame as DataFrame
from redshift_connector.cursor import Cursor
from RedshiftConnector import RedshiftConnector
import pandas as pd

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

