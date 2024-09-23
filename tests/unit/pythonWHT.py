import unittest

from datetime import datetime
from unittest.mock import patch, Mock
from src.predictions.profiles_mlcorelib.wht.pythonWHT import PythonWHT
from src.predictions.profiles_mlcorelib.utils.constants import TrainTablesInfo
from src.predictions.profiles_mlcorelib.connectors.RedshiftConnector import (
    RedshiftConnector,
)


class RedshiftConnectorV2(RedshiftConnector):
    def __init__(self, folder_path):
        super().__init__({}, folder_path)

    def build_session(self, creds):
        self.schema = creds.get("schema", None)
        pass


class MockTableRow:
    FEATURE_SEQ_NO = None
    LABEL_SEQ_NO = None
    FEATURE_END_TS = None
    LABEL_END_TS = None


class MockConnector:
    is_valid_table = None


# class TestFetchValidHistoricMaterials(unittest.TestCase):
#     def setUp(self) -> None:
#         self.pythonWHT = PythonWHT("site_config", "project_folder")
#         connector = MockConnector()
#         connector.is_valid_table = Mock(return_value=True)
#         self.pythonWHT.set_connector(connector)
#         self.pythonWHT.connector.feature_table_name = (
#             "material_model_name_hash_seq_feature_table"
#         )
#         self.pythonWHT.connector.join_input_tables = Mock(return_value=None)
#         self.inputs = []
#         self.input_columns = ["COL1", "COL2", "COL3"]
#         self.entity_column = "user_main_id"

#     @patch(
#         "src.predictions.profiles_mlcorelib.wht.pythonWHT.PythonWHT._validate_historical_materials_hash"
#     )
#     def test_all_data_present_and_valid(self, mock_validate_historical_materials_hash):
#         # Mock dependencies

#         mock_validate_historical_materials_hash.return_value = True
#         materials = []
#         table_row = MockTableRow()
#         table_row.FEATURE_SEQ_NO = 10
#         table_row.LABEL_SEQ_NO = 20
#         table_row.FEATURE_END_TS = datetime.strptime("2023-01-01", "%Y-%m-%d")
#         table_row.LABEL_END_TS = datetime.strptime("2023-01-07", "%Y-%m-%d")

#         # Call the function
#         self.pythonWHT._fetch_valid_historic_materials(
#             table_row,
#             self.inputs,
#             self.input_columns,
#             self.entity_column,
#             materials,
#             False,
#         )

#         # Assertions
#         self.assertEqual(len(materials), 1)

# @patch(
#     "src.predictions.profiles_mlcorelib.wht.pythonWHT.PythonWHT._validate_historical_materials_hash"
# )
# def test_missing_sequence_number(self, mock_compute_material_name):

#     materials = []
#     table_row = MockTableRow()
#     table_row.FEATURE_SEQ_NO = 10
#     table_row.LABEL_SEQ_NO = None
#     table_row.FEATURE_END_TS = datetime.strptime("2023-01-01", "%Y-%m-%d")
#     table_row.LABEL_END_TS = None

#     # Call the function
#     self.pythonWHT._fetch_valid_historic_materials(
#         table_row,
#         self.inputs,
#         self.input_columns,
#         self.entity_column,
#         materials,
#         True,
#     )

#     # Assertions
#     self.assertEqual(len(materials), 1)

#     materials = []

#     # Call the function
#     self.pythonWHT._fetch_valid_historic_materials(
#         table_row,
#         self.inputs,
#         self.input_columns,
#         self.entity_column,
#         materials,
#         False,
#     )

#     # Assertions
#     self.assertEqual(len(materials), 0)  # No data appended

#     materials = []
#     table_row = MockTableRow()  # All None values

#     # Call the function
#     self.pythonWHT._fetch_valid_historic_materials(
#         table_row,
#         self.inputs,
#         self.input_columns,
#         self.entity_column,
#         materials,
#         False,
#     )

#     # Assertions
#     self.assertEqual(len(materials), 0)

#     # Call the function
#     self.pythonWHT._fetch_valid_historic_materials(
#         table_row,
#         self.inputs,
#         self.input_columns,
#         self.entity_column,
#         materials,
#         True,
#     )

#     # Assertions
#     self.assertEqual(len(materials), 0)


class TestGetPastMaterialsWithValidDateRange(unittest.TestCase):
    def setUp(self):
        self.materials = [
            TrainTablesInfo(
                "feature_table_1", "2024-05-16", "label_table_1", "2024-05-23"
            ),
            TrainTablesInfo("feature_table_2", "None", "label_table_2", "2024-05-24"),
            TrainTablesInfo("feature_table_3", "2024-05-19", "label_table_3", "None"),
            TrainTablesInfo("feature_table_4", "None", "label_table_4", "None"),
        ]
        self.pythonWHT = PythonWHT("site_config", "project_folder")

    def test_valid_materials(self):
        valid_materials = self.pythonWHT.get_past_materials_with_valid_date_range(
            self.materials, 7, 3
        )
        self.assertEqual(len(valid_materials), 2)
        self.assertEqual(valid_materials[0].feature_table_name, "feature_table_1")
        self.assertEqual(valid_materials[1].feature_table_name, "feature_table_3")

    def test_empty_materials(self):
        valid_materials = self.pythonWHT.get_past_materials_with_valid_date_range(
            [], 1, 1
        )
        self.assertEqual(len(valid_materials), 0)


class TestGetLatestSeqNo(unittest.TestCase):
    def test_get_latest_seq_no(self):
        result = PythonWHT("site_config", "project_folder").get_latest_seq_no(
            [{"table_name": "material_user_var_table_123"}]
        )
        self.assertEqual(result, 123)


class TestGetInputs(unittest.TestCase):
    @patch("src.predictions.profiles_mlcorelib.wht.rudderPB.RudderPB.show_models")
    def test_get_inputs_from_stdout(self, mock_rudderpb_show_models):
        selector_sqls = [
            '''SELECT last_seen FROM "schema"."material_user_var_table_54ddc22a_383"''',
            """SELECT last_seen FROM `schema`.`material_user_var_table_54ddc22a_383`""",
            '''SELECT last_seen FROM "material_user_var_table_54ddc22a_383"''',
            '''SELECT last_seen2 FROM "schema"."material_user_var_table_54ddc22a_383"''',
            """SELECT * FROM schema.MATERIAL_FEATURE_TABLE_MODEL1_45223ds1_384""",
            """SELECT * FROM schema.MATERIAL_FEATURE_TABLE_MODEL2_45223ds1_384""",
        ]
        mock_rudderpb_show_models.return_value = """Some text before 
            dummy entity.var{ }
            printing models
            {
                "base_features/inputs/user_var_table1": {
                        "warehouse_name": "user_main_id_inputs_rsTracks_var_table",
                        "model_type": "input_var_item",
                        "output_type": "column",
                        "run_type": "discrete",
                        "sql_type": "multi",
                        "enable_status": "enabled"
                },
                "base_features/inputs/user_var_table2": {
                        "warehouse_name": "user_main_id_inputs_rsTracks_var_table",
                        "model_type": "input_var_item",
                        "output_type": "column",
                        "run_type": "discrete",
                        "sql_type": "multi",
                        "enable_status": "enabled"
                },
                "project/user/all/last_seen": {
                        "model_type": "entity_var_item"
                },
                "project/user/all/last_seen2": {
                        "model_type": "entity_var_item"
                },
                "base_features/models/feature_table_model1": {
                        "model_type": "feature_table_model"
                },
                "base_features/models/feature_table_model2": {
                        "model_type": "feature_table_model"
                }
            }
            Some text after"""
        wht = PythonWHT("site_config_path", "project_folder")
        result = wht.get_inputs(selector_sqls, False)
        expected_result = [
            {
                "selector_sql": '''SELECT last_seen FROM "schema"."material_user_var_table_54ddc22a_383"''',
                "table_name": "material_user_var_table_54ddc22a_383",
                "model_name": "last_seen",
                "column_name": "last_seen",
                "model_ref": "user/all/last_seen",
                "model_type": "entity_var_item",
                "model_hash": None,
                "encapsulating_model_name": "user_var_table",
                "encapsulating_model_hash": "54ddc22a",
            },
            {
                "selector_sql": """SELECT last_seen FROM `schema`.`material_user_var_table_54ddc22a_383`""",
                "table_name": "material_user_var_table_54ddc22a_383",
                "model_name": "last_seen",
                "column_name": "last_seen",
                "model_ref": "user/all/last_seen",
                "model_type": "entity_var_item",
                "model_hash": None,
                "encapsulating_model_name": "user_var_table",
                "encapsulating_model_hash": "54ddc22a",
            },
            {
                "selector_sql": '''SELECT last_seen FROM "material_user_var_table_54ddc22a_383"''',
                "table_name": "material_user_var_table_54ddc22a_383",
                "model_name": "last_seen",
                "column_name": "last_seen",
                "model_ref": "user/all/last_seen",
                "model_type": "entity_var_item",
                "model_hash": None,
                "encapsulating_model_name": "user_var_table",
                "encapsulating_model_hash": "54ddc22a",
            },
            {
                "selector_sql": '''SELECT last_seen2 FROM "schema"."material_user_var_table_54ddc22a_383"''',
                "table_name": "material_user_var_table_54ddc22a_383",
                "model_name": "last_seen2",
                "column_name": "last_seen2",
                "model_ref": "user/all/last_seen2",
                "model_type": "entity_var_item",
                "model_hash": None,
                "encapsulating_model_name": "user_var_table",
                "encapsulating_model_hash": "54ddc22a",
            },
            {
                "selector_sql": """SELECT * FROM schema.MATERIAL_FEATURE_TABLE_MODEL1_45223ds1_384""",
                "table_name": "MATERIAL_FEATURE_TABLE_MODEL1_45223ds1_384",
                "model_name": "feature_table_model1",
                "column_name": None,
                "model_ref": "models/feature_table_model1",
                "model_type": "feature_table_model",
                "model_hash": "45223ds1",
                "encapsulating_model_name": None,
                "encapsulating_model_hash": None,
            },
            {
                "selector_sql": """SELECT * FROM schema.MATERIAL_FEATURE_TABLE_MODEL2_45223ds1_384""",
                "table_name": "MATERIAL_FEATURE_TABLE_MODEL2_45223ds1_384",
                "model_name": "feature_table_model2",
                "column_name": None,
                "model_ref": "models/feature_table_model2",
                "model_type": "feature_table_model",
                "model_hash": "45223ds1",
                "encapsulating_model_name": None,
                "encapsulating_model_hash": None,
            },
        ]
        self.assertEqual(result, expected_result)

    @patch("src.predictions.profiles_mlcorelib.wht.rudderPB.RudderPB.show_models")
    def test_get_inputs_from_stderr(self, mock_rudderpb_show_models):
        selector_sqls = [
            '''SELECT last_seen FROM "schema"."material_user_var_table_54ddc22a_383"''',
        ]
        mock_rudderpb_show_models.return_value = """Some text before 
            dummy entity.var
            {
                "project/user/all/last_seen": {
                        "model_type": "entity_var_item"
                }
            }
            Some text after"""
        wht = PythonWHT("site_config_path", "project_folder")
        result = wht.get_inputs(selector_sqls, False)
        expected_result = [
            {
                "selector_sql": '''SELECT last_seen FROM "schema"."material_user_var_table_54ddc22a_383"''',
                "table_name": "material_user_var_table_54ddc22a_383",
                "model_name": "last_seen",
                "column_name": "last_seen",
                "model_ref": "user/all/last_seen",
                "model_type": "entity_var_item",
                "model_hash": None,
                "encapsulating_model_name": "user_var_table",
                "encapsulating_model_hash": "54ddc22a",
            },
        ]
        self.assertEqual(result, expected_result)
