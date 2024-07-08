import unittest

from datetime import datetime
from unittest.mock import patch, Mock
from src.predictions.profiles_mlcorelib.wht.pythonWHT import PythonWHT
from src.predictions.profiles_mlcorelib.utils.constants import TrainTablesInfo


class TestGetInputModels(unittest.TestCase):
    def setUp(self) -> None:
        self.pythonWHT = PythonWHT()

    @patch("src.predictions.profiles_mlcorelib.wht.rudderPB.RudderPB.show_models")
    def test_get_input_models(self, mock_rudderpb_show_models):
        original_input_models = [
            "Material_user_var_table1_54ddc22a_383",
            "Material_user_var_table2_54ddc22a_383",
            '''SELECT last_seen FROM "rs_profiles_3"."material_user_var_table_54ddc22a_383"''',
            'select last_seen2 from "rs_profiles_3"."material_user_var_table_54ddc22a_383"',
            "select last_seen2 from 'rs_profiles_3'.'material_user_var_table_54ddc22a_383'",
            'SELECT * FROM "rs_profiles_3"."Material_user_var_table2_54ddc22a_383"',
        ]

        self.pythonWHT.init(
            connector=None,
            site_config_path="site_config",
            project_folder_path="project_folder",
        )

        stdout = """Some text before 
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
                        }
                    }
                    Some text after"""

        # Mocking necessary dependencies
        mock_rudderpb_show_models.return_value = stdout

        # Calling the function under test
        result = self.pythonWHT.get_input_models(
            original_input_models,
        )

        expected = [
                    "inputs/user_var_table1",
                    "inputs/user_var_table2",
                    "user/all/last_seen",
                    "user/all/last_seen2",
                ]
        self.assertEqual(len(result), len(expected))
        self.assertEqual(
            set(result),
            set(expected),
        )


class MockTableRow:
    FEATURE_SEQ_NO = None
    LABEL_SEQ_NO = None
    FEATURE_END_TS = None
    LABEL_END_TS = None


class MockConnector:
    is_valid_table = None


class TestFetchValidHistoricMaterials(unittest.TestCase):
    def setUp(self) -> None:
        self.pythonWHT = PythonWHT()
        self.inputs = [
            "SELECT * FROM Material_user_var_table_54ddc22a_333",
            "SELECT * FROM Material_user_var_table_54ddc22a_383",
        ]

    @patch(
        "src.predictions.profiles_mlcorelib.wht.pythonWHT.PythonWHT._validate_historical_materials_hash"
    )
    def test_all_data_present_and_valid(self, mock_validate_historical_materials_hash):
        # Mock dependencies
        connector = MockConnector()
        connector.is_valid_table = Mock(return_value=True)

        self.pythonWHT.init(
            connector=connector,
            site_config_path="site_config",
            project_folder_path="project_folder",
        )

        mock_validate_historical_materials_hash.return_value = True
        materials = []
        table_row = MockTableRow()
        table_row.FEATURE_SEQ_NO = 10
        table_row.LABEL_SEQ_NO = 20
        table_row.FEATURE_END_TS = datetime.strptime("2023-01-01", "%Y-%m-%d")
        table_row.LABEL_END_TS = datetime.strptime("2023-01-07", "%Y-%m-%d")

        # Call the function
        self.pythonWHT._fetch_valid_historic_materials(
            table_row, "user_var_table", "54ddc22a", self.inputs, materials, False
        )

        # Assertions
        self.assertEqual(len(materials), 1)

    @patch(
        "src.predictions.profiles_mlcorelib.wht.pythonWHT.PythonWHT._validate_historical_materials_hash"
    )
    def test_missing_sequence_number(self, mock_compute_material_name):
        connector = MockConnector()
        connector.is_valid_table = Mock(return_value=True)

        self.pythonWHT.init(
            connector=connector,
            site_config_path="site_config",
            project_folder_path="project_folder",
        )

        materials = []
        table_row = MockTableRow()
        table_row.FEATURE_SEQ_NO = 10
        table_row.LABEL_SEQ_NO = None
        table_row.FEATURE_END_TS = datetime.strptime("2023-01-01", "%Y-%m-%d")
        table_row.LABEL_END_TS = None

        # Call the function
        self.pythonWHT._fetch_valid_historic_materials(
            table_row, "user_var_table", "54ddc22a", self.inputs, materials, True
        )

        # Assertions
        self.assertEqual(len(materials), 1)

        materials = []

        # Call the function
        self.pythonWHT._fetch_valid_historic_materials(
            table_row, "user_var_table", "54ddc22a", self.inputs, materials, False
        )

        # Assertions
        self.assertEqual(len(materials), 0)  # No data appended

        materials = []
        table_row = MockTableRow()  # All None values

        # Call the function
        self.pythonWHT._fetch_valid_historic_materials(
            table_row, "user_var_table", "54ddc22a", self.inputs, materials, False
        )

        # Assertions
        self.assertEqual(len(materials), 0)

        # Call the function
        self.pythonWHT._fetch_valid_historic_materials(
            table_row, "user_var_table", "54ddc22a", self.inputs, materials, True
        )

        # Assertions
        self.assertEqual(len(materials), 0)


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
        self.pythonWHT = PythonWHT()

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
