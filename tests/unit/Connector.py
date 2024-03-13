import unittest
from unittest.mock import MagicMock, patch
from src.predictions.rudderstack_predictions.connectors.ConnectorFactory import (
    ConnectorFactory,
)
from src.predictions.rudderstack_predictions.wht.pb import getPB
from src.predictions.rudderstack_predictions.utils import utils


class TestGetInputModels(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = ConnectorFactory.create("snowflake")

    def test_extract_json_from_stdout(self):
        stdout = """Some text before
                    printing models
                    {
                        "key1": "value1",
                        "key2": "value2"
                    }
                    Some text after"""
        expected_json = {"key1": "value1", "key2": "value2"}

        json_data = self.connector.extract_json_from_stdout(stdout)

        self.assertEqual(json_data, expected_json)

    @patch("src.predictions.rudderstack_predictions.utils.utils.get_project_folder")
    @patch("src.predictions.rudderstack_predictions.wht.rudderPB.RudderPB.show_models")
    def test_get_input_models(self, mock_rudderpb_show_models, mock_get_project_folder):
        original_input_models = ["model1", "model2"]
        train_summary_output_file_name = "summary_file"
        project_folder = "project_folder"
        site_config_path = "site_config"

        stdout = """Some text before 
                    dummy entity.var{ }
                    printing models
                    {
                        "base_features/inputs/model1": {
                                "warehouse_name": "user_main_id_inputs_rsTracks_var_table",
                                "model_type": "input_var_item",
                                "output_type": "column",
                                "run_type": "discrete",
                                "sql_type": "multi",
                                "enable_status": "enabled"
                        },
                        "base_features/inputs/model2": {
                                "warehouse_name": "user_main_id_inputs_rsTracks_var_table",
                                "model_type": "input_var_item",
                                "output_type": "column",
                                "run_type": "discrete",
                                "sql_type": "multi",
                                "enable_status": "enabled"
                        }
                    }
                    Some text after"""

        # Mocking necessary dependencies
        mock_get_project_folder.return_value = "project_path"
        mock_rudderpb_show_models.return_value = stdout

        # Calling the function under test
        result = self.connector.get_input_models(
            original_input_models,
            train_summary_output_file_name,
            project_folder,
            site_config_path,
        )

        # Asserting the result and mock calls
        self.assertEqual(result, ["inputs/model1", "inputs/model2"])
