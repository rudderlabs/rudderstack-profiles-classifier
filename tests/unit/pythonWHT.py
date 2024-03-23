import unittest
from unittest.mock import MagicMock, patch
from src.predictions.rudderstack_predictions.connectors.ConnectorFactory import (
    ConnectorFactory,
)
from src.predictions.rudderstack_predictions.utils import utils
from src.predictions.rudderstack_predictions.wht.pythonWHT import PythonWHT

class TestGetInputModels(unittest.TestCase):
    def setUp(self) -> None:
        self.pythonWHT = PythonWHT()

    @patch("src.predictions.rudderstack_predictions.wht.rudderPB.RudderPB.show_models")
    def test_get_input_models(self, mock_rudderpb_show_models):
        original_input_models = ["Material_user_var_table1_54ddc22a_383", "Material_user_var_table2_54ddc22a_383"]

        self.pythonWHT.init(connector=None,
        session=None,
        site_config_path = "site_config",
        project_folder_path= "project_folder")
        
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
                        }
                    }
                    Some text after"""             

        # Mocking necessary dependencies
        mock_rudderpb_show_models.return_value = stdout

        # Calling the function under test
        result = self.pythonWHT.get_input_models(
            original_input_models,
        )

        # Asserting the result and mock calls
        self.assertEqual(result, ["inputs/user_var_table1", "inputs/user_var_table2"])