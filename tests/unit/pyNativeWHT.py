import unittest
from unittest.mock import MagicMock, Mock

from src.predictions.rudderstack_predictions.wht.pyNativeWHT import PyNativeWHT


class TestPyNativeWHT(unittest.TestCase):
    def setUp(self):
        self.whtMaterial = Mock()
        self.pyNativeWHT = PyNativeWHT(self.whtMaterial)
        self.pyNativeWHT.init(None, None, None, None)

    def test_run(self):
        self.pyNativeWHT.pythonWHT.run = Mock()
        self.pyNativeWHT.run("entity/user/is_churned", "2021-10-10")
        self.pyNativeWHT.pythonWHT.run.assert_called_once_with(
            "entity/user/is_churned", "2021-10-10"
        )

    def test_get_material_names(self):
        self.pyNativeWHT.pythonWHT.get_material_names = Mock()
        self.pyNativeWHT.get_material_names(
            "2021-10-10",
            "2021-10-10",
            "user_var_table",
            "f2345h",
            "7",
            ["entity/user/is_churned"],
            ["material_{model_name}_{hash}_{seq_no}"],
        )
        self.pyNativeWHT.pythonWHT.get_material_names.assert_called_once_with(
            "2021-10-10",
            "2021-10-10",
            "user_var_table",
            "f2345h",
            "7",
            ["entity/user/is_churned"],
            ["material_{model_name}_{hash}_{seq_no}"],
        )

    def test_get_latest_entity_var_table(self):
        self.pyNativeWHT.pythonWHT.get_model_creation_ts = Mock(
            return_value="2021-10-10"
        )
        material_mock = Mock()
        material_mock.name = Mock(return_value="material_user_var_table_f2345h_1")
        self.pyNativeWHT.whtMaterial.de_ref = Mock(return_value=material_mock)
        result = self.pyNativeWHT.get_latest_entity_var_table("user")
        self.assertEqual(result, ("f2345h", "user_var_table", "2021-10-10"))
        self.pyNativeWHT.whtMaterial.de_ref.assert_called_once_with(
            "entity/user/var_table"
        )
        self.pyNativeWHT.pythonWHT.get_model_creation_ts.assert_called_once_with(
            "f2345h", "user"
        )
