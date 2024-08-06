import unittest
from unittest.mock import MagicMock, Mock
from datetime import datetime, timedelta

from src.predictions.profiles_mlcorelib.wht.pyNativeWHT import PyNativeWHT


class TestPyNativeWHT(unittest.TestCase):
    def setUp(self):
        self.whtMaterial = Mock()
        self.pyNativeWHT = PyNativeWHT(self.whtMaterial)
        self.pyNativeWHT.init(None, None, None)

    def test_run(self):
        self.pyNativeWHT.pythonWHT.run = Mock()
        self.pyNativeWHT.run("entity/user/is_churned", "2021-10-10")
        self.pyNativeWHT.pythonWHT.run.assert_called_once_with(
            "entity/user/is_churned", "2021-10-10"
        )

    def test_get_date_range_with_both_none(self):
        creation_ts = datetime.strptime("2024-07-30 15:30:45", "%Y-%m-%d %H:%M:%S")
        prediction_horizon_days = 7
        self.pyNativeWHT.whtMaterial.wht_ctx.time_info = Mock(return_value=(None, None))
        result = self.pyNativeWHT.get_date_range(creation_ts, prediction_horizon_days)
        expected_result = ("2024-07-16", "2024-07-23")
        self.assertEqual(result, expected_result)

    def test_get_date_range_with_only_start_date_none(self):
        creation_ts = datetime.strptime("2024-07-30 15:30:45", "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime("2024-07-29 15:30:45", "%Y-%m-%d %H:%M:%S")
        prediction_horizon_days = 7
        self.pyNativeWHT.whtMaterial.wht_ctx.time_info = Mock(
            return_value=(None, end_date)
        )
        result = self.pyNativeWHT.get_date_range(creation_ts, prediction_horizon_days)
        expected_result = ("2024-07-15", "2024-07-22")
        self.assertEqual(result, expected_result)

    def test_get_date_range_with_less_than_expected_value(self):
        creation_ts = datetime.strptime("2024-07-30 15:30:45", "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime("2024-07-29 15:30:45", "%Y-%m-%d %H:%M:%S")
        begin_time = datetime.strptime("2024-07-20 15:30:45", "%Y-%m-%d %H:%M:%S")
        prediction_horizon_days = 7
        self.pyNativeWHT.whtMaterial.wht_ctx.time_info = Mock(
            return_value=(begin_time, end_date)
        )

        model_name = "prediction_model"
        self.whtMaterial.model.name = Mock(return_value=model_name)

        with self.assertRaises(Exception) as context:
            _ = self.pyNativeWHT.get_date_range(creation_ts, prediction_horizon_days)
        self.assertIn(
            str(context.exception),
            f"begin_time and end_time needs to be atleast {2*prediction_horizon_days} days apart for the predictive feature {self.whtMaterial.model.name()} with prediction_horizon_days: {prediction_horizon_days}",
        )

    def test_get_date_range_with_expected_value(self):
        creation_ts = datetime.strptime("2024-07-30 15:30:45", "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime("2024-07-29 15:30:45", "%Y-%m-%d %H:%M:%S")
        begin_time = datetime.strptime("2024-07-10 15:30:45", "%Y-%m-%d %H:%M:%S")
        prediction_horizon_days = 7
        self.pyNativeWHT.whtMaterial.wht_ctx.time_info = Mock(
            return_value=(begin_time, end_date)
        )
        result = self.pyNativeWHT.get_date_range(creation_ts, prediction_horizon_days)
        expected_result = ("2024-07-15", "2024-07-22")
        self.assertEqual(result, expected_result)

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
            3,
        )
        self.pyNativeWHT.pythonWHT.get_material_names.assert_called_once_with(
            "2021-10-10",
            "2021-10-10",
            "user_var_table",
            "f2345h",
            "7",
            ["entity/user/is_churned"],
            ["material_{model_name}_{hash}_{seq_no}"],
            False,
            3,
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
