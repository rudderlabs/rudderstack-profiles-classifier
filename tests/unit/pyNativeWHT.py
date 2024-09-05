import unittest
from unittest.mock import Mock
from datetime import datetime

from src.predictions.profiles_mlcorelib.wht.pyNativeWHT import PyNativeWHT


class MockModel:
    def __init__(
        self,
        model_type: str,
        db_object_name: str,
        output_type: str,
        model_ref: str,
        encapsulating_db_object_name: str,
    ) -> None:
        self._model_type = model_type
        self.db_object_name = db_object_name
        self.output_type = output_type
        self._model_ref = model_ref
        self.encapsulating_db_object_name = encapsulating_db_object_name

    def model_type(self):
        return self._model_type

    def model_ref(self):
        return self._model_ref

    def db_object_name_prefix(self):
        return self.db_object_name

    def materialization(self):
        return {"output_type": self.output_type}

    def entity(self):
        return {"IdColumnName": "user_main_id"}

    def encapsulating_model(self):
        return MockModel(
            "entity_var_table",
            self.encapsulating_db_object_name,
            "table",
            "entity/user/var_table",
            None,
        )


class MockMaterial:
    def __init__(
        self,
        model_type: str,
        db_object_name: str,
        output_type: str,
        model_ref: str,
        encapsulating_db_object_name: str,
    ) -> None:
        self.model = MockModel(
            model_type,
            db_object_name,
            output_type,
            model_ref,
            encapsulating_db_object_name,
        )
        self.db_object_name = db_object_name
        self.output_type = output_type
        self.encapsulating_db_object_name = encapsulating_db_object_name

    def name(self):
        return f"Material_{self.db_object_name}_hash_100"

    def get_selector_sql(self):
        if self.output_type == "column":
            return (
                f"SELECT {self.db_object_name} FROM {self.encapsulating_db_object_name}"
            )
        return f"SELECT * FROM {self.db_object_name}"


class MockWhtMaterial:
    def __init__(self) -> None:
        self.model = MockModel(None, None, None, None, None)

    def de_ref(self, input):
        input_map = {
            "entity/user/is_churned": MockMaterial(
                "entity_var_item",
                "is_churned",
                "column",
                "entity/user/is_churned",
                "user_var_table",
            ),
            "entity/user/var_table": MockMaterial(
                "entity_var_table",
                "user_var_table",
                "table",
                "entity/user/var_table",
                None,
            ),
            "models/shopify_user_features": MockMaterial(
                "feature_table",
                "shopify_user_features",
                "table",
                "models/shopify_user_features",
                None,
            ),
            "models/shopify_sql_model": MockMaterial(
                "sql_template",
                "shopify_sql_model",
                "table",
                "models/shopify_sql_model",
                None,
            ),
            "models/shopify_sql_model/var_table": MockMaterial(
                "input_var_table",
                "shopify_sql_model_var_table",
                "table",
                "models/shopify_sql_model/var_table",
                None,
            ),
            "models/shopify_sql_model/var_table/user_main_id": MockMaterial(
                "input_var_item",
                "shopify_sql_model_var_table_user_main_id",
                "column",
                "models/shopify_sql_model/var_table/user_main_id",
                "shopify_sql_model_var_table",
            ),
        }
        return input_map[input]


class TestPyNativeWHT(unittest.TestCase):
    def setUp(self):
        self.whtMaterial = Mock()
        self.pyNativeWHT = PyNativeWHT(self.whtMaterial, None, None)

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
            [
                {
                    "model_ref": "entity/user/is_churned",
                    "table_name": "material_{model_name}_{hash}_{seq_no}",
                }
            ],
            ["is_churned"],
            "user_main_id",
            3,
        )
        self.pyNativeWHT.pythonWHT.get_material_names.assert_called_once_with(
            "2021-10-10",
            "2021-10-10",
            "user_var_table",
            "f2345h",
            "7",
            [
                {
                    "model_ref": "entity/user/is_churned",
                    "table_name": "material_{model_name}_{hash}_{seq_no}",
                }
            ],
            ["is_churned"],
            "user_main_id",
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

    def test_get_latest_seq_no(self):
        result = self.pyNativeWHT.get_latest_seq_no(
            [{"table_name": "MATERIAL_model_hash_123"}]
        )
        self.assertEqual(result, 123)


class TestGetInputs(unittest.TestCase):
    def test_get_inputs(self):
        wht_service = PyNativeWHT(MockWhtMaterial(), None, None)
        inputs = [
            "entity/user/is_churned",
            "entity/user/var_table",
            "models/shopify_user_features",
            "models/shopify_sql_model",
        ]
        result = wht_service.get_inputs(inputs)
        self.assertEqual(
            result,
            [
                {
                    "column_name": "is_churned",
                    "model_ref": "entity/user/is_churned",
                    "model_type": "entity_var_item",
                    "selector_sql": "SELECT is_churned FROM user_var_table",
                    "table_name": "Material_user_var_table_hash_100",
                    "model_name": "is_churned",
                    "model_hash": "hash",
                },
                {
                    "column_name": None,
                    "model_ref": "entity/user/var_table",
                    "model_type": "entity_var_table",
                    "selector_sql": "SELECT * FROM user_var_table",
                    "table_name": "Material_user_var_table_hash_100",
                    "model_name": "user_var_table",
                    "model_hash": "hash",
                },
                {
                    "column_name": None,
                    "model_ref": "models/shopify_user_features",
                    "model_type": "feature_table",
                    "selector_sql": "SELECT * FROM shopify_user_features",
                    "table_name": "Material_shopify_user_features_hash_100",
                    "model_name": "shopify_user_features",
                    "model_hash": "hash",
                },
                {
                    "column_name": None,
                    "model_ref": "models/shopify_sql_model",
                    "model_type": "sql_template",
                    "selector_sql": "SELECT * FROM shopify_sql_model",
                    "table_name": "Material_shopify_sql_model_hash_100",
                    "model_name": "shopify_sql_model",
                    "model_hash": "hash",
                },
            ],
        )
