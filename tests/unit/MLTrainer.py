import unittest
from unittest.mock import Mock, patch
from src.trainers.MLTrainer import *


def build_trainer_config():
    config = {"data": {}, "preprocessing": {}, "outputs": {}}

    config["data"]["label_value"] = None
    config["data"]["label_column"] = None
    config["data"]["entity_column"] = None
    config["data"]["entity_key"] = None
    config["data"]["output_profiles_ml_model"] = None
    config["data"]["index_timestamp"] = None
    config["data"]["train_start_dt"] = None
    config["data"]["train_end_dt"] = None
    config["data"]["eligible_users"] = None
    config["data"]["prediction_horizon_days"] = None
    config["data"]["inputs"] = None
    config["data"]["max_row_count"] = None
    config["data"]["prep"] = None
    config["data"]["recall_to_precision_importance"] = 0.0

    config["preprocessing"]["timestamp_columns"] = None
    config["preprocessing"]["ignore_features"] = None
    config["preprocessing"]["numeric_pipeline"] = None
    config["preprocessing"]["categorical_pipeline"] = None
    config["preprocessing"]["feature_selectors"] = None
    config["preprocessing"]["train_size"] = None
    config["preprocessing"]["test_size"] = None
    config["preprocessing"]["val_size"] = None

    config["outputs"]["column_names"] = None
    config["outputs"]["feature_meta_data"] = None

    return config


class TestClassificationTrainer(unittest.TestCase):
    def test_prepare_training_summary(self):
        config = build_trainer_config()
        trainer = ClassificationTrainer(**config)
        metrics = {"test": {}, "train": {}, "val": {}}
        timestamp = "2023-11-08"
        threshold = 0.62
        result = trainer.prepare_training_summary(
            {"metrics": metrics, "prob_th": threshold}, timestamp
        )
        self.assertEqual(
            result,
            {
                "data": {"metrics": metrics, "threshold": threshold},
                "timestamp": timestamp,
            },
        )

    def test_validate_data(self):
        config = build_trainer_config()
        trainer = ClassificationTrainer(**config)
        mock_connector = Mock()
        mock_connector.validate_columns_are_present = Mock(return_value=True)
        mock_connector.validate_class_proportions = Mock(return_value=True)
        mock_connector.validate_label_distinct_values = Mock(return_value=False)
        self.assertTrue(trainer.validate_data(mock_connector, None))
        mock_connector.validate_class_proportions = Mock(return_value=False)
        self.assertFalse(trainer.validate_data(mock_connector, None))
        mock_connector.validate_columns_are_present = Mock(return_value=False)
        mock_connector.validate_class_proportions = Mock(return_value=True)
        self.assertFalse(trainer.validate_data(mock_connector, None))
        mock_connector.validate_columns_are_present = Mock(return_value=False)
        mock_connector.validate_class_proportions = Mock(return_value=False)
        self.assertFalse(trainer.validate_data(mock_connector, None))

    def test_validate_data_raises_exception_on_failure(self):
        config = build_trainer_config()
        trainer = ClassificationTrainer(**config)
        mock_connector = Mock()
        mock_connector.validate_columns_are_present.side_effect = Exception(
            "Raise exception"
        )
        mock_connector.validate_class_proportions = Mock(return_value=True)
        with self.assertRaises(Exception) as context:
            trainer.validate_data(mock_connector, None)
        self.assertIn(
            "Raise exception",
            str(context.exception),
            [],
        )
        mock_connector.validate_columns_are_present.side_effect = Exception(
            "Raise exception"
        )
        mock_connector.validate_class_proportions = Mock(return_value=False)
        with self.assertRaises(Exception) as context:
            trainer.validate_data(mock_connector, None)
        self.assertIn(
            "Raise exception",
            str(context.exception),
            [],
        )
        mock_connector.validate_columns_are_present.side_effect = Exception(
            "Raise exception"
        )
        mock_connector.validate_class_proportions.side_effect = Exception(
            "Raise exception"
        )
        with self.assertRaises(Exception) as context:
            trainer.validate_data(mock_connector, None)
        self.assertIn(
            "Raise exception",
            str(context.exception),
            [],
        )


class TestRegressionTrainer(unittest.TestCase):
    def test_prepare_training_summary(self):
        config = build_trainer_config()
        trainer = RegressionTrainer(**config)
        metrics = {"test": {}, "train": {}, "val": {}}
        timestamp = "2023-11-08"
        result = trainer.prepare_training_summary({"metrics": metrics}, timestamp)
        self.assertEqual(result, {"data": {"metrics": metrics}, "timestamp": timestamp})

    def test_validate_data(self):
        config = build_trainer_config()
        trainer = RegressionTrainer(**config)
        mock_connector = Mock()
        mock_connector.validate_columns_are_present = Mock(return_value=True)
        mock_connector.validate_label_distinct_values = Mock(return_value=True)
        mock_connector.validate_class_proportions = Mock(return_value=False)
        self.assertTrue(trainer.validate_data(mock_connector, None))
        mock_connector.validate_label_distinct_values = Mock(return_value=False)
        self.assertFalse(trainer.validate_data(mock_connector, None))
        mock_connector.validate_columns_are_present = Mock(return_value=False)
        mock_connector.validate_label_distinct_values = Mock(return_value=True)
        self.assertFalse(trainer.validate_data(mock_connector, None))
        mock_connector.validate_columns_are_present = Mock(return_value=False)
        mock_connector.validate_label_distinct_values = Mock(return_value=False)
        self.assertFalse(trainer.validate_data(mock_connector, None))

    def test_validate_data_raises_exception_on_failure(self):
        config = build_trainer_config()
        trainer = RegressionTrainer(**config)
        mock_connector = Mock()
        mock_connector.validate_columns_are_present.side_effect = Exception(
            "Raise exception"
        )
        mock_connector.validate_label_distinct_values = Mock(return_value=True)
        with self.assertRaises(Exception) as context:
            trainer.validate_data(mock_connector, None)
        self.assertIn(
            "Raise exception",
            str(context.exception),
            [],
        )
        mock_connector.validate_columns_are_present.side_effect = Exception(
            "Raise exception"
        )
        mock_connector.validate_label_distinct_values = Mock(return_value=False)
        with self.assertRaises(Exception) as context:
            trainer.validate_data(mock_connector, None)
        self.assertIn(
            "Raise exception",
            str(context.exception),
            [],
        )
        mock_connector.validate_columns_are_present.side_effect = Exception(
            "Raise exception"
        )
        mock_connector.validate_label_distinct_values.side_effect = Exception(
            "Raise exception"
        )
        with self.assertRaises(Exception) as context:
            trainer.validate_data(mock_connector, None)
        self.assertIn(
            "Raise exception",
            str(context.exception),
            [],
        )
