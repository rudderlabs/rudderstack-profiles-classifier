import unittest
from MLTrainer import *


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


class TestRegressionTrainer(unittest.TestCase):
    def test_prepare_training_summary(self):
        config = build_trainer_config()
        trainer = RegressionTrainer(**config)
        metrics = {"test": {}, "train": {}, "val": {}}
        timestamp = "2023-11-08"
        result = trainer.prepare_training_summary({"metrics": metrics}, timestamp)
        self.assertEqual(result, {"data": {"metrics": metrics}, "timestamp": timestamp})
