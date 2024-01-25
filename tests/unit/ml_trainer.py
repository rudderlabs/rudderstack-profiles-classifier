import unittest
from MLTrainer import *

def build_trainer_config():
    config = {}
    config['label_value'] = None
    config['label_column'] = None
    config['entity_column'] = None
    config['entity_key'] = None
    config['output_profiles_ml_model'] = None
    config['index_timestamp'] = None
    config['train_start_dt'] = None
    config['train_end_dt'] = None
    config['eligible_users'] = None
    config['prediction_horizon_days'] = None
    config['inputs'] = None
    config['max_row_count'] = None
    config['prep'] = None
    config['recall_to_precision_importance'] = 0.
    return config

class TestClassificationTrainer(unittest.TestCase):

    def test_prepare_training_summary(self):
        config = build_trainer_config()
        trainer = ClassificationTrainer(**config)
        metrics = { "test": {}, "train": {}, "val": {} }
        timestamp = "2023-11-08"
        threshold = 0.62
        result = trainer.prepare_training_summary({"metrics": metrics, "prob_th": threshold}, timestamp)
        self.assertEqual(result, { "data": { "metrics": metrics, "threshold": threshold }, "timestamp": timestamp })

class TestRegressionTrainer(unittest.TestCase):

    def test_prepare_training_summary(self):
        config = build_trainer_config()
        trainer = RegressionTrainer(**config)
        metrics = { "test": {}, "train": {}, "val": {} }
        timestamp = "2023-11-08"
        result = trainer.prepare_training_summary({ "metrics": metrics }, timestamp)
        self.assertEqual(result, { "data": { "metrics": metrics }, "timestamp": timestamp })
