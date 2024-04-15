from typing import List
import pandas as pd
from ..utils import utils
from ..utils.logger import logger
from ..connectors.Connector import Connector
from ..trainers.MLTrainer import MLTrainer
from ..utils import constants


from pycaret.classification import (
    setup as classification_setup,
    compare_models as classification_compare_models,
    get_config as classification_get_config,
    save_model as classification_save_model,
    pull as classification_results_pull,
    load_model as classification_load_model,
    predict_model as classification_predict_model,
    add_metric as classification_add_metric,
)

from sklearn.metrics import roc_auc_score


class ClassificationTrainer(MLTrainer):
    def __init__(
        self, connector: Connector, session, entity_var_model_name: str, **kwargs
    ):
        super().__init__(**kwargs)
        if self.label_value is None:
            self.label_value = connector.get_default_label_value(
                session,
                entity_var_model_name,
                self.label_column,
                constants.POSITIVE_BOOLEAN_FLAGS,
            )

        self.figure_names = {
            "roc-auc-curve": f"04-test-roc-auc-{self.output_profiles_ml_model}.png",
            "pr-auc-curve": f"03-test-pr-auc-{self.output_profiles_ml_model}.png",
            "lift-chart": f"02-test-lift-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"01-feature-importance-chart-{self.output_profiles_ml_model}.png",
        }
        self.pred_output_df_columns = {
            "label": "prediction_label",
            "score": "prediction_score",
        }
        self.isStratify = True

    def get_name(self):
        return "classification"

    def prepare_label_table(self, connector: Connector, session, label_table_name: str):
        label_table = connector.label_table(
            session,
            label_table_name,
            self.label_column,
            self.entity_column,
            self.label_value,
        )
        distinct_values = connector.get_distinct_values_in_column(
            label_table, self.label_column
        )
        if len(distinct_values) == 1:
            raise ValueError(
                f"Only one value of label column found in label table {label_table_name}. Please check if the label column is correct. Label column: {self.label_column}"
            )
        return label_table

    def train_model(
        self,
        feature_df: pd.DataFrame,
        merged_config: dict,
        model_file: str,
    ):
        custom_metrics = [
            {
                "id": "roc_auc",
                "name": "roc_auc",
                "function": roc_auc_score,
                "greater_is_better": False,
            }
        ]

        return self.train_model_(
            feature_df,
            merged_config,
            model_file,
            classification_setup,
            classification_add_metric,
            custom_metrics,
            classification_compare_models,
            classification_save_model,
            classification_get_config,
        )

    def get_metrics(self, model, fold_param, X_train, y_train) -> dict:
        model_metrics = classification_results_pull().iloc[0].to_dict()
        train_metrics = self.trainer_utils.get_metrics_classifier(
            model, X_train, y_train
        )

        key_mapping = {
            "F1": "f1_score",
            "AUC": "pr_auc",
            "Prec.": "precision",
            "Recall": "recall",
            "roc_auc": "roc_auc",
        }

        # Create a new dictionary with updated keys
        test_metrics = {}
        for old_key, new_key in key_mapping.items():
            test_metrics[new_key] = model_metrics.get(old_key, None)

        test_metrics["users"] = int(1 / fold_param * len(X_train))

        result_dict = {
            "output_model_name": self.output_profiles_ml_model,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "val": test_metrics,
                "prob_th": 0,
            },
            "prob_th": 0,
        }
        return result_dict

    def plot_diagnostics(
        self,
        connector: Connector,
        session,
        model,
        stage_name: str,
        x: pd.DataFrame,
        y: pd.DataFrame,
        label_column: str,
    ) -> None:
        try:
            predictions = classification_predict_model(model, x)["prediction_label"]
            y_true = y.to_numpy().reshape(
                -1,
            )
            y_pred = predictions.to_numpy()

            roc_auc_file = connector.join_file_path(self.figure_names["roc-auc-curve"])
            utils.plot_roc_auc_curve(y_pred, y_true, roc_auc_file)
            connector.save_file(session, roc_auc_file, stage_name, overwrite=True)

            pr_auc_file = connector.join_file_path(self.figure_names["pr-auc-curve"])
            utils.plot_pr_auc_curve(y_pred, y_true, pr_auc_file)
            connector.save_file(session, pr_auc_file, stage_name, overwrite=True)

            lift_chart_file = connector.join_file_path(self.figure_names["lift-chart"])
            utils.plot_lift_chart(y_pred, y_true, lift_chart_file)
            connector.save_file(session, lift_chart_file, stage_name, overwrite=True)
        except Exception as e:
            logger.error(f"Could not generate plots. {e}")
        pass

    def prepare_training_summary(
        self, model_results: dict, model_timestamp: str
    ) -> dict:
        training_summary = {
            "timestamp": model_timestamp,
            "data": {
                "metrics": model_results["metrics"],
                "threshold": model_results["prob_th"],
            },
        }
        return training_summary

    def validate_data(self, connector, feature_table):
        return connector.validate_columns_are_present(
            feature_table, self.label_column
        ) and connector.validate_class_proportions(feature_table, self.label_column)

    def check_min_data_requirement(
        self, connector: Connector, session, materials
    ) -> bool:
        label_column = self.label_column
        return connector.check_for_classification_data_requirement(
            session, materials, label_column, self.label_value
        )

    def load_model(self, model_file: str):
        return classification_load_model(model_file)

    def predict(self, model, test_x: pd.DataFrame):
        return classification_predict_model(model, test_x)
