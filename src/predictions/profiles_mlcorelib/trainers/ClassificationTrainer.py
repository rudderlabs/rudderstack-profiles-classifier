from typing import Tuple
import pandas as pd
from ..utils import utils
from ..utils.logger import logger
from ..connectors.Connector import Connector
from ..trainers.MLTrainer import MLTrainer
from ..utils import constants


from pycaret.classification import (
    setup as classification_setup,
    compare_models as classification_compare_models,
    tune_model as classification_tune_model,
    get_config as classification_get_config,
    save_model as classification_save_model,
    load_model as classification_load_model,
    predict_model as classification_predict_model,
    add_metric as classification_add_metric,
)

from sklearn.metrics import (
    average_precision_score,
    precision_recall_fscore_support,
    roc_auc_score,
)


class ClassificationTrainer(MLTrainer):
    def __init__(self, connector: Connector, entity_var_model_name: str, **kwargs):
        super().__init__(**kwargs)
        if self.label_value is None:
            self.label_value = connector.get_default_label_value(
                entity_var_model_name,
                self.label_column,
                constants.POSITIVE_BOOLEAN_FLAGS,
            )

        self.metrics_key_mapping = {
            "F1": "f1_score",
            "AUC": "pr_auc",
            "Prec.": "precision",
            "Recall": "recall",
            "roc_auc": "roc_auc",
        }

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

    def prepare_label_table(self, connector: Connector, label_table_name: str):
        label_table = connector.label_table(
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
        input_col_types: dict,
        merged_config: dict,
        model_file: str,
    ):
        custom_metrics = [
            {
                "id": "roc_auc",
                "name": "roc_auc",
                "function": roc_auc_score,
                "greater_is_better": True,
            }
        ]
        metric_to_optimize = "F1"
        models_to_include = merged_config["model_params"]["models"]["include"][
            "classifiers"
        ]

        return self._train_model(
            feature_df,
            input_col_types,
            merged_config,
            model_file,
            classification_setup,
            classification_add_metric,
            custom_metrics,
            classification_compare_models,
            classification_tune_model,
            classification_save_model,
            classification_get_config,
            metric_to_optimize,
            models_to_include,
        )

    def get_metrics(self, model, X_test, y_test, X_train, y_train, n_folds) -> dict:
        train_metrics = self._evaluate_classifier(model, X_train, y_train)
        test_metrics = self._evaluate_classifier(model, X_test, y_test)
        val_metrics = self._evaluate_classifier(model, X_test, y_test)

        val_metrics["users"] = int(1 / n_folds * len(y_train))
        train_metrics["users"] = len(y_train) - int(1 / n_folds * len(y_train))

        result_dict = {
            "output_model_name": self.output_profiles_ml_model,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "val": val_metrics,
                "prob_th": 0,
            },
            "prob_th": 0,
        }
        return result_dict

    def _get_classification_metrics(
        self,
        y_true: pd.DataFrame,
        y_pred: pd.DataFrame,
        y_pred_proba: pd.DataFrame,
    ) -> dict:
        """Returns classification metrics in form of a dict for the given thresold."""
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true,
            y_pred,
            beta=self.recall_to_precision_importance,
        )
        precision = precision[1]
        recall = recall[1]
        f1 = f1[1]
        roc_auc = roc_auc_score(y_true, y_pred_proba)
        pr_auc = average_precision_score(y_true, y_pred_proba)
        user_count = y_true.shape[0]
        metrics = {
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "roc_auc": roc_auc,
            "pr_auc": pr_auc,
            "users": user_count,
        }
        return metrics

    def _evaluate_classifier(
        self,
        model,
        x,
        y,
    ) -> Tuple:
        preds_df = classification_predict_model(model, x, raw_score=True)[
            ["prediction_label", "prediction_score_1"]
        ]

        preds = preds_df["prediction_label"].to_numpy()
        preds_proba = preds_df["prediction_score_1"].to_numpy()
        y = y.to_numpy()

        train_metrics = self._get_classification_metrics(y, preds, preds_proba)

        return train_metrics

    def get_prev_pred_metrics(self, y_true, y_pred):
        metrics = self._get_classification_metrics(y_true, y_pred, y_pred)
        return metrics

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
            predictions = classification_predict_model(model, x, raw_score=True)[
                "prediction_score_1"
            ]

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
            logger.get().error(f"Could not generate plots. {e}")
        pass

    def prepare_training_summary(
        self, model_results: dict, model_timestamp: str
    ) -> dict:
        training_summary = {
            "timestamp": model_timestamp,
            "task": "classification",
            "data": {
                "model": model_results["model_class_name"],
                "metrics": model_results["metrics"],
                "threshold": model_results["prob_th"],
            },
        }
        return training_summary

    def validate_data(
        self, connector, feature_table, train_table_pairs, min_sample_for_training
    ):
        return (
            connector.validate_columns_are_present(feature_table, self.label_column)
            and connector.validate_class_proportions(
                feature_table, self.label_column, train_table_pairs
            )
            and connector.validate_row_count(
                feature_table, min_sample_for_training, train_table_pairs
            )
        )

    def check_min_data_requirement(self, connector: Connector, materials) -> bool:
        label_column = self.label_column
        return connector.check_for_classification_data_requirement(
            materials,
            label_column,
            self.label_value,
            self.entity_column,
            self.eligible_users,
        )

    def load_model(self, model_file: str):
        return classification_load_model(model_file)

    def predict(self, model, test_x: pd.DataFrame):
        return classification_predict_model(model, test_x)
