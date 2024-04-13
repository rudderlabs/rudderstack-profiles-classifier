from typing import List
import pandas as pd
from ..trainers.MLTrainer import MLTrainer
from ..utils import utils
from ..utils.logger import logger
from ..connectors.Connector import Connector

from pycaret.regression import (
    setup as regression_setup,
    compare_models as regression_compare_models,
    get_config as get_regression_config,
    save_model as regression_save_model,
    pull as regression_results_pull,
    load_model as regression_load_model,
    predict_model as regression_predict_model,
    add_metric as regression_add_metric,
)


class RegressionTrainer(MLTrainer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.figure_names = {
            # "regression-lift-chart" : f"04-regression-chart-{self.output_profiles_ml_model}.png",
            "deciles-plot": f"03-deciles-plot-{self.output_profiles_ml_model}.png",
            "residuals-chart": f"02-residuals-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"01-feature-importance-chart-{self.output_profiles_ml_model}.png",
        }
        self.pred_output_df_columns = {
            "score": "prediction_label",
        }
        self.isStratify = False

    def get_name(self):
        return "regression"

    def prepare_data(self, feature_df: pd.DataFrame):
        return self._prepare_data(
            regression_setup,
            get_regression_config,
            feature_df,
        )

    def prepare_label_table(self, connector: Connector, session, label_table_name: str):
        return connector.label_table(
            session,
            label_table_name,
            self.label_column,
            self.entity_column,
            None,
        )

    def train_model(
        self,
        feature_df: pd.DataFrame,
        merged_config: dict,
        model_file: str,
    ):
        custom_metrics = []

        return self.train_model_(
            feature_df,
            merged_config,
            model_file,
            regression_setup,
            regression_add_metric,
            custom_metrics,
            regression_compare_models,
            regression_save_model,
        )

    def plot_diagnostics(
        self,
        connector: Connector,
        session,
        model,
        stage_name: str,
        x: pd.DataFrame,
        y: pd.DataFrame,
        label_column: str,
    ):
        try:
            y_pred = model.predict(x)
            y_true = y.to_numpy()

            residuals_file = connector.join_file_path(
                self.figure_names["residuals-chart"]
            )
            utils.plot_regression_residuals(y_pred, y_true, residuals_file)
            connector.save_file(session, residuals_file, stage_name, overwrite=True)

            deciles_file = connector.join_file_path(self.figure_names["deciles-plot"])
            utils.plot_regression_deciles(y_pred, y_true, deciles_file, label_column)
            connector.save_file(session, deciles_file, stage_name, overwrite=True)

            # For future reference
            # regression_chart_file = connector.join_file_path(self.figure_names['regression-lift-chart'])
            # utils.regression_evaluation_plot(y_pred, y_true, regression_chart_file)
            # connector.save_file(session, regression_chart_file, stage_name, overwrite=True)

        except Exception as e:
            logger.error(f"Could not generate regression plots. {e}")

    def get_metrics(self, model, fold_param, X_train, y_train) -> dict:
        model_metrics = regression_results_pull().iloc[0].to_dict()
        train_metrics = self.trainer_utils.get_metrics_regressor(
            model, X_train, y_train
        )

        key_mapping = {
            "MAE": "mean_absolute_error",
            "MSE": "mean_squared_error",
            "R2": "r2_score",
        }

        # # Create a new dictionary with updated keys
        test_metrics = {}
        for old_key, new_key in key_mapping.items():
            test_metrics[new_key] = model_metrics.get(old_key, None)

        test_metrics["users"] = int(1 / fold_param * len(X_train))

        result_dict = {
            "output_model_name": self.output_profiles_ml_model,
            "metrics": {
                "prob_th": 0,
                "train": train_metrics,
                "test": test_metrics,
                "val": test_metrics,
            },
        }
        return result_dict

    def prepare_training_summary(
        self, model_results: dict, model_timestamp: str
    ) -> dict:
        training_summary = {
            "timestamp": model_timestamp,
            "data": {"metrics": model_results["metrics"]},
        }
        return training_summary

    def validate_data(self, connector, feature_table):
        return connector.validate_columns_are_present(
            feature_table, self.label_column
        ) and connector.validate_label_distinct_values(feature_table, self.label_column)

    def check_min_data_requirement(
        self, connector: Connector, session, materials
    ) -> bool:
        return connector.check_for_regression_data_requirement(session, materials)

    def load_model(self, model_file: str):
        return regression_load_model(model_file)

    def predict(self, model, test_x: pd.DataFrame):
        return regression_predict_model(model, test_x)
