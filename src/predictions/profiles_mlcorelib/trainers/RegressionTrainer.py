import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from ..trainers.MLTrainer import MLTrainer
from ..utils import utils
from ..utils.logger import logger
from ..connectors.Connector import Connector

from pycaret.regression import (
    setup as regression_setup,
    compare_models as regression_compare_models,
    tune_model as regression_tune_model,
    get_config as regression_get_config,
    save_model as regression_save_model,
    load_model as regression_load_model,
    predict_model as regression_predict_model,
    add_metric as regression_add_metric,
)


class RegressionTrainer(MLTrainer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.metrics_key_mapping = {
            "MAE": "mean_absolute_error",
            "MSE": "mean_squared_error",
            "R2": "r2_score",
        }
        self.figure_names = {
            # "regression-lift-chart" : f"04-regression-chart-{self.output_profiles_ml_model}.png",
            "deciles-plot": f"03-deciles-plot-{self.output_profiles_ml_model}.png",
            "residuals-chart": f"02-residuals-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"01-feature-importance-chart-{self.output_profiles_ml_model}.png",
        }
        self.pred_output_df_columns = {
            "score": "prediction_label",
        }
        self.evalution_metrics_map_regressor = {
            metric.__name__: metric
            for metric in [mean_absolute_error, mean_squared_error, r2_score]
        }
        self.isStratify = False

    def get_name(self):
        return "regression"

    def prepare_label_table(self, connector: Connector, label_table_name: str):
        return connector.label_table(
            label_table_name,
            self.label_column,
            self.entity_column,
            None,
        )

    def train_model(
        self,
        feature_df: pd.DataFrame,
        input_col_types: dict,
        merged_config: dict,
        model_file: str,
    ):
        custom_metrics = []
        metric_to_optimize = "R2"
        models_to_include = merged_config["model_params"]["models"]["include"][
            "regressors"
        ]

        return self._train_model(
            feature_df,
            input_col_types,
            merged_config,
            model_file,
            regression_setup,
            regression_add_metric,
            custom_metrics,
            regression_compare_models,
            regression_tune_model,
            regression_save_model,
            regression_get_config,
            metric_to_optimize,
            models_to_include,
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
            predictions = regression_predict_model(model, x)["prediction_label"]
            y_true = y.to_numpy().reshape(
                -1,
            )
            y_pred = predictions.to_numpy()

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
            logger.get().error(f"Could not generate regression plots. {e}")

    def get_metrics(self, model, X_test, y_test, X_train, y_train, n_folds) -> dict:
        train_metrics = self._evaluate_regressor(model, X_train, y_train)
        test_metrics = self._evaluate_regressor(model, X_test, y_test)
        val_metrics = self._evaluate_regressor(model, X_test, y_test)

        val_metrics["users"] = int(1 / n_folds * len(y_train))
        train_metrics["users"] = len(y_train) - val_metrics["users"]

        result_dict = {
            "output_model_name": self.output_profiles_ml_model,
            "metrics": {
                "prob_th": 0,
                "train": train_metrics,
                "test": test_metrics,
                "val": val_metrics,
            },
        }
        return result_dict

    def _get_regression_metrics(self, y_true, y_pred):
        metrics = {}
        for metric_name, metric_func in self.evalution_metrics_map_regressor.items():
            metrics[metric_name] = float(metric_func(y_true, y_pred))
        return metrics

    def _evaluate_regressor(
        self,
        model,
        x,
        y,
    ):
        preds = regression_predict_model(model, x)["prediction_label"].to_numpy()
        return self._get_regression_metrics(y, preds)

    def get_prev_pred_metrics(self, y_true, y_pred):
        metrics = self._get_regression_metrics(y_true, y_pred)
        return metrics

    def prepare_training_summary(
        self, model_results: dict, model_timestamp: str
    ) -> dict:
        training_summary = {
            "timestamp": model_timestamp,
            "task": "regression",
            "data": {
                "model": model_results["model_class_name"],
                "metrics": model_results["metrics"],
            },
        }
        return training_summary

    def validate_data(
        self, connector, feature_table, train_table_pairs, min_sample_for_training
    ):
        return (
            connector.validate_columns_are_present(feature_table, self.label_column)
            and connector.validate_label_distinct_values(
                feature_table, self.label_column, train_table_pairs
            )
            and connector.validate_row_count(
                feature_table,
                min_sample_for_training,
                train_table_pairs,
            )
        )

    def check_min_data_requirement(self, connector: Connector, materials) -> bool:
        return connector.check_for_regression_data_requirement(
            materials, self.eligible_users
        )

    def load_model(self, model_file: str):
        return regression_load_model(model_file)

    def predict(self, model, test_x: pd.DataFrame):
        return regression_predict_model(model, test_x)
