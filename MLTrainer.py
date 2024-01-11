import joblib
import time
import json
import numpy as np
import pandas as pd
import snowflake.snowpark

from logger import logger
from copy import deepcopy
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Tuple, List, Union, Dict
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier, XGBRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (
    precision_recall_fscore_support,
    average_precision_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)

import utils
import constants
from Connector import Connector

trainer_utils = utils.TrainerUtils()


@dataclass
class MLTrainer(ABC):
    def __init__(
        self,
        label_value: int,
        label_column: str,
        entity_column: str,
        entity_key: str,
        package_name: str,
        features_profiles_model: str,
        output_profiles_ml_model: str,
        index_timestamp: str,
        eligible_users: str,
        train_start_dt: str,
        train_end_dt: str,
        prediction_horizon_days: int,
        inputs: List[str],
        max_row_count: int,
        prep: utils.PreprocessorConfig,
    ):
        self.label_value = label_value
        self.label_column = label_column
        self.entity_column = entity_column
        self.entity_key = entity_key

        # setting default value for entity_key
        if self.entity_key is None:
            self.entity_key = "user"

        self.package_name = package_name
        self.features_profiles_model = features_profiles_model
        self.output_profiles_ml_model = output_profiles_ml_model
        self.index_timestamp = index_timestamp
        self.eligible_users = eligible_users
        self.train_start_dt = train_start_dt
        self.train_end_dt = train_end_dt
        self.prediction_horizon_days = prediction_horizon_days
        self.inputs = inputs
        self.max_row_count = max_row_count
        self.prep = prep
        self.isStratify = None
        del package_name # Retained this in the class signature for backward compatibility. Not using it anywhere else, hence deleting.

    hyperopts_expressions_map = {
        exp.__name__: exp for exp in [hp.choice, hp.quniform, hp.uniform, hp.loguniform]
    }

    def get_preprocessing_pipeline(
        self,
        numeric_columns: List[str],
        categorical_columns: List[str],
        numerical_pipeline_config: List[str],
        categorical_pipeline_config: List[str],
    ):
        """Returns a preprocessing pipeline for given numeric and categorical columns and pipeline config

        Args:
            numeric_columns (list): name of the columns that are numeric in nature
            categorical_columns (list): name of the columns that are categorical in nature
            numerical_pipeline_config (list): configs for numeric pipeline from model_configs file
            categorical_pipeline_config (list): configs for categorical pipeline from model_configs file

        Raises:
            ValueError: If num_params_name is invalid for numeric pipeline
            ValueError: If cat_params_name is invalid for catagorical pipeline

        Returns:
            _type_: preprocessing pipeline
        """
        numerical_pipeline_config_ = deepcopy(numerical_pipeline_config)
        categorical_pipeline_config_ = deepcopy(categorical_pipeline_config)
        for numerical_params in numerical_pipeline_config_:
            num_params_name = numerical_params.pop("name")
            if num_params_name == "SimpleImputer":
                missing_values = numerical_params.get("missing_values")
                if missing_values == "np.nan":
                    numerical_params["missing_values"] = np.nan
                num_imputer_params = numerical_params
            else:
                error_message = (
                    f"Invalid num_params_name: {num_params_name} for numeric pipeline."
                )
                logger.error(error_message)
                raise ValueError(error_message)
        num_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(**num_imputer_params)),
            ]
        )

        pipeline_params_ = dict()
        for categorical_params in categorical_pipeline_config_:
            cat_params_name = categorical_params.pop("name")
            pipeline_params_[cat_params_name] = categorical_params
            try:
                assert cat_params_name in ["SimpleImputer", "OneHotEncoder"]
            except AssertionError:
                error_message = f"Invalid cat_params_name: {cat_params_name} for categorical pipeline."
                logger.error(error_message)
                raise ValueError(error_message)

        cat_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(**pipeline_params_["SimpleImputer"])),
                ("encoder", OneHotEncoder(**pipeline_params_["OneHotEncoder"])),
            ]
        )

        preprocessor = ColumnTransformer(
            transformers=[
                ("num", num_pipeline, numeric_columns),
                ("cat", cat_pipeline, categorical_columns),
            ]
        )
        return preprocessor

    def get_model_pipeline(self, preprocessor, clf):
        pipe = Pipeline([("preprocessor", preprocessor), ("model", clf)])
        return pipe

    def generate_hyperparameter_space(self, hyperopts: List[dict]) -> dict:
        """Returns a dict of hyper-parameters expression map

        Args:
            hyperopts (List[dict]): list of all the hyper-parameter that are needed to be optimized

        Returns:
            dict: hyper-parameters expression map
        """
        space = {}
        for expression in hyperopts:
            expression_ = expression.copy()
            exp_type = expression_.pop("type")
            name = expression_.pop("name")

            # Handle expression for explicit choices and
            # implicit choices using "low", "high" and optinal "step" values
            if exp_type == "choice":
                options = expression_["options"]
                if not isinstance(options, list):
                    expression_["options"] = list(
                        range(options["low"], options["high"], options.get("step", 1))
                    )

            space[name] = self.hyperopts_expressions_map[f"hp_{exp_type}"](
                name, **expression_
            )
        return space

    def set_end_ts(self, end_ts: str):
        self.end_ts = end_ts

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def select_best_model(self, models, train_x, train_y, val_x, val_y, models_map):
        pass

    @abstractmethod
    def prepare_label_table(self, connector: Connector, session, label_table_name: str):
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def get_metrics(
        self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config
    ):
        pass

    @abstractmethod
    def prepare_training_summary(
        self, model_results: dict, model_timestamp: str
    ) -> dict:
        pass

    def train_model(
        self,
        feature_df: pd.DataFrame,
        categorical_columns: List[str],
        numeric_columns: List[str],
        merged_config: dict,
        model_file: str,
    ):
        """Creates and saves the trained model pipeline after performing preprocessing and classification
        and returns the various variables required for further processing by training procesudres/functions.

        Args:

            feature_df (pd.DataFrame): dataframe containing all the features and labels
            categorical_columns (List[str]): list of categorical columns in the feature_df
            numeric_columns (List[str]): list of numeric columns in the feature_df
            merged_config (dict): configs generated by merging configs from profiles.yaml and model_configs.yaml file
            model_file (str): path to the file where the model is to be saved

        Returns:
            train_x (pd.DataFrame): dataframe containing all the features for training
            test_x (pd.DataFrame): dataframe containing all the features for testing
            test_y (pd.DataFrame): dataframe containing all the labels for testing
            pipe (sklearn.pipeline.Pipeline): pipeline containing all the preprocessing steps and the final model
            model_id (str): model id
            metrics_df (pd.DataFrame): dataframe containing all the metrics generated by training
            results (dict): dictionary containing all the metrics generated by training
        """
        train_config = merged_config["train"]
        models = train_config["model_params"]["models"]
        model_id = str(int(time.time()))

        train_x, train_y, test_x, test_y, val_x, val_y = utils.split_train_test(
            feature_df=feature_df,
            label_column=self.label_column,
            entity_column=self.entity_column,
            train_size=self.prep.train_size,
            val_size=self.prep.val_size,
            test_size=self.prep.test_size,
            isStratify=self.isStratify,
        )

        train_x = utils.transform_null(train_x, numeric_columns, categorical_columns)
        val_x = utils.transform_null(val_x, numeric_columns, categorical_columns)

        preprocessor_pipe_x = self.get_preprocessing_pipeline(
            numeric_columns,
            categorical_columns,
            self.prep.numeric_pipeline.get("pipeline"),
            self.prep.categorical_pipeline.get("pipeline"),
        )
        train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
        val_x_processed = preprocessor_pipe_x.transform(val_x)

        final_model = self.select_best_model(
            models, train_x_processed, train_y, val_x_processed, val_y
        )
        preprocessor_pipe_optimized = self.get_preprocessing_pipeline(
            numeric_columns,
            categorical_columns,
            self.prep.numeric_pipeline.get("pipeline"),
            self.prep.categorical_pipeline.get("pipeline"),
        )
        pipe = self.get_model_pipeline(preprocessor_pipe_optimized, final_model)
        pipe.fit(train_x, train_y)

        joblib.dump(pipe, model_file)

        results = self.get_metrics(
            pipe, train_x, train_y, test_x, test_y, val_x, val_y, train_config
        )
        results["model_id"] = model_id
        metrics_df = pd.DataFrame(
            {
                "model_id": [results["model_id"]],
                "metrics": [results["metrics"]],
                "output_model_name": [results["output_model_name"]],
            }
        ).reset_index(drop=True)

        return train_x, test_x, test_y, pipe, model_id, metrics_df, results


class ClassificationTrainer(MLTrainer):
    evalution_metrics_map = {
        metric.__name__: metric
        for metric in [average_precision_score, precision_recall_fscore_support]
    }
    models_map = {
        model.__name__: model
        for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]
    }

    def __init__(self, **kwargs):
        self.recall_to_precision_importance = kwargs.pop(
            "recall_to_precision_importance"
        )
        super().__init__(**kwargs)

        self.figure_names = {
            "roc-auc-curve": f"04-test-roc-auc-{self.output_profiles_ml_model}.png",
            "pr-auc-curve": f"03-test-pr-auc-{self.output_profiles_ml_model}.png",
            "lift-chart": f"02-test-lift-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"01-feature-importance-chart-{self.output_profiles_ml_model}.png",
        }
        self.isStratify = True

    def build_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        model_class: Union[XGBClassifier, RandomForestClassifier, MLPClassifier],
        model_config: Dict,
    ) -> Tuple:
        """Returns the classifier with best hyper-parameters after performing hyper-parameter tuning.

        Args:
            X_train (pd.DataFrame): X_train dataframe
            y_train (pd.DataFrame): y_train dataframe
            X_val (pd.DataFrame): X_val dataframe
            y_val (pd.DataFrame): y_val dataframe
            model_class (Union[XGBClassifier, RandomForestClassifier, MLPClassifier]): classifier to build model
            model_config (dict): configurations for the given model

        Returns:
            Tuple: classifier with best hyper-parameters found out using val_data along with trials info
        """
        hyperopt_space = self.generate_hyperparameter_space(model_config["hyperopts"])

        # We can set evaluation set for xgboost model which we cannot directly configure from configuration file
        fit_params = model_config.get("fitparams", {}).copy()
        if model_class.__name__ == "XGBClassifier":
            fit_params["eval_set"] = [(X_train, y_train), (X_val, y_val)]

        # Objective method to run for different hyper-parameter space
        def objective(space):
            clf = model_class(**model_config["modelparams"], **space)
            clf.fit(X_train, y_train, **fit_params)
            pred = clf.predict_proba(X_val)
            eval_metric_name = model_config["evaluation_metric"]
            pr_auc = self.evalution_metrics_map[eval_metric_name](y_val, pred[:, 1])

            return {"loss": (0 - pr_auc), "status": STATUS_OK, "config": space}

        trials = Trials()
        best_hyperparams = fmin(
            fn=objective,
            space=hyperopt_space,
            algo=tpe.suggest,
            max_evals=model_config["hyperopts_config"]["max_evals"],
            return_argmin=False,
            trials=trials,
        )
        if "early_stopping_rounds" in model_config["modelparams"]:
            del model_config["modelparams"]["early_stopping_rounds"]
        clf = model_class(**best_hyperparams, **model_config["modelparams"])
        return clf, trials

    def get_name(self):
        return "classification"

    def select_best_model(self, models, train_x, train_y, val_x, val_y):
        """
        Selects the best classifier model based on the given list of models and their configurations.

        Args:
            models (list): A list of dictionaries representing the models to be trained.
            train_x (pd.DataFrame): The training data features.
            train_y (pd.DataFrame): The training data labels.
            val_x (pd.DataFrame): The validation data features.
            val_y (pd.DataFrame): The validation data labels.
        Returns:
            final_clf (object): The selected classifier model with the best hyperparameters.
        """
        best_acc = 0
        for model_config in models:
            name = model_config["name"]

            if name in self.models_map.keys():
                clf, trials = self.build_model(
                    train_x, train_y, val_x, val_y, self.models_map[name], model_config
                )

                if best_acc < max([-1 * loss for loss in trials.losses()]):
                    final_clf = clf
                    best_acc = max([-1 * loss for loss in trials.losses()])

        return final_clf

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
                f"Only one value of label column found in label table. Please check if the label column is correct. Label column: {self.label_column}"
            )
        return label_table

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
        """Plots the diagnostics for the given model

        Args:
            Connector (Connector): Connector instance to access data warehouse
            session: valid snowpark session or redshift cursor to access data warehouse
            model (object): trained model
            stage_name (str): name of the stage
            x (pd.DataFrame): test data features
            y (pd.DataFrame): test data labels
            figure_names (dict): dict of figure names
            label_column (str): name of the label column
        """
        try:
            y_pred = model.predict_proba(x)[:, 1]
            y_true = y[label_column.upper()].values

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

    def get_metrics(
        self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config
    ) -> dict:
        model_metrics, _, prob_th = trainer_utils.get_metrics_classifier(
            model,
            train_x,
            train_y,
            test_x,
            test_y,
            val_x,
            val_y,
            train_config,
            self.recall_to_precision_importance,
        )
        model_metrics["prob_th"] = prob_th
        result_dict = {
            "output_model_name": self.output_profiles_ml_model,
            "prob_th": prob_th,
            "metrics": model_metrics,
        }
        return result_dict

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


class RegressionTrainer(MLTrainer):
    evalution_metrics_map = {
        metric.__name__: metric
        for metric in [mean_absolute_error, mean_squared_error, r2_score]
    }

    models_map = {
        model.__name__: model
        for model in [XGBRegressor, RandomForestRegressor, MLPRegressor]
    }

    def __init__(self, **kwargs):
        _ = kwargs.pop("recall_to_precision_importance", 0.0)
        super().__init__(**kwargs)

        self.figure_names = {
            # "regression-lift-chart" : f"04-regression-chart-{self.output_profiles_ml_model}.png",
            "deciles-plot": f"03-deciles-plot-{self.output_profiles_ml_model}.png",
            "residuals-chart": f"02-residuals-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"01-feature-importance-chart-{self.output_profiles_ml_model}.png",
        }
        self.isStratify=False

    def build_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        model_class: Union[XGBRegressor, RandomForestRegressor, MLPRegressor],
        model_config: Dict,
    ) -> Tuple:
        """
        Returns the regressor with best hyper-parameters after performing hyper-parameter tuning.

        Args:
            X_train (pd.DataFrame): X_train dataframe
            y_train (pd.DataFrame): y_train dataframe
            X_val (pd.DataFrame): X_val dataframe
            y_val (pd.DataFrame): y_val dataframe
            model_class: Regressor class to build the model
            model_config (dict): configurations for the given model

        Returns:
            Tuple: regressor with best hyper-parameters found out using val_data along with trials info
        """
        hyperopt_space = self.generate_hyperparameter_space(model_config["hyperopts"])

        # We can set evaluation set for XGB Regressor model which we cannot directly configure from the configuration file
        fit_params = model_config.get("fitparams", {}).copy()
        if model_class.__name__ == "XGBRegressor":
            fit_params["eval_set"] = [(X_train, y_train), (X_val, y_val)]

        # Objective method to run for different hyper-parameter space
        def objective(space):
            reg = model_class(**model_config["modelparams"], **space)
            reg.fit(X_train, y_train, **fit_params)
            pred = reg.predict(X_val)
            eval_metric_name = model_config["evaluation_metric"]
            loss = self.evalution_metrics_map[eval_metric_name](y_val, pred)

            return {"loss": loss, "status": STATUS_OK, "config": space}

        trials = Trials()
        best_hyperparams = fmin(
            fn=objective,
            space=hyperopt_space,
            algo=tpe.suggest,
            max_evals=model_config["hyperopts_config"]["max_evals"],
            return_argmin=False,
            trials=trials,
        )
        if "early_stopping_rounds" in model_config["modelparams"]:
            del model_config["modelparams"]["early_stopping_rounds"]
        reg = model_class(**best_hyperparams, **model_config["modelparams"])
        return reg, trials

    def get_name(self):
        return "regression"

    def select_best_model(self, models, train_x, train_y, val_x, val_y):
        """
        Selects the best regressor model based on the given list of models and their configurations.

        Args:
            models (list): A list of dictionaries representing the models to be trained.
            train_x (pd.DataFrame): The training data features.
            train_y (pd.DataFrame): The training data labels.
            val_x (pd.DataFrame): The validation data features.
            val_y (pd.DataFrame): The validation data labels.

        Returns:
            final_reg (object): The selected regressor model with the best hyperparameters.
        """
        best_loss = float("inf")

        for model_config in models:
            name = model_config["name"]
            if name in self.models_map.keys():
                reg, trials = self.build_model(
                    train_x, train_y, val_x, val_y, self.models_map[name], model_config
                )

                if best_loss > min(trials.losses()):
                    final_reg = reg
                    best_loss = min(trials.losses())

        return final_reg

    def prepare_label_table(self, connector: Connector, session, label_table_name: str):
        return connector.label_table(
            session,
            label_table_name,
            self.label_column,
            self.entity_column,
            None,
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

            residuals_file = connector.join_file_path(
                self.figure_names["residuals-chart"]
            )
            y_true = y[label_column.upper()]
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
            logger.error(f"Could not generate plots. {e}")

    def get_metrics(
        self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config
    ) -> dict:
        model_metrics = trainer_utils.get_metrics_regressor(
            model, train_x, train_y, test_x, test_y, val_x, val_y
        )
        result_dict = {
            "output_model_name": self.output_profiles_ml_model,
            "prob_th": None,
            "metrics": model_metrics,
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
