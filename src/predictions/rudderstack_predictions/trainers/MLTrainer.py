from datetime import datetime
import joblib
import time
import numpy as np
import pandas as pd

from copy import deepcopy
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Tuple, List, Union, Dict
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder


from ..utils.constants import TrainTablesInfo, MATERIAL_DATE_FORMAT
from ..wht.pythonWHT import PythonWHT

from ..utils import utils
from ..utils.logger import logger
from ..connectors.Connector import Connector
from ..utils import constants

trainer_utils = utils.TrainerUtils()


@dataclass
class MLTrainer(ABC):
    def __init__(
        self,
        **kwargs,
    ):
        self.label_value = kwargs["data"]["label_value"]
        self.label_column = kwargs["data"]["label_column"]
        self.entity_column = kwargs["data"]["entity_column"]
        self.entity_key = kwargs["data"]["entity_key"]
        self.output_profiles_ml_model = kwargs["data"]["output_profiles_ml_model"]
        self.index_timestamp = kwargs["data"]["index_timestamp"]
        self.eligible_users = kwargs["data"]["eligible_users"]
        self.train_start_dt = kwargs["data"]["train_start_dt"]
        self.train_end_dt = kwargs["data"]["train_end_dt"]
        self.prediction_horizon_days = kwargs["data"]["prediction_horizon_days"]
        self.max_row_count = kwargs["data"]["max_row_count"]
        self.recall_to_precision_importance = kwargs["data"][
            "recall_to_precision_importance"
        ]
        self.prep = utils.PreprocessorConfig(**kwargs["preprocessing"])
        self.outputs = utils.OutputsConfig(**kwargs["outputs"])
        self.isStratify = None
        new_materialisations_config = kwargs["data"].get(
            "new_materialisations_config", {}
        )
        self.load_materialisation_config(new_materialisations_config)
        self.trainer_utils = utils.TrainerUtils()

    hyperopts_expressions_map = {
        exp.__name__: exp for exp in [hp.choice, hp.quniform, hp.uniform, hp.loguniform]
    }

    def load_materialisation_config(self, materialisation_config: dict):
        self.materialisation_strategy = materialisation_config.get(
            "strategy", ""
        ).lower()
        assert self.materialisation_strategy in [
            "auto",
            "manual",
            "",
        ], "materialisation strategy can only be 'auto', 'manual', or ''."
        if self.materialisation_strategy == "manual":
            try:
                self.materialisation_dates = materialisation_config["dates"]
            except KeyError:
                raise KeyError(
                    "materialisation dates are required for manual strategy in the input config."
                )
        elif self.materialisation_strategy == "auto":
            try:
                self.materialisation_max_no_dates = int(
                    materialisation_config["max_no_of_dates"]
                )
            except KeyError as e:
                raise KeyError(
                    f"max_no_of_dates required for auto materialisation strategy. {e} not found in input config"
                )
            try:
                self.feature_data_min_date_diff = int(
                    materialisation_config["feature_data_min_date_diff"]
                )
            except KeyError as e:
                raise KeyError(
                    f"feature_data_min_date_diff required for auto materialisation strategy. {e} not found in input config"
                )
        elif self.materialisation_strategy == "":
            logger.info(
                "No past materialisation strategy given. The training will be done on the existing eligible past materialised data only."
            )

    @abstractmethod
    def get_name(self):
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
    def get_metrics(self):
        pass

    @abstractmethod
    def prepare_training_summary(
        self, model_results: dict, model_timestamp: str
    ) -> dict:
        pass

    @abstractmethod
    def load_model(self, model_file: str):
        pass

    def prepare_data(
        self,
        feature_df: pd.DataFrame,
    ):
        preprocess_config = {
            "numeric_imputation": "median",
            "categorical_imputation": "mode",
            "iterative_imputation_iters": 5,
            "numeric_iterative_imputer": "lightgbm",
            "categorical_iterative_imputer": "lightgbm",
        }

        train_x, train_y, test_x, test_y = utils.split_train_test(
            feature_df=feature_df,
            label_column=self.label_column,
            entity_column=self.entity_column,
            train_size=self.prep.train_size,
            isStratify=self.isStratify,
        )

        train_data = pd.concat([train_x, train_y], axis=1)
        test_data = pd.concat([test_x, test_y], axis=1)

        return (
            train_x,
            train_y,
            test_x,
            test_y,
            train_data,
            test_data,
        )

    def train_model_(
        self,
        feature_df: pd.DataFrame,
        train_config: dict,
        model_file: str,
        pycaret_model_setup: callable,
        pycaret_add_custom_metric: callable,
        custom_metrics: dict,
        pycaret_compare_models: callable,
        pycaret_save_model: callable,
        pycaret_get_config: callable,
    ):
        """Creates and saves the trained model pipeline after performing preprocessing and classification
        and returns the various variables required for further processing by training procesudres/functions.

        Args:

            feature_df (pd.DataFrame): dataframe containing all the features and labels
            categorical_columns (List[str]): list of categorical columns in the feature_df
            numeric_columns (List[str]): list of numeric columns in the feature_df
            train_config (dict): configs generated by merging configs from profiles.yaml and model_configs.yaml file
            model_file (str): path to the file where the model is to be saved
            pycaret_model_setup (function): function to setup the model
            pycaret_compare_models (function): function to compare the models

        Returns:
            train_x (pd.DataFrame): dataframe containing all the features for training
            test_x (pd.DataFrame): dataframe containing all the features for testing
            test_y (pd.DataFrame): dataframe containing all the labels for testing
            pipe (sklearn.pipeline.Pipeline): pipeline containing all the preprocessing steps and the final model
            model_id (str): model id
            metrics_df (pd.DataFrame): dataframe containing all the metrics generated by training
            results (dict): dictionary containing all the metrics generated by training
        """
        models = train_config["model_params"]["models"]
        fold_param = 5

        model_id = str(int(time.time()))

        (
            train_x,
            train_y,
            test_x,
            test_y,
            train_data,
            test_data,
        ) = self.prepare_data(feature_df)

        # Initialize PyCaret setup for the model with train and test data
        setup = pycaret_model_setup(
            data=train_data,
            test_data=test_data,
            fold_strategy="stratifiedkfold",
            fold=fold_param,
            session_id=42,
        )

        for custom_metric in custom_metrics:
            pycaret_add_custom_metric(
                custom_metric["id"],
                custom_metric["name"],
                custom_metric["function"],
                greater_is_better=custom_metric["greater_is_better"],
            )

        # Compare different models and select the best one
        best_model = pycaret_compare_models()

        # Save the final model
        pycaret_save_model(best_model, model_file)

        # Get metrics
        results = self.get_metrics(best_model, fold_param, train_x, train_y)

        train_x_transformed = pycaret_get_config("X_train_transformed")

        results["model_id"] = model_id
        metrics_df = pd.DataFrame(
            {
                "model_id": [results["model_id"]],
                "metrics": [results["metrics"]],
                "output_model_name": [results["output_model_name"]],
            }
        ).reset_index(drop=True)

        return (
            train_x_transformed,
            test_x,
            test_y,
            best_model,
            model_id,
            metrics_df,
            results,
        )

    @abstractmethod
    def train_model(
        self,
        feature_df: pd.DataFrame,
        merged_config: dict,
        model_file: str,
    ):
        pass

    @abstractmethod
    def validate_data(self, connector, feature_table):
        pass

    @abstractmethod
    def check_min_data_requirement(
        self, connector: Connector, session, materials
    ) -> bool:
        pass

    @abstractmethod
    def predict(self, trained_model) -> Any:
        pass

    def check_and_generate_more_materials(
        self,
        get_material_func: callable,
        materials: List[TrainTablesInfo],
        input_models: str,
        whtService: PythonWHT,
        connector: Connector,
        session,
    ):
        met_data_requirement = self.check_min_data_requirement(
            connector, session, materials
        )

        logger.debug(f"Min data requirement satisfied: {met_data_requirement}")
        logger.debug(
            f"New material generation strategy : {self.materialisation_strategy}"
        )
        if met_data_requirement or self.materialisation_strategy == "":
            return materials

        feature_package_path = utils.get_feature_package_path(input_models)
        max_materializations = (
            self.materialisation_max_no_dates
            if self.materialisation_strategy == "auto"
            else len(self.materialisation_dates)
        )

        for i in range(max_materializations):
            feature_date = None
            label_date = None

            if self.materialisation_strategy == "auto":
                training_dates = [
                    utils.datetime_to_date_string(m.feature_table_date)
                    for m in materials
                ]
                training_dates = [
                    date_str for date_str in training_dates if len(date_str) != 0
                ]
                logger.info(f"training_dates : {training_dates}")
                training_dates = sorted(
                    training_dates,
                    key=lambda x: datetime.strptime(x, MATERIAL_DATE_FORMAT),
                    reverse=True,
                )

                max_feature_date = training_dates[0]
                min_feature_date = training_dates[-1]

                feature_date, label_date = utils.generate_new_training_dates(
                    max_feature_date,
                    min_feature_date,
                    training_dates,
                    self.prediction_horizon_days,
                    self.feature_data_min_date_diff,
                )
                logger.info(
                    f"new generated dates for feature: {feature_date}, label: {label_date}"
                )
            elif self.materialisation_strategy == "manual":
                dates = self.materialisation_dates[i].split(",")
                if len(dates) >= 2:
                    feature_date = dates[0]
                    label_date = dates[1]

                if feature_date is None or label_date is None:
                    continue

            try:
                for date in [feature_date, label_date]:
                    whtService.run(feature_package_path, date)
            except Exception as e:
                logger.warning(str(e))
                logger.warning("Stopped generating new material dates.")
                break

            logger.info(
                "Materialised feature and label data successfully, "
                f"for dates {feature_date} and {label_date}"
            )

            # Get materials with new feature start date
            # and validate min data requirement again
            materials = get_material_func(start_date=feature_date)
            logger.debug(
                f"new feature tables: {[m.feature_table_name for m in materials]}"
            )
            logger.debug(f"new label tables: {[m.label_table_name for m in materials]}")
            if (
                self.materialisation_strategy == "auto"
                and self.check_min_data_requirement(connector, session, materials)
            ):
                logger.info("Minimum data requirement satisfied.")
                break
        if not self.check_min_data_requirement(connector, session, materials):
            logger.warning(
                "Minimum data requirement not satisfied. Model performance may suffer. Try adding more datapoints by including more dates or increasing max_no_of_dates in the config."
            )

        return materials
