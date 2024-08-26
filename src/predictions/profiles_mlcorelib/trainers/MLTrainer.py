from datetime import datetime
import time
import pandas as pd

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, List
from hyperopt import hp


from ..utils.constants import TrainTablesInfo, MATERIAL_DATE_FORMAT
from ..wht.pythonWHT import PythonWHT

from ..utils import utils
from ..utils.logger import logger
from ..connectors.Connector import Connector


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
            logger.get().info(
                "No past materialisation strategy given. The training will be done on the existing eligible past materialised data only."
            )

    def map_metrics_keys(self, model_metrics: dict):
        modified_metrics = {}
        for old_key, new_key in self.metrics_key_mapping.items():
            modified_metrics[new_key] = model_metrics.get(old_key, None)

        return modified_metrics

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def prepare_label_table(self, connector: Connector, label_table_name: str):
        pass

    @abstractmethod
    def plot_diagnostics(
        self,
        connector: Connector,
        # session is being passed as argument since "self.session" is not available in Snowpark stored procedure
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
    def get_prev_pred_metrics(self, y_true, y_pred):
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

    def _train_model(
        self,
        feature_df: pd.DataFrame,
        input_col_types: dict,
        train_config: dict,
        model_file: str,
        pycaret_model_setup: callable,
        pycaret_add_custom_metric: callable,
        custom_metrics: dict,
        pycaret_compare_models: callable,
        pycaret_tune_model: callable,
        pycaret_save_model: callable,
        pycaret_get_config: callable,
        metric_to_optimize: str,
        models_to_include: List,
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

        model_id = str(int(time.time()))

        numeric_cols = [
            col.upper()
            for col in input_col_types["numeric"]
            if col.upper() in feature_df
        ]
        categorical_cols = [
            col.upper()
            for col in input_col_types["categorical"]
            if col.upper() in feature_df
        ]

        feature_df = utils.transform_null(feature_df, numeric_cols, categorical_cols)

        (
            train_x,
            train_y,
            test_x,
            test_y,
            train_data,
            test_data,
        ) = self.prepare_data(feature_df)

        numeric_cols = [
            col.upper()
            for col in input_col_types["numeric"]
            if col.upper() in feature_df
        ]
        categorical_cols = [
            col.upper()
            for col in input_col_types["categorical"]
            if col.upper() in feature_df
        ]

        n_folds = train_config["model_params"]["fold"]
        fold_strategy = train_config["model_params"]["fold_strategy"]

        # Initialize PyCaret setup for the model with train and test data
        setup = pycaret_model_setup(
            data=train_data,
            test_data=test_data,
            session_id=42,
            numeric_features=numeric_cols,
            categorical_features=categorical_cols,
            fold=n_folds,
            fold_strategy=fold_strategy,
        )

        for custom_metric in custom_metrics:
            pycaret_add_custom_metric(
                custom_metric["id"],
                custom_metric["name"],
                custom_metric["function"],
                greater_is_better=custom_metric["greater_is_better"],
            )

        best_model = pycaret_compare_models(
            sort=metric_to_optimize, include=models_to_include
        )
        tuned_model = pycaret_tune_model(
            best_model, optimize=metric_to_optimize, return_train_score=True
        )

        model_class_name = tuned_model.__class__.__name__
        pycaret_save_model(tuned_model, model_file)
        results = self.get_metrics(
            tuned_model, test_x, test_y, train_x, train_y, n_folds
        )
        train_x_transformed = pycaret_get_config("X_train_transformed")

        results["model_id"] = model_id
        results["model_class_name"] = model_class_name
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
            tuned_model,
            model_id,
            metrics_df,
            results,
        )

    @abstractmethod
    def train_model(
        self,
        feature_df: pd.DataFrame,
        input_col_types: dict,
        merged_config: dict,
        model_file: str,
    ):
        pass

    @abstractmethod
    def validate_data(self, connector, feature_table):
        pass

    @abstractmethod
    def check_min_data_requirement(self, connector: Connector, materials) -> bool:
        pass

    @abstractmethod
    def predict(self, trained_model) -> Any:
        pass

    def check_and_generate_more_materials(
        self,
        get_material_func: callable,
        materials: List[TrainTablesInfo],
        inputs: List[dict],
        whtService: PythonWHT,
        connector: Connector,
    ):
        met_data_requirement = self.check_min_data_requirement(connector, materials)

        logger.get().debug(f"Min data requirement satisfied: {met_data_requirement}")
        if met_data_requirement or self.materialisation_strategy == "":
            return materials

        model_refs = [input["model_ref"] for input in inputs]
        feature_package_path = utils.get_feature_package_path(model_refs)
        max_materializations = (
            self.materialisation_max_no_dates
            if self.materialisation_strategy == "auto"
            else len(self.materialisation_dates)
        )

        logger.get().info(
            f"""Generating snapshots of past data by doing a profiles run on input models, at different points of time in the past. 
                                Expected to run max of {2 * max_materializations} runs."""
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
                logger.get().info(f"training_dates : {training_dates}")
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
                logger.get().info(
                    f"new generated dates for feature: {feature_date}, label: {label_date}"
                )
            elif self.materialisation_strategy == "manual":
                dates = self.materialisation_dates[i].split(",")
                if len(dates) >= 2:
                    feature_date = dates[0]
                    label_date = dates[1]

                if feature_date is None or label_date is None:
                    continue

            logger.get().info(
                f"Looking for past data on dates {feature_date} and {label_date}. Will do a profiles run if they are not present."
            )
            try:
                # Check wether the materialisation is already present
                existing_materials = get_material_func(
                    start_date=feature_date,
                    end_date=feature_date,
                    return_partial_pairs=True,
                )

                if len(existing_materials) > 0:
                    # Check if any full sequence is exist or not
                    complete_sequences = [
                        sequence
                        for sequence in existing_materials
                        if all(element is not None for element in sequence)
                    ]

                    if len(complete_sequences) < 1:
                        existing_material = existing_materials[0]
                        if existing_material.feature_table_date is None:
                            whtService.run(feature_package_path, feature_date)

                        if existing_material.label_table_date is None:
                            whtService.run(feature_package_path, label_date)
                else:
                    for date in [feature_date, label_date]:
                        whtService.run(feature_package_path, date)
            except Exception as e:
                logger.get().error(str(e))
                logger.get().error("Stopped generating new material dates.")
                break

            logger.get().info(
                "Materialised feature and label data successfully, "
                f"for dates {feature_date} and {label_date}"
            )

            # Get materials with new feature start date
            # and validate min data requirement again

            # For manual strategy, we need to get the materials for the selected dates separately
            # because the selected dates may not be in the search window. so searching for materials
            # with "feature_date" as start and end date.

            # This logic will be valid for "auto" strategy as well, so we are not handling it separately.
            materials += get_material_func(
                start_date=feature_date, end_date=feature_date
            )

            logger.get().debug(
                f"new feature tables: {[m.feature_table_name for m in materials]}"
            )
            logger.get().debug(
                f"new label tables: {[m.label_table_name for m in materials]}"
            )
            if (
                self.materialisation_strategy == "auto"
                and self.check_min_data_requirement(connector, materials)
            ):
                logger.get().info("Minimum data requirement satisfied.")
                break
        if not self.check_min_data_requirement(connector, materials):
            logger.get().error(
                "Minimum data requirement not satisfied. Model performance may suffer. Try adding more datapoints by including more dates or increasing max_no_of_dates in the config."
            )

        return materials
