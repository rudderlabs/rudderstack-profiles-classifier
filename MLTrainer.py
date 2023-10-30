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
from sklearn.metrics import precision_recall_fscore_support, average_precision_score, mean_absolute_error, mean_squared_error, r2_score

import utils
from Connector import Connector
trainer_utils = utils.TrainerUtils()

@dataclass
class MLTrainer(ABC):

    def __init__(self,
                 label_value: int,
                 label_column: str,
                 entity_column: str,
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
                 prep: utils.PreprocessorConfig):
        self.label_value = label_value
        self.label_column = label_column
        self.entity_column = entity_column
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
    hyperopts_expressions_map = {exp.__name__: exp for exp in [hp.choice, hp.quniform, hp.uniform, hp.loguniform]}    
    
    def get_preprocessing_pipeline(self, numeric_columns: List[str], 
                                    categorical_columns: List[str], 
                                    numerical_pipeline_config: List[str], 
                                    categorical_pipeline_config: List[str]):        
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
            num_params_name = numerical_params.pop('name')
            if num_params_name == 'SimpleImputer':
                missing_values = numerical_params.get('missing_values')
                if missing_values == 'np.nan':
                    numerical_params['missing_values'] = np.nan
                num_imputer_params = numerical_params
            else:
                error_message = f"Invalid num_params_name: {num_params_name} for numeric pipeline."
                logger.error(error_message)
                raise ValueError(error_message)
        num_pipeline = Pipeline([
            ('imputer', SimpleImputer(**num_imputer_params)),
        ])
        
        pipeline_params_ = dict()
        for categorical_params in categorical_pipeline_config_:
            cat_params_name = categorical_params.pop('name')
            pipeline_params_[cat_params_name] = categorical_params
            try:
                assert cat_params_name in ['SimpleImputer', 'OneHotEncoder']
            except AssertionError:
                error_message = f"Invalid cat_params_name: {cat_params_name} for categorical pipeline."
                logger.error(error_message)
                raise ValueError(error_message)
            
        cat_pipeline = Pipeline([('imputer', SimpleImputer(**pipeline_params_['SimpleImputer'])),
                                ('encoder', OneHotEncoder(**pipeline_params_['OneHotEncoder']))])

        preprocessor = ColumnTransformer(
            transformers=[('num', num_pipeline, numeric_columns),
                        ('cat', cat_pipeline, categorical_columns)])
        return preprocessor
    
    def get_model_pipeline(self, preprocessor, clf):           
        pipe = Pipeline([('preprocessor', preprocessor), 
                        ('model', clf)])
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
                    expression_["options"] = list(range( options["low"], options["high"], options.get("step", 1)))
                    
            space[name] = self.hyperopts_expressions_map[f"hp_{exp_type}"](name, **expression_)
        return space

    def prepare_feature_table(self, connector: Connector, session,
                            feature_table_name: str, 
                            label_table_name: str) -> Union[snowflake.snowpark.Table, pd.DataFrame]:
        """This function creates a feature table as per the requirement of customer that is further used for training and prediction.

        Args:
            session (snowflake.snowpark.Session): Snowpark session for data warehouse access
            feature_table_name (str): feature table from the retrieved material_names tuple
            label_table_name (str): label table from the retrieved material_names tuple
        Returns:
            snowflake.snowpark.Table: feature table made using given instance from material names
        """
        try:
            label_ts_col = f"{self.index_timestamp}_label_ts"
            if self.eligible_users:
                feature_table = connector.get_table(session, feature_table_name, filter_condition=self.eligible_users)
            else:
                feature_table = connector.get_table(session, feature_table_name) #.withColumn(label_ts_col, F.dateadd("day", F.lit(prediction_horizon_days), F.col(index_timestamp)))
            arraytype_features = connector.get_arraytype_features(session, feature_table_name)
            ignore_features = utils.merge_lists_to_unique(self.prep.ignore_features, arraytype_features)
            feature_table = connector.drop_cols(feature_table, [self.label_column])
            timestamp_columns = self.prep.timestamp_columns
            if len(timestamp_columns) == 0:
                timestamp_columns = connector.get_timestamp_columns(session, feature_table_name, self.index_timestamp)
            for col in timestamp_columns:
                feature_table = connector.add_days_diff(feature_table, col, col, self.index_timestamp)
            label_table = self.prepare_label_table(connector, session, label_table_name, label_ts_col)
            uppercase_list = lambda names: [name.upper() for name in names]
            lowercase_list = lambda names: [name.lower() for name in names]
            ignore_features_ = [col for col in feature_table.columns if col in uppercase_list(ignore_features) or col in lowercase_list(ignore_features)]
            self.prep.ignore_features = ignore_features_
            self.prep.timestamp_columns = timestamp_columns
            feature_table = connector.join_feature_table_label_table(feature_table, label_table, self.entity_column, "inner")
            feature_table = connector.drop_cols(feature_table, [label_ts_col])
            feature_table = connector.drop_cols(feature_table, ignore_features_)
            column_dict = {'arraytype_features': arraytype_features, 'timestamp_columns': timestamp_columns}
            column_name_file = connector.join_file_path(f"{self.output_profiles_ml_model}_array_time_feature_names.json")
            json.dump(column_dict, open(column_name_file,"w"))
            return feature_table
        except Exception as e:
            print("Exception occured while preparing feature table. Please check the logs for more details")
            raise e

    @abstractmethod
    def select_best_model(self, models, train_x, train_y, val_x, val_y, models_map):
        pass

    @abstractmethod
    def prepare_label_table(self, connector: Connector, session, label_table_name: str, label_ts_col: str):
        pass

    @abstractmethod
    def plot_diagnostics(self, connector: Connector, session,
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str):
        pass

    @abstractmethod
    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config):
        pass

    @abstractmethod
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        pass

class ClassificationTrainer(MLTrainer):

    evalution_metrics_map = {metric.__name__: metric for metric in [average_precision_score, precision_recall_fscore_support]}
    models_map = { model.__name__: model for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]}  

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.figure_names = {
            "roc-auc-curve": f"01-test-roc-auc-{self.output_profiles_ml_model}.png",
            "pr-auc-curve": f"02-test-pr-auc-{self.output_profiles_ml_model}.png",
            "lift-chart": f"03-test-lift-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"04-feature-importance-chart-{self.output_profiles_ml_model}.png"
        }

    def build_model(self, X_train:pd.DataFrame, 
                    y_train:pd.DataFrame,
                    X_val:pd.DataFrame, 
                    y_val:pd.DataFrame,
                    model_class: Union[XGBClassifier, RandomForestClassifier, MLPClassifier],
                    model_config: Dict) -> Tuple:
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

        #We can set evaluation set for xgboost model which we cannot directly configure from configuration file
        fit_params = model_config.get("fitparams", {}).copy()
        if model_class.__name__ == "XGBClassifier":                         
            fit_params["eval_set"] = [( X_train, y_train), ( X_val, y_val)]

        #Objective method to run for different hyper-parameter space
        def objective(space):
            clf = model_class(**model_config["modelparams"], **space)
            clf.fit(X_train, y_train, **fit_params)
            pred = clf.predict_proba(X_val)
            eval_metric_name = model_config["evaluation_metric"]
            pr_auc = self.evalution_metrics_map[eval_metric_name](y_val, pred[:, 1])
            
            return {'loss': (0  - pr_auc), 'status': STATUS_OK , "config": space}

        trials = Trials()
        best_hyperparams = fmin(fn = objective,
                                space = hyperopt_space,
                                algo = tpe.suggest,
                                max_evals = model_config["hyperopts_config"]["max_evals"],
                                return_argmin=False,
                                trials = trials)

        clf = model_class(**best_hyperparams, **model_config["modelparams"])
        return clf, trials
    
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
                print(f"Training {name}")

                clf, trials = self.build_model(train_x, train_y, val_x, val_y, self.models_map[name], model_config)

                if best_acc < max([ -1*loss for loss in trials.losses()]):
                    final_clf = clf
                    best_acc = max([ -1*loss for loss in trials.losses()])

        return final_clf

    def prepare_label_table(self, connector: Connector, session, label_table_name: str, label_ts_col: str):
        label_table = connector.label_table(session, label_table_name, self.label_column, self.entity_column, self.index_timestamp, self.label_value, label_ts_col)
        distinct_values = connector.get_distinct_values_in_column(label_table, self.label_column)
        if len(distinct_values) == 1:
            raise ValueError(f"Only one value of label column found in label table. Please check if the label column is correct. Label column: {self.label_column}")
        return label_table

    def plot_diagnostics(self, connector: Connector, session,
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str) -> None:
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
            roc_auc_file = connector.join_file_path(figure_names['roc-auc-curve'])
            utils.plot_roc_auc_curve(model, x, y, roc_auc_file, label_column)
            connector.save_file(session, roc_auc_file, stage_name, overwrite=True)

            pr_auc_file = connector.join_file_path(figure_names['pr-auc-curve'])
            utils.plot_pr_auc_curve(model, x, y, pr_auc_file, label_column)
            connector.save_file(session, pr_auc_file, stage_name, overwrite=True)

            lift_chart_file = connector.join_file_path(figure_names['lift-chart'])
            utils.plot_lift_chart(model, x, y, lift_chart_file, label_column)
            connector.save_file(session, lift_chart_file, stage_name, overwrite=True)
        except Exception as e:
            logger.error(f"Could not generate plots. {e}")
        pass

    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config) -> dict:
        model_metrics, _, prob_th = trainer_utils.get_metrics_classifier(model, train_x, train_y, test_x, test_y, val_x, val_y, train_config)
        model_metrics['prob_th'] = prob_th
        result_dict = {"output_model_name": self.output_profiles_ml_model,
                       "prob_th": prob_th,
                        "metrics": model_metrics}
        return result_dict
    
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        training_summary ={"timestamp": model_timestamp,
                           "data": {"metrics": model_results['metrics'], 
                                    "threshold": model_results['prob_th']}}
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

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.figure_names = {}

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
        hyperopt_space = self.generate_hyperparameter_space(
            model_config["hyperopts"]
        )

        # We can set evaluation set for XGB Regressor model which we cannot directly configure from the configuration file
        fit_params = model_config.get("fitparams", {}).copy()
        if model_class.__name__ == "XGBRegressor":
            fit_params["eval_set"] = [(X_train, y_train), (X_val, y_val)]

        # Objective method to run for different hyper-parameter space
        def objective(space):
            reg = model_class(**model_config["modelparams"], **space)
            reg.fit(X_train, y_train)
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

        reg = model_class(**best_hyperparams, **model_config["modelparams"])
        return reg, trials
    
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
            print(f"Training {name}")

            if name in self.models_map.keys():
                reg, trials = self.build_model(
                    train_x, train_y, val_x, val_y, self.models_map[name], model_config
                )

                if best_loss > min(trials.losses()):
                    final_reg = reg
                    best_loss = min(trials.losses())

        return final_reg
    
    def prepare_label_table(self, connector: Connector, session, label_table_name: str, label_ts_col: str):
        return connector.label_table(session, label_table_name, self.label_column, self.entity_column, self.index_timestamp, None, label_ts_col)

    def plot_diagnostics(self, connector: Connector, session, 
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str):
        # To implemenet for regression - can be residual plot, binned lift chart adjusted to quantiles etc
        pass

    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config) -> dict:
        model_metrics = trainer_utils.get_metrics_regressor(model, train_x, train_y, test_x, test_y, val_x, val_y)
        result_dict = {"output_model_name": self.output_profiles_ml_model,
                       "prob_th": None,
                        "metrics": model_metrics}
        return result_dict
    
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        training_summary ={"timestamp": model_timestamp,
                           "data": {"metrics": model_results['metrics']}}
        return training_summary
