#!/usr/bin/env python
# coding: utf-8

import yaml
import json
from datetime import datetime
import joblib
import os
import time
from typing import Tuple, List, Union
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from copy import deepcopy

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from xgboost import XGBClassifier, XGBRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import sproc
import snowflake.snowpark
from snowflake.snowpark.functions import col

from logger import logger

import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support, average_precision_score, mean_absolute_error, mean_squared_error
import numpy as np 
import pandas as pd
from typing import Tuple, List, Dict

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

import utils
import constants
from abc import ABC, abstractmethod
from dataclasses import dataclass
logger.info("Start")

class MLTrainer(ABC):    
    hyperopts_expressions_map = {exp.__name__: exp for exp in [hp.choice, hp.quniform, hp.uniform, hp.loguniform]}
    def __init__(self, data_config: utils.DataConfig, preprocessing_config: utils.PreprocessorConfig):
        self.data = data_config
        self.prep = preprocessing_config
    
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

        for categorical_params in categorical_pipeline_config_:
            cat_params_name = categorical_params.pop('name')
            if cat_params_name == 'SimpleImputer':
                cat_imputer_params = categorical_params
            elif cat_params_name == 'OneHotEncoder':
                cat_encoder_params = categorical_params
            else:
                error_message = f"Invalid cat_params_name: {num_params_name} for catagorical pipeline."
                # logger.error(error_message)
                raise ValueError(error_message)

        cat_pipeline = Pipeline([('imputer', SimpleImputer(**cat_imputer_params)),
                                ('encoder', OneHotEncoder(**cat_encoder_params))])

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

    @abstractmethod
    def build_model(self, 
                    X_train: pd.DataFrame, 
                    y_train: pd.DataFrame, 
                    X_val: pd.DataFrame,
                    y_val: pd.DataFrame,
                    model_class: Union[XGBClassifier, RandomForestClassifier, MLPClassifier, XGBRegressor, RandomForestRegressor, MLPRegressor],
                    model_config: Dict) -> Tuple:
        pass
    @abstractmethod
    def select_best_model(self, models, train_x, train_y, val_x, val_y, models_map):
        pass
    @abstractmethod
    def plot_diagnotics(self, session: snowflake.snowpark.Session, 
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
    def prepare_training_summary(self):
        pass


class ClassificationTrainer(MLTrainer):
    evalution_metrics_map = {metric.__name__: metric for metric in [average_precision_score, precision_recall_fscore_support]}
    models_map = { model.__name__: model for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]}
    def __init__(self, data_config: utils.ClassifierDataConfig, preprocessing_config: utils.PreprocessorConfig):
        super().__init__(data_config, preprocessing_config)
        
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
            print(f"Training {name}")

            clf, trials = self.build_model(train_x, train_y, val_x, val_y, self.models_map[name], model_config)

            if best_acc < max([ -1*loss for loss in trials.losses()]):
                final_clf = clf
                best_acc = max([ -1*loss for loss in trials.losses()])

        return final_clf
    
    def plot_diagnotics(self, session: snowflake.snowpark.Session, 
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str) -> None:
        """Plots the diagnostics for the given model

        Args:
            session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
            model (object): trained model
            stage_name (str): name of the stage
            x (pd.DataFrame): test data features
            y (pd.DataFrame): test data labels
            figure_names (dict): dict of figure names
            label_column (str): name of the label column
        """
        try:
            utils.plot_roc_auc_curve(session, model, stage_name, x, y, figure_names['roc-auc-curve'], label_column)
            utils.plot_pr_auc_curve(session, model, stage_name, x, y, figure_names['pr-auc-curve'], label_column)
            utils.plot_lift_chart(session, model, stage_name, x, y, figure_names['lift-chart'], label_column)
        except Exception as e:
            print(e)
            print("Could not generate plots")
        pass
    
    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config):
        model_metrics, _, prob_th = utils.get_metrics(model, train_x, train_y, test_x, test_y, val_x, val_y, train_config)
        result_dict = {"model_name_prefix": self.data.model_name_prefix,
                       "prob_th": prob_th,
                        "metrics": model_metrics}
        pass
    
    def prepare_training_summary(self):
        # Preare the training_summary.json as some contents (ex: prob_th) are model specific
        pass
            

class RegressionTrainer(MLTrainer):
    # A different set of evaluation metrics. 
    evalution_metrics_map = {metric.__name__: metric for metric in [mean_absolute_error, mean_squared_error]}
    def build_model(self, X_train: pd.DataFrame, y_train: pd.DataFrame, X_val: pd.DataFrame, y_val: pd.DataFrame,
                    model_config: Dict) -> Tuple:
        # Implementation for regression model building
        pass
    def select_best_model(self, models, train_x, train_y, val_x, val_y, models_map):
        # Implementation for regression model selection
        pass
    
    def plot_diagnotics(self, session: snowflake.snowpark.Session, 
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str):
        # To implemenet for regression - can be residual plot, binned lift chart adjusted to quantiles etc
        pass
    
    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config):
        # TODO: To implement get_metrics and return metrics
        pass
    
    def prepare_training_summary(self):
        pass


def train(creds: dict, inputs: str, output_filename: str, config: dict) -> None:
    """Trains the model and saves the model with given output_filename.

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        inputs (str): Not being used currently. Can pass a blank string. For future support
        output_filename (str): path to the file where the model details including model id etc are written. Used in prediction step.
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

    Raises:
        ValueError: If num_params_name is invalid for numeric pipeline
        ValueError: If cat_params_name is invalid for catagorical pipeline

    Returns:
        None: saves the model but returns nothing
    """
    connection_parameters = utils.remap_credentials(creds)
    session = Session.builder.configs(connection_parameters).create()

    model_file_name = constants.MODEL_FILE_NAME
    stage_name = constants.STAGE_NAME
    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX
    session.sql(f"create stage if not exists {stage_name.replace('@', '')}").collect()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.join(current_dir, 'utils.py')
    constants_path = os.path.join(current_dir, 'constants.py')
    config_path = os.path.join(current_dir, 'config', 'model_configs.yaml')
    folder_path = os.path.dirname(output_filename)
    target_path = utils.get_output_directory(folder_path)

    notebook_config = utils.load_yaml(config_path)
    merged_config = utils.combine_config(notebook_config, config)
    
    prediction_task = merged_config['data'].get('task', 'classification') # Assuming default as classification

    figure_names = {"roc-auc-curve": f"01-test-roc-auc.png",
                    "pr-auc-curve": f"02-test-pr-auc.png",
                    "lift-chart": f"03-test-lift-chart.png",
                    "feature-importance-chart": f"04-feature-importance-chart.png"}
    train_procedure = 'train_sproc'

    import_paths = [utils_path, constants_path]
    utils.delete_import_files(session, stage_name, import_paths)
    utils.delete_procedures(session, train_procedure)
    
    prep_config = utils.PreprocessorConfig(**merged_config["preprocessing"])
    if prediction_task == 'classification':
        data_config = utils.ClassifierDataConfig(**merged_config["data"])
        trainer = ClassificationTrainer(data_config, prep_config)
    elif prediction_task == 'regression':
        data_config = utils.DataConfig(**merged_config["data"])
        trainer = RegressionTrainer(data_config, prep_config)
    
    @sproc(name=train_procedure, is_permanent=True, stage_location=stage_name, replace=True, imports= [current_dir]+import_paths, 
        packages=["snowflake-snowpark-python==0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "PyYAML", "numpy==1.23.1", "pandas", "hyperopt", "shap==0.41.0", "matplotlib==3.7.1", "seaborn==0.12.0", "scikit-plot==0.3.7"])
    def train_sp(session: snowflake.snowpark.Session,
                feature_table_name: str,
                figure_names: dict,
                merged_config: dict) -> list:
        """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

        Args:
            session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
            feature_table_name (str): name of the user feature table generated by profiles feature table model, and is input to training and prediction
            figure_names: A dict with the file names to be generated as its values, and the keys as the names of the figures.
            merged_config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

        Returns:
            list: returns the model_id which is basically the time converted to key at which results were generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
        """
        model_file_name = constants.MODEL_FILE_NAME
        stage_name = constants.STAGE_NAME
        metrics_table = constants.METRICS_TABLE
        train_config = merged_config['train']

        models = train_config["model_params"]["models"]
        model_id = str(int(time.time()))

        feature_table = session.table(feature_table_name)
        train_x, train_y, test_x, test_y, val_x, val_y = utils.split_train_test(session, feature_table, 
                                                                                trainer.data.label_column, 
                                                                                trainer.data.entity_column, 
                                                                                trainer.data.model_name_prefix, 
                                                                                trainer.prep.train_size, 
                                                                                trainer.prep.val_size, 
                                                                                trainer.prep.test_size)

        categorical_columns = utils.get_categorical_columns(feature_table, trainer.data.label_column, trainer.data.entity_column)
        numeric_columns = utils.get_numeric_columns(feature_table, trainer.data.label_column, trainer.data.entity_column)
        train_x = utils.transform_null(train_x, numeric_columns, categorical_columns)

        preprocessor_pipe_x = trainer.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer.prep.numeric_pipeline.get("pipeline"), trainer.prep.categorical_pipeline.get("pipeline"))
        train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
        val_x_processed = preprocessor_pipe_x.transform(val_x)
        
        final_model = trainer.select_best_model(models, train_x_processed, train_y, val_x_processed, val_y)
        preprocessor_pipe_optimized = trainer.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer.prep.numeric_pipeline.get("pipeline"), trainer.prep.categorical_pipeline.get("pipeline"))
        pipe = trainer.get_model_pipeline(preprocessor_pipe_optimized, final_model)
        pipe.fit(train_x, train_y)
        
        model_file = os.path.join('/tmp', model_file_name)
        joblib.dump(pipe, model_file)
        session.file.put(model_file, stage_name,overwrite=True)
        
        column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
        column_name_file = os.path.join('/tmp', f"{trainer.data.model_name_prefix}_{model_id}_column_names.json")
        json.dump(column_dict, open(column_name_file,"w"))
        session.file.put(column_name_file, stage_name,overwrite=True)
        
        #TODO: Add support for regression. no prob_th in that case
        model_metrics, _, prob_th = utils.get_metrics(pipe, train_x, train_y, test_x, test_y, val_x, val_y, train_config)

        result_dict = {"model_id": model_id,
                        "model_name_prefix": trainer.data.model_name_prefix,
                        "prob_th": prob_th,
                        "metrics": model_metrics}
        
        metrics_df = pd.DataFrame.from_dict(result_dict).reset_index()

        session.write_pandas(metrics_df, table_name=f"{metrics_table}", auto_create_table=True, overwrite=False)
        
        try:
            trainer.plot_diagnotics(session, pipe, stage_name, test_x, test_y, figure_names, trainer.data.label_column)
            utils.plot_top_k_feature_importance(session, pipe, stage_name, train_x, numeric_columns, categorical_columns, figure_names['feature-importance-chart'], top_k_features=5)
        except Exception as e:
            print(e)
            print("Could not generate plots")
        return [model_id, model_metrics, prob_th]
    
    material_table = utils.get_material_registry_name(session, material_registry_table_prefix)
    model_hash, creation_ts = utils.get_latest_material_hash(session, material_table, trainer.data.model_name)
    start_date, end_date = trainer.data.train_start_dt, trainer.data.train_end_dt
    if start_date == None or end_date == None:
        start_date, end_date = utils.get_date_range(creation_ts, trainer.data.prediction_horizon_days)

    material_names, training_dates = utils.get_material_names(session, material_table, start_date, end_date, 
                                                              trainer.data.package_name, 
                                                              trainer.data.model_name, 
                                                              model_hash, 
                                                              material_table_prefix, 
                                                              trainer.data.prediction_horizon_days, 
                                                              output_filename)
 
    feature_table = None
    for row in material_names:
        feature_table_name, label_table_name = row
        feature_table_instance = utils.prepare_feature_table(session, 
                                    feature_table_name, 
                                    label_table_name,
                                    trainer.data, 
                                    trainer.prep)
        if feature_table is None:
            feature_table = feature_table_instance
            logger.info("Taking only one material for training. Remove the break point to train on all materials")
            break
        else:
            feature_table = feature_table.unionAllByName(feature_table_instance)

    feature_table_name_remote = f"{trainer.data.model_name_prefix}_features"
    sorted_feature_table = feature_table.sort(col(trainer.data.entity_column).asc(), col(trainer.data.index_timestamp).desc()).drop([trainer.data.index_timestamp])
    sorted_feature_table.write.mode("overwrite").save_as_table(feature_table_name_remote)

    model_eval_data = session.call(train_procedure, 
                    feature_table_name_remote,
                    figure_names,
                    merged_config)

    (model_id, model_metrics, prob_th) = json.loads(model_eval_data)

    for figure_name in figure_names.values():
        try:
            utils.fetch_staged_file(session, stage_name, figure_name, target_path)
        except:
            print(f"Could not fetch {figure_name}")
            

    results = {"config": {'training_dates': training_dates,
                        'material_names': material_names,
                        'eligible_users': trainer.data.eligible_users,
                        'prediction_horizon_days': trainer.data.prediction_horizon_days,
                        'label_column': trainer.data.label_column,
                        'label_value': trainer.data.label_value,
                        'material_hash': model_hash,
                        'task': prediction_task,},
            "model_info": {'file_location': {'stage': stage_name, 'file_name': model_file_name}, 'model_id': model_id},
            "input_model_name": trainer.data.model_name}
    json.dump(results, open(output_filename,"w"))

    model_timestamp = datetime.utcfromtimestamp(int(model_id)).strftime('%Y-%m-%dT%H:%M:%SZ')
    summary = {"timestamp": model_timestamp,
               "data": {"metrics": model_metrics, "threshold": prob_th}}
    json.dump(summary, open(os.path.join(target_path, 'training_summary.json'), "w"))

if __name__ == "__main__":
    homedir = os.path.expanduser("~")
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
    inputs = None
    output_folder = 'output/dev/seq_no/4'
    output_file_name = f"{output_folder}/train_output.json"
    from pathlib import Path
    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)
       
    train(creds, inputs, output_file_name, None)
    # logger.info("Training completed")
    # materialise_past_data('2022-')