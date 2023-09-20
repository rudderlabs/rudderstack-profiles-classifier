#!/usr/bin/env python
# coding: utf-8

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import average_precision_score, precision_recall_curve, PrecisionRecallDisplay, roc_curve, RocCurveDisplay, auc
from sklearn.compose import ColumnTransformer
from xgboost import XGBClassifier, XGBRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPClassifier
import joblib
import os
import gzip
import shutil
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from typing import List
from snowflake.snowpark.functions import sproc
import snowflake.snowpark
from snowflake.snowpark.functions import col
import time
from typing import Tuple, List, Union
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

import pickle
import sys
from copy import deepcopy
# from logger import logger

import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, f1_score

from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, f1_score, average_precision_score
import numpy as np 
import pandas as pd
from typing import Tuple, List


from utils import load_yaml, remap_credentials, combine_config, get_date_range, get_column_names, get_numeric_columns, get_categorical_columns, transform_null, delete_import_files, delete_procedures, get_material_registry_name, get_latest_material_hash, get_timestamp_columns, get_material_names, prepare_feature_table, split_train_test, get_classification_metrics, get_best_th, get_metrics, get_label_date_ref, plot_pr_auc_curve, plot_roc_auc_curve, plot_lift_chart, plot_top_k_feature_importance, fetch_staged_file, get_output_directory
import constants as constants
import yaml
import json
import subprocess
from datetime import datetime, timezone
import matplotlib.pyplot as plt
import scikitplot as skplt
import shap
from abc import ABC, abstractmethod


# logger.info("Start")

class Task(ABC):
    def __init__(self, model_classes):
        self.model_classes = model_classes

    @abstractmethod
    def get_preprocessing_pipeline(self, numeric_columns: List[str], categorical_columns: List[str],
                                   numerical_pipeline_config: List[Dict], categorical_pipeline_config: List[Dict]):
        pass

    @abstractmethod
    def generate_hyperparameter_space(self, hyperopts: List[Dict]) -> Dict:
        pass

    @abstractmethod
    def build_model(self, X_train: pd.DataFrame, y_train: pd.DataFrame, X_val: pd.DataFrame, y_val: pd.DataFrame,
                    model_config: Dict) -> Tuple:
        pass


class ClassificationTask(Task):
    def __init__(self):
        super().__init__(XGBClassifier)  # Default model class for classification

    def get_preprocessing_pipeline(self, numeric_columns: List[str], categorical_columns: List[str],
                                   numerical_pipeline_config: List[Dict], categorical_pipeline_config: List[Dict]):
        # Implementation for classification preprocessing pipeline
        pass

    def generate_hyperparameter_space(self, hyperopts: List[Dict]) -> Dict:
        # Implementation for classification hyperparameter space
        pass

    def build_model(self, X_train: pd.DataFrame, y_train: pd.DataFrame, X_val: pd.DataFrame, y_val: pd.DataFrame,
                    model_config: Dict) -> Tuple:
        # Implementation for classification model building
        pass

class RegressionTask(Task):
    def __init__(self):
        super().__init__(RandomForestRegressor)  # Default model class for regression

    def get_preprocessing_pipeline(self, numeric_columns: List[str], categorical_columns: List[str],
                                   numerical_pipeline_config: List[Dict], categorical_pipeline_config: List[Dict]):
        # Implementation for regression preprocessing pipeline
        pass

    def generate_hyperparameter_space(self, hyperopts: List[Dict]) -> Dict:
        # Implementation for regression hyperparameter space
        pass

    def build_model(self, X_train: pd.DataFrame, y_train: pd.DataFrame, X_val: pd.DataFrame, y_val: pd.DataFrame,
                    model_config: Dict) -> Tuple:
        # Implementation for regression model building
        pass

def get_preprocessing_pipeline(numeric_columns: list, 
                                categorical_columns: list, 
                                numerical_pipeline_config: list, 
                                categorical_pipeline_config: list):        
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
            # logger.error(error_message)
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


hyperopts_expressions_map = {exp.__name__: exp for exp in [hp.choice, hp.quniform, hp.uniform, hp.loguniform]}
evalution_metrics_map = {metric.__name__: metric for metric in [average_precision_score, precision_recall_fscore_support]}

def get_model_pipeline(preprocessor, clf):           
    pipe = Pipeline([('preprocessor', preprocessor), 
                    ('model', clf)])
    return pipe

#Generate hyper parameter space for given options
def generate_hyperparameter_space(hyperopts: List[dict]) -> dict:
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
                
        space[name] = hyperopts_expressions_map[f"hp_{exp_type}"](name, **expression_)
    return space


def build_model(X_train:pd.DataFrame, 
                y_train:pd.DataFrame,
                X_val:pd.DataFrame, 
                y_val:pd.DataFrame,
                model_class: Union[XGBClassifier, RandomForestClassifier, MLPClassifier],
                model_config: dict) -> Tuple:
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
    hyperopt_space = generate_hyperparameter_space(model_config["hyperopts"])

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
        pr_auc = evalution_metrics_map[eval_metric_name](y_val, pred[:, 1])
        
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

def select_best_clf(models, train_x, train_y, val_x, val_y, models_map):
    """
    Selects the best classifier model based on the given list of models and their configurations.

    Args:
        models (list): A list of dictionaries representing the models to be trained.
        train_x (pd.DataFrame): The training data features.
        train_y (pd.DataFrame): The training data labels.
        val_x (pd.DataFrame): The validation data features.
        val_y (pd.DataFrame): The validation data labels.
        models_map (dict): A dictionary mapping model names to their corresponding classes.

    Returns:
        final_clf (object): The selected classifier model with the best hyperparameters.
    """
    best_acc = 0
    for model_config in models:
        name = model_config["name"]
        print(f"Training {name}")

        clf, trials = build_model(train_x, train_y, val_x, val_y, models_map[name], model_config)

        if best_acc < max([ -1*loss for loss in trials.losses()]):
            final_clf = clf
            best_acc = max([ -1*loss for loss in trials.losses()])

    return final_clf

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
    connection_parameters = remap_credentials(creds)
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
    target_path = get_output_directory(folder_path)

    notebook_config = load_yaml(config_path)
    merged_config = combine_config(notebook_config, config)

    entity_column = merged_config['data']['entity_column']
    index_timestamp = merged_config['data']['index_timestamp']
    label_column = merged_config['data']['label_column']
    label_value = merged_config['data'].get('label_value')
    prediction_task = merged_config['data'].get('task', 'classification') # Assuming default as classification
    timestamp_columns = merged_config["preprocessing"]["timestamp_columns"]
    eligible_users = merged_config['data']['eligible_users']
    ignore_features = merged_config['preprocessing']['ignore_features']
    model_name_prefix = merged_config['data']['model_name_prefix']
    start_date = merged_config['data']['train_start_dt']
    end_date = merged_config['data']['train_end_dt']
    prediction_horizon_days = merged_config['data']['prediction_horizon_days']
    model_name = merged_config['data']['model_name']
    package_name = merged_config['data']['package_name']
    train_size = merged_config['preprocessing']['train_size']
    val_size = merged_config['preprocessing']['val_size']
    test_size = merged_config['preprocessing']['test_size']
    numerical_pipeline_config = merged_config['preprocessing']['numeric_pipeline']['pipeline']
    categorical_pipeline_config = merged_config['preprocessing']['categorical_pipeline']['pipeline']

    figure_names = {"roc-auc-curve": f"01-test-roc-auc.png",
                    "pr-auc-curve": f"02-test-pr-auc.png",
                    "lift-chart": f"03-test-lift-chart.png",
                    "feature-importance-chart": f"04-feature-importance-chart.png"}
    train_procedure = 'train_sproc'

    import_paths = [utils_path, constants_path]
    delete_import_files(session, stage_name, import_paths)
    delete_procedures(session, train_procedure)
    
    if prediction_task == 'classification':
        task = ClassificationTask()
    elif prediction_task == 'regression':
        task = RegressionTask()

    @sproc(name=train_procedure, is_permanent=True, stage_location=stage_name, replace=True, imports= [current_dir]+import_paths, 
        packages=["snowflake-snowpark-python==0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "PyYAML", "numpy==1.23.1", "pandas", "hyperopt", "shap==0.41.0", "matplotlib==3.7.1", "seaborn==0.12.0", "scikit-plot==0.3.7"])
    def train_sp(session: snowflake.snowpark.Session,
                feature_table_name: str,
                entity_column: str,
                label_column: str,
                model_name_prefix: str,
                numerical_pipeline_config: list,
                categorical_pipeline_config: list,
                figure_names: dict,
                train_size: float, 
                val_size: float,
                test_size: float,
                merged_config: dict) -> list:
        """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

        Args:
            session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
            feature_table_name (str): name of the user feature table generated by profiles feature table model, and is input to training and prediction
            entity_column (str): name of entity column from feature table
            label_column (str): name of label column from feature table
            model_name_prefix (str): prefix for the model from model_configs file
            numerical_pipeline_config (list): configs for numeric pipeline from model_configs file
            categorical_pipeline_config (list): configs for categorical pipeline from model_configs file
            train_size (float): partition fraction for train data
            val_size (float): partition fraction for validation data
            test_size (float): partition fraction for test data
            merged_config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

        Returns:
            list: returns the model_id which is basically the time converted to key at which results were generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
        """
        model_file_name = constants.MODEL_FILE_NAME
        stage_name = constants.STAGE_NAME
        metrics_table = constants.METRICS_TABLE

        train_config = merged_config['train']

        models_map = { model.__name__: model for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]}
        models = train_config["model_params"]["models"]

        feature_table = session.table(feature_table_name)
        train_x, train_y, test_x, test_y, val_x, val_y = split_train_test(session, feature_table, label_column, entity_column, model_name_prefix, train_size, val_size, test_size)

        categorical_columns = get_categorical_columns(feature_table, label_column, entity_column)
        numeric_columns = get_numeric_columns(feature_table, label_column, entity_column)
        train_x = transform_null(train_x, numeric_columns, categorical_columns)

        preprocessor_pipe_x = get_preprocessing_pipeline(numeric_columns, categorical_columns, numerical_pipeline_config, categorical_pipeline_config)
        train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
        val_x_processed = preprocessor_pipe_x.transform(val_x)
        
        final_clf = select_best_clf(models, train_x_processed, train_y, val_x_processed, val_y, models_map)
        preprocessor_pipe_optimized = get_preprocessing_pipeline(numeric_columns, categorical_columns, numerical_pipeline_config, categorical_pipeline_config)
        pipe = get_model_pipeline(preprocessor_pipe_optimized, final_clf)
        pipe.fit(train_x, train_y)

        model_metrics, _, prob_th = get_metrics(pipe, train_x, train_y, test_x, test_y, val_x, val_y)

        model_id = str(int(time.time()))
        result_dict = {"model_id": model_id,
                        "model_name_prefix": model_name_prefix,
                        "prob_th": prob_th,
                        "metrics": model_metrics}
        
        metrics_df = pd.DataFrame.from_dict(result_dict).reset_index()

        session.write_pandas(metrics_df, table_name=f"{metrics_table}", auto_create_table=True, overwrite=False)

        model_file = os.path.join('/tmp', model_file_name)
        joblib.dump(pipe, model_file)
        session.file.put(model_file, stage_name,overwrite=True)

        column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
        column_name_file = os.path.join('/tmp', f"{model_name_prefix}_{model_id}_column_names.json")
        json.dump(column_dict, open(column_name_file,"w"))
        session.file.put(column_name_file, stage_name,overwrite=True)
        
        plot_roc_auc_curve(session, pipe, stage_name, test_x, test_y, figure_names['roc-auc-curve'], label_column)
        plot_pr_auc_curve(session, pipe, stage_name, test_x, test_y, figure_names['pr-auc-curve'], label_column)
        plot_lift_chart(session, pipe, stage_name, test_x, test_y, figure_names['lift-chart'], label_column)
        plot_top_k_feature_importance(session, pipe, stage_name, train_x, numeric_columns, categorical_columns, figure_names['feature-importance-chart'], top_k_features=5)
        return [model_id, model_metrics, prob_th]
    
    material_table = get_material_registry_name(session, material_registry_table_prefix)
    model_hash, creation_ts = get_latest_material_hash(session, material_table, model_name)

    if start_date == None or end_date == None:
        start_date, end_date = get_date_range(creation_ts, prediction_horizon_days)

    material_names, training_dates = get_material_names(session, material_table, start_date, end_date, package_name, model_name, model_hash, material_table_prefix, prediction_horizon_days, output_filename)
 
    feature_table = None
    for row in material_names:
        feature_table_name, label_table_name = row
        feature_table_instance = prepare_feature_table(session, 
                                    feature_table_name, 
                                    label_table_name,
                                    entity_column, 
                                    index_timestamp,
                                    timestamp_columns, 
                                    eligible_users, 
                                    label_column,
                                    label_value, 
                                    prediction_horizon_days,
                                    ignore_features)
        if feature_table is None:
            feature_table = feature_table_instance
        else:
            feature_table = feature_table.unionAllByName(feature_table_instance)

    feature_table_name_remote = f"{model_name_prefix}_features"
    sorted_feature_table = feature_table.sort(col(entity_column).asc(), col(index_timestamp).desc()).drop([index_timestamp])
    sorted_feature_table.write.mode("overwrite").save_as_table(feature_table_name_remote)

    model_eval_data = session.call(train_procedure, 
                    feature_table_name_remote,
                    entity_column,
                    label_column,
                    model_name_prefix,
                    numerical_pipeline_config,
                    categorical_pipeline_config,
                    figure_names,
                    train_size,
                    val_size,
                    test_size,
                    merged_config)

    (model_id, model_metrics, prob_th) = json.loads(model_eval_data)

    for figure_name in figure_names.values():
        fetch_staged_file(session, stage_name, figure_name, target_path)

    results = {"config": {'training_dates': training_dates,
                        'material_names': material_names,
                        'eligible_users': eligible_users,
                        'prediction_horizon_days': prediction_horizon_days,
                        'label_column': label_column,
                        'label_value': label_value,
                        'material_hash': model_hash,
                        'task': prediction_task,},
            "model_info": {'file_location': {'stage': stage_name, 'file_name': model_file_name}, 'model_id': model_id},
            "input_model_name": model_name}
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
    output_folder = 'output/dev/seq_no/2'
    output_file_name = f"{output_folder}/train_output.json"
    from pathlib import Path
    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)
       
    train(creds, inputs, output_file_name, None)
    # logger.info("Training completed")
    # materialise_past_data('2022-')