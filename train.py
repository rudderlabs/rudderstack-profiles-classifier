#!/usr/bin/env python
# coding: utf-8

import yaml
import json
from datetime import datetime
import joblib
import os
import time
from typing import Tuple, List, Union, Any
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from copy import deepcopy

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import sproc
import snowflake.snowpark
from snowflake.snowpark.functions import col

# from logger import logger

import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support, average_precision_score
import numpy as np 
import pandas as pd
from typing import Tuple, List

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

import utils
import constants

# logger.info("Start")
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
    connection_parameters = utils.remap_credentials(creds)
    session = Session.builder.configs(connection_parameters).create()

    metrics_table = constants.METRICS_TABLE
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
    entity_column = merged_config['data']['entity_column']
    index_timestamp = merged_config['data']['index_timestamp']
    label_column = merged_config['data']['label_column']
    label_value = merged_config['data']['label_value']
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
    utils.delete_import_files(session, stage_name, import_paths)
    utils.delete_procedures(session, train_procedure)

    def train_model(feature_df: pd.DataFrame,
                    entity_column: str,
                    label_column: str,
                    model_name_prefix: str,
                    numerical_pipeline_config: list,
                    categorical_pipeline_config: list,
                    train_size: float, 
                    val_size: float,
                    test_size: float,
                    merged_config: dict,
                    env_typ: str) -> Any:
        """Creates and saves the trained model pipeline after performing preprocessing and classification and
        returns the model id attached with the results generated."""

        train_config = merged_config['train']

        models_map = { model.__name__: model for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]}
        models = train_config["model_params"]["models"]

        train_x, train_y, test_x, test_y, val_x, val_y, X_train, X_val, X_test = utils.split_train_test(feature_df, label_column, entity_column, train_size, val_size, test_size)

        categorical_columns = utils.get_categorical_columns(feature_df, label_column, entity_column)
        numeric_columns = utils.get_numeric_columns(feature_df, label_column, entity_column)
        train_x = utils.transform_null(train_x, numeric_columns, categorical_columns)
        val_x = utils.transform_null(val_x, numeric_columns, categorical_columns)

        preprocessor_pipe_x = get_preprocessing_pipeline(numeric_columns, categorical_columns, numerical_pipeline_config, categorical_pipeline_config)
        train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
        val_x_processed = preprocessor_pipe_x.transform(val_x)
        
        final_clf = select_best_clf(models, train_x_processed, train_y, val_x_processed, val_y, models_map)
        preprocessor_pipe_optimized = get_preprocessing_pipeline(numeric_columns, categorical_columns, numerical_pipeline_config, categorical_pipeline_config)
        pipe = get_model_pipeline(preprocessor_pipe_optimized, final_clf)
        pipe.fit(train_x, train_y)

        model_metrics, _, prob_th = utils.get_metrics(pipe, train_x, train_y, test_x, test_y, val_x, val_y,train_config)

        model_id = str(int(time.time()))
        result_dict = {"model_id": model_id,
                        "model_name_prefix": model_name_prefix,
                        "prob_th": prob_th,
                        "metrics": model_metrics}
        
        metrics_df = pd.DataFrame.from_dict(result_dict).reset_index()

        if env_typ == 'snowflake':
            model_file = os.path.join('/tmp', model_file_name)
        else:
            model_file = os.path.join('tmp', f"{model_file_name}").replace('\\', '/')
        
        joblib.dump(pipe, model_file)

        column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
        if env_typ == 'snowflake':
            column_name_file = os.path.join('/tmp', f"{model_name_prefix}_{model_id}_column_names.json")
        else:
            column_name_file = os.path.join('tmp', f"{model_name_prefix}_{model_id}_column_names.json").replace('\\', '/')
        json.dump(column_dict, open(column_name_file,"w"))
        return train_x, test_x, test_y, X_train, X_val, X_test, categorical_columns, numeric_columns, model_id, model_metrics, prob_th, metrics_df, pipe, model_file, column_name_file

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
        

        feature_table = session.table(feature_table_name)
        feature_df = feature_table.to_pandas()
        feature_df.columns = feature_df.columns.str.upper()

        train_x, test_x, test_y, X_train, X_val, X_test, categorical_columns, numeric_columns, model_id, model_metrics, prob_th, metrics_df, pipe, model_file, column_name_file = train_model(feature_df, entity_column, label_column, model_name_prefix, numerical_pipeline_config, categorical_pipeline_config, train_size, val_size, test_size, merged_config, 'snowflake')

        session.write_pandas(X_train, table_name=f"{model_name_prefix.upper()}_TRAIN", auto_create_table=True, overwrite=True)
        session.write_pandas(X_val, table_name=f"{model_name_prefix.upper()}_VAL", auto_create_table=True, overwrite=True)
        session.write_pandas(X_test, table_name=f"{model_name_prefix.upper()}_TEST", auto_create_table=True, overwrite=True)
        session.write_pandas(metrics_df, table_name=f"{metrics_table}", auto_create_table=True, overwrite=False)
        session.file.put(model_file, stage_name,overwrite=True)
        session.file.put(column_name_file, stage_name,overwrite=True)
        # arg_list = [session, pipe]
        try:
            utils.plot_roc_auc_curve(session, pipe, stage_name, test_x, test_y, figure_names['roc-auc-curve'], label_column)
            utils.plot_pr_auc_curve(session, pipe, stage_name, test_x, test_y, figure_names['pr-auc-curve'], label_column)
            utils.plot_lift_chart(session, pipe, stage_name, test_x, test_y, figure_names['lift-chart'], label_column)
            utils.plot_top_k_feature_importance(session, pipe, stage_name, train_x, numeric_columns, categorical_columns, figure_names['feature-importance-chart'], top_k_features=5)
        except Exception as e:
            print(e)
            print("Could not generate plots")
        return [model_id, model_metrics, prob_th]
    
    def local_train_sp(feature_table: snowflake.snowpark.Table,
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
        feature_df = feature_table.to_pandas()
        feature_df.columns = feature_df.columns.str.upper()
        
        train_x, test_x, test_y, X_train, X_val, X_test, categorical_columns, numeric_columns, model_id, model_metrics, prob_th, metrics_df, pipe, model_file, column_name_file = train_model(feature_df, entity_column, label_column, model_name_prefix, numerical_pipeline_config, categorical_pipeline_config, train_size, val_size, test_size, merged_config, creds['type'])
        
        X_train.to_csv(f"tables/{model_name_prefix}_train.csv", index=False)
        X_val.to_csv(f"tables/{model_name_prefix}_val.csv", index=False)
        X_test.to_csv(f"tables/{model_name_prefix}_test.csv", index=False)
        metrics_table_path = os.path.join('tables', f"{metrics_table}.csv").replace('\\', '/')
        metrics_df.to_csv(metrics_table_path, index=False)

        try:
            utils.local_plot_roc_auc_curve(pipe, test_x, test_y, figure_names['roc-auc-curve'], label_column, target_path)
            utils.local_plot_pr_auc_curve(pipe, test_x, test_y, figure_names['pr-auc-curve'], label_column, target_path)
            utils.local_plot_lift_chart(pipe, test_x, test_y, figure_names['lift-chart'], label_column, target_path)
            utils.local_plot_top_k_feature_importance(pipe, train_x, target_path, numeric_columns, categorical_columns, figure_names['feature-importance-chart'], top_k_features=5)
        except Exception as e:
            print(e)
            print("Could not generate plots")
        return json.dumps([model_id, model_metrics, prob_th])
    
    material_table = utils.get_material_registry_name(session, material_registry_table_prefix)
    model_hash, creation_ts = utils.get_latest_material_hash(session, material_table, model_name)

    if start_date == None or end_date == None:
        start_date, end_date = utils.get_date_range(creation_ts, prediction_horizon_days)

    material_names, training_dates = utils.get_material_names(session, material_table, start_date, end_date, package_name, model_name, model_hash, material_table_prefix, prediction_horizon_days, output_filename)
 
    feature_table = None
    for row in material_names:
        feature_table_name, label_table_name = row
        feature_table_instance = utils.prepare_feature_table(session, 
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

    if creds['type'] == 'snowflake':
        print("Training on snowpark")
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
    else:
        print("Training locally")
        model_eval_data = local_train_sp(sorted_feature_table,
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

    if creds['type'] == 'snowflake':
        for figure_name in figure_names.values():
            try:
                utils.fetch_staged_file(session, stage_name, figure_name, target_path)
            except:
                print(f"Could not fetch {figure_name}")
            

    results = {"config": {'training_dates': training_dates,
                        'material_names': material_names,
                        'eligible_users': eligible_users,
                        'prediction_horizon_days': prediction_horizon_days,
                        'label_column': label_column,
                        'label_value': label_value,
                        'material_hash': model_hash},
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
    output_folder = 'output/dev/seq_no/4'
    output_file_name = f"{output_folder}/train_output.json"
    from pathlib import Path
    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)
    # creds['type'] = 'redshift'
       
    train(creds, inputs, output_file_name, None)
    # logger.info("Training completed")
    # materialise_past_data('2022-')