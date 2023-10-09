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

from .utils import utils
from .constants import constants
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
# from logger import logger
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
# logger.info("Start")


def train(session: snowflake.snowpark.Session, inputs: str, output_filename: str, config: dict, this: WhtMaterial) -> None:
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
    logger = Logger("ChurnTrain")
    logger.info("Creating a session and loading configs")

    model_file_name = constants.MODEL_FILE_NAME
    stage_name = constants.STAGE_NAME
    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX
    session.sql(f"create stage if not exists {stage_name.replace('@', '')}").collect()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.join(current_dir, 'utils.py')
    constants_path = os.path.join(current_dir, 'constants.py')
    # logger_path = os.path.join(current_dir, "logger.py")
    config_path = os.path.join(current_dir, 'config', 'model_configs.yaml')
    folder_path = os.path.dirname(output_filename)
    target_path = utils.get_output_directory(folder_path, this.name())

    notebook_config = utils.load_yaml(config_path)
    merged_config = utils.combine_config(notebook_config, config)
    
    prediction_task = merged_config['data'].pop('task', 'classification') # Assuming default as classification

    logger.info("Initialising trainer")
    if prediction_task == 'classification':
        trainer = utils.ClassificationTrainer(**merged_config["data"], **merged_config["preprocessing"])
    elif prediction_task == 'regression':
        trainer = utils.RegressionTrainer(**merged_config["data"], **merged_config["preprocessing"])

    figure_names = {"roc-auc-curve": f"01-test-roc-auc-{trainer.output_profiles_ml_model}.png",
                    "pr-auc-curve": f"02-test-pr-auc-{trainer.output_profiles_ml_model}.png",
                    "lift-chart": f"03-test-lift-chart-{trainer.output_profiles_ml_model}.png",
                    "feature-importance-chart": f"04-feature-importance-chart-{trainer.output_profiles_ml_model}.png"}
    train_procedure = 'train_sproc'

    import_paths = [utils_path, constants_path]
    utils.delete_import_files(session, stage_name, import_paths)
    utils.delete_procedures(session, train_procedure)

    # session.add_import(os.path.join(current_dir, 'constants.py'), 'constants')
    # session.add_import(os.path.join(current_dir, 'utils.py'), 'utils')
    
    @sproc(name=train_procedure, is_permanent=True, stage_location=stage_name, replace=True, imports= import_paths,
        packages=["snowflake-snowpark-python==0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "PyYAML", "numpy==1.23.1", "pandas", "hyperopt", "shap==0.41.0", "matplotlib==3.7.1", "seaborn==0.12.0", "scikit-plot==0.3.7"])
    def train_sp(session: snowflake.snowpark.Session,
                feature_table_name: str,
                figure_names: dict,
                merged_config: dict,
                prediction_task: str) -> dict:
        """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

        Args:
            session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
            feature_table_name (str): name of the user feature table generated by profiles feature table model, and is input to training and prediction
            figure_names (dict): A dict with the file names to be generated as its values, and the keys as the names of the figures.
            merged_config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

        Returns:
            list: returns the model_id which is basically the time converted to key at which results were generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
        """
        from constants import constants
        from utils import utils

        if prediction_task == 'classification':
            trainer_sproc = utils.ClassificationTrainer(**merged_config["data"], **merged_config["preprocessing"])
        elif prediction_task == 'regression':
            trainer = utils.RegressionTrainer(**merged_config["data"], **merged_config["preprocessing"])

        model_file_name = constants.MODEL_FILE_NAME
        stage_name = constants.STAGE_NAME
        metrics_table = constants.METRICS_TABLE
        train_config = merged_config['train']

        models = train_config["model_params"]["models"]
        model_id = str(int(time.time()))

        feature_table = session.table(feature_table_name)
        train_x, train_y, test_x, test_y, val_x, val_y = utils.split_train_test(session, feature_table, 
                                                                                trainer_sproc.label_column, 
                                                                                trainer_sproc.entity_column, 
                                                                                trainer_sproc.output_profiles_ml_model, 
                                                                                trainer_sproc.train_size,
                                                                                trainer_sproc.val_size,
                                                                                trainer_sproc.test_size)

        stringtype_features = utils.get_stringtype_features(feature_table, trainer_sproc.label_column, trainer_sproc.entity_column)
        categorical_columns = utils.merge_lists_to_unique(trainer_sproc.categorical_pipeline['categorical_columns'], stringtype_features)

        non_stringtype_features = utils.get_non_stringtype_features(feature_table, trainer_sproc.label_column, trainer_sproc.entity_column)
        numeric_columns = utils.merge_lists_to_unique(trainer_sproc.numeric_pipeline['numeric_columns'], non_stringtype_features)

        train_x = utils.transform_null(train_x, numeric_columns, categorical_columns)

        preprocessor_pipe_x = trainer_sproc.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer_sproc.numeric_pipeline["pipeline"], trainer_sproc.categorical_pipeline["pipeline"])
        train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
        val_x_processed = preprocessor_pipe_x.transform(val_x)
        
        final_model = trainer_sproc.select_best_model(models, train_x_processed, train_y, val_x_processed, val_y)
        preprocessor_pipe_optimized = trainer_sproc.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer_sproc.numeric_pipeline["pipeline"], trainer_sproc.categorical_pipeline["pipeline"])
        pipe = trainer_sproc.get_model_pipeline(preprocessor_pipe_optimized, final_model)
        pipe.fit(train_x, train_y)
        
        model_file = os.path.join('/tmp', f"{trainer_sproc.output_profiles_ml_model}_{model_file_name}")
        joblib.dump(pipe, model_file)
        session.file.put(model_file, stage_name,overwrite=True)
        
        column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
        column_name_file = os.path.join('/tmp', f"{trainer_sproc.output_profiles_ml_model}_{model_id}_column_names.json")
        json.dump(column_dict, open(column_name_file,"w"))
        session.file.put(column_name_file, stage_name,overwrite=True)
        trainer_sproc.plot_diagnostics(session, pipe, stage_name, test_x, test_y, figure_names, trainer_sproc.label_column)
        try:           
            utils.plot_top_k_feature_importance(session, pipe, stage_name, train_x, numeric_columns, categorical_columns, figure_names['feature-importance-chart'], top_k_features=5)
        except Exception as e:
            # logger.error(f"Could not generate plots {e}")
            print(f"Could not generate plots {e}")
        results = trainer_sproc.get_metrics(pipe, train_x, train_y, test_x, test_y, val_x, val_y, train_config)
        results["model_id"] = model_id
        metrics_df = pd.DataFrame.from_dict(results).reset_index()
        session.write_pandas(metrics_df, table_name=f"{metrics_table}", auto_create_table=True, overwrite=False)
        return results
    
    logger.info("Getting past data for training")
    material_table = utils.get_material_registry_name(session, material_registry_table_prefix)
    model_hash, creation_ts = utils.get_latest_material_hash(session, material_table, trainer.features_profiles_model)
    start_date, end_date = trainer.train_start_dt, trainer.train_end_dt
    if start_date == None or end_date == None:
        start_date, end_date = utils.get_date_range(creation_ts, trainer.prediction_horizon_days)

    # material_names, training_dates = utils.get_material_names(session, material_table, start_date, end_date, 
    #                                                           trainer.package_name, 
    #                                                           trainer.features_profiles_model, 
    #                                                           model_hash, 
    #                                                           material_table_prefix, 
    #                                                           trainer.prediction_horizon_days, 
    #                                                           output_filename)
 
    # feature_table = None
    # for row in material_names:
    #     feature_table_name, label_table_name = row
    #     feature_table_instance = trainer.prepare_feature_table(session, 
    #                                                            feature_table_name, 
    #                                                            label_table_name)
    #     if feature_table is None:
    #         feature_table = feature_table_instance
    #         logger.warning("Taking only one material for training. Remove the break point to train on all materials")
    #         break
    #     else:
    #         feature_table = feature_table.unionAllByName(feature_table_instance)

    feature_table_name_remote = f"{trainer.output_profiles_ml_model}_features"
    # sorted_feature_table = feature_table.sort(col(trainer.entity_column).asc(), col(trainer.index_timestamp).desc()).drop([trainer.index_timestamp])
    # sorted_feature_table.write.mode("overwrite").save_as_table(feature_table_name_remote)
    logger.info("Training and fetching the results")
    
    train_results_json = session.call(train_procedure, 
                                        feature_table_name_remote,
                                        figure_names,
                                        merged_config,
                                        prediction_task)
    logger.info("Saving train results to file")
    train_results = json.loads(train_results_json)
    model_id = train_results["model_id"]
    
    # results = {"config": {'training_dates': training_dates,
    #                     'material_names': material_names,
    #                     'material_hash': model_hash,
    #                     **asdict(trainer)},
    #         "model_info": {'file_location': {'stage': stage_name, 'file_name': f"{trainer.output_profiles_ml_model}_{model_file_name}"}, 'model_id': model_id},
    #         "input_model_name": trainer.features_profiles_model}

    #TODO: Correct results json file and remove this sample.
    results = {"config": {'material_hash': model_hash,
                        **asdict(trainer)},
            "model_info": {'file_location': {'stage': stage_name, 'file_name': f"{trainer.output_profiles_ml_model}_{model_file_name}"}, 'model_id': model_id},
            "input_model_name": trainer.features_profiles_model}

    json.dump(results, open(output_filename,"w"))

    model_timestamp = datetime.utcfromtimestamp(int(model_id)).strftime('%Y-%m-%dT%H:%M:%SZ')
    summary = trainer.prepare_training_summary(train_results, model_timestamp)
    json.dump(summary, open(os.path.join(target_path, 'training_summary.json'), "w"))
    logger.info("Fetching visualisations to local")
    for figure_name in figure_names.values():
        try:
            utils.fetch_staged_file(session, stage_name, figure_name, target_path)
        except:
            print(f"Could not fetch {figure_name}")

if __name__ == "__main__":
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
    inputs = None
    output_folder = 'output/dev/seq_no/7'
    output_file_name = f"{output_folder}/train_output.json"
    from pathlib import Path
    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)
       
    train(creds, inputs, output_file_name, None)
