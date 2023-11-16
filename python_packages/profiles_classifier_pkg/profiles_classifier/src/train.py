#!/usr/bin/env python
# coding: utf-8
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yaml
import json
from datetime import datetime
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
warnings.filterwarnings("ignore", category=DeprecationWarning)
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

import utils
import constants
from SnowflakeConnector import SnowflakeConnector
from MLTrainer import ClassificationTrainer, RegressionTrainer
from profiles_rudderstack.material import WhtMaterial
from logger import logger
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
# logger.info("Start")

def train(session: snowflake.snowpark.Session, inputs: str, output_filename: str, config: dict, wh_type: str, this: WhtMaterial) -> None:
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
    # logger = Logger("ChurnTrain")
    logger.info("Creating a session and loading configs")

    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX
    positive_boolean_flags = constants.POSITIVE_BOOLEAN_FLAGS
    cardinal_feature_threshold = constants.CARDINAL_FEATURE_THRESOLD
    min_sample_for_training = constants.MIN_SAMPLES_FOR_TRAINING
    metrics_table = constants.METRICS_TABLE
    model_file_name = constants.MODEL_FILE_NAME
    stage_name = constants.STAGE_NAME

    current_dir = os.path.dirname(os.path.abspath(__file__))
    import_files = ("utils.py","constants.py", "logger.py", "Connector.py", "SnowflakeConnector.py", "MLTrainer.py")
    import_paths = []
    for file in import_files:
        import_paths.append(os.path.join(current_dir, file))
    config_path = os.path.join(current_dir, 'config', 'model_configs.yaml')
    folder_path = os.path.dirname(output_filename)
    target_path = utils.get_output_directory(folder_path)

    """ Initialising trainer """
    logger.info("Initialising trainer")
    notebook_config = utils.load_yaml(config_path)
    merged_config = utils.combine_config(notebook_config, config)
    
    prediction_task = merged_config['data'].pop('task', 'classification') # Assuming default as classification

    logger.debug("Initialising trainer")
    prep_config = utils.PreprocessorConfig(**merged_config["preprocessing"])
    if prediction_task == 'classification':    
        trainer = ClassificationTrainer(**merged_config["data"], prep=prep_config)
    elif prediction_task == 'regression':
        trainer = RegressionTrainer(**merged_config["data"], prep=prep_config)
    
    logger.info(f"Started training for {trainer.output_profiles_ml_model} to predict {trainer.label_column}")
    if trainer.eligible_users:
        logger.info(f"Only following users are considered for training: {trainer.eligible_users}")
    else:
        logger.warning("Consider shortlisting the users through eligible_users flag to get better results for a specific user group - such as payers only, monthly active users etc.")

    """ Building session """
    warehouse = wh_type
    logger.info(f"Building session for {warehouse}")
    if warehouse == 'snowflake':
        train_procedure = 'train_and_store_model_results_sf'
        connector = SnowflakeConnector()
        # session = connector.build_session(creds)
        connector.create_stage(session, stage_name)
        connector.delete_import_files(session, stage_name, import_paths)
        connector.delete_procedures(session)

        @sproc(name=train_procedure, is_permanent=True, stage_location=stage_name, replace=True, imports=[current_dir]+import_paths, 
            packages=["snowflake-snowpark-python==0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "PyYAML", "numpy==1.23.1", "pandas", "hyperopt", "shap==0.41.0", "matplotlib==3.7.1", "seaborn==0.12.0", "scikit-plot==0.3.7"])
        def train_and_store_model_results_sf(session: snowflake.snowpark.Session,
                    feature_table_name: str,
                    figure_names: dict,
                    merged_config: dict) -> dict:
            """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

            Args:
                session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
                feature_table_name (str): name of the user feature table generated by profiles feature table model, and is input to training and prediction
                figure_names (dict): A dict with the file names to be generated as its values, and the keys as the names of the figures.
                merged_config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

            Returns:
                dict: returns the model_id which is basically the time converted to key at which results were generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
            """
            feature_df = connector.get_table_as_dataframe(session, feature_table_name)
            model_file = connector.join_file_path(f"{trainer.output_profiles_ml_model}_{model_file_name}")
            categorical_columns_inferred = connector.get_stringtype_features(feature_table_name, trainer.label_column, trainer.entity_column, session=session)
            categorical_columns_config = [col for col in trainer.prep.categorical_pipeline['categorical_columns'] if col.upper() in feature_df.columns]
            categorical_columns = utils.merge_lists_to_unique(categorical_columns_config, categorical_columns_inferred)

            numeric_columns_inferred = connector.get_non_stringtype_features(feature_table_name, trainer.label_column, trainer.entity_column, session=session)
            numeric_columns_config = [col for col in trainer.prep.numeric_pipeline['numeric_columns'] if col.upper() in feature_df.columns]
            numeric_columns = utils.merge_lists_to_unique(numeric_columns_config, numeric_columns_inferred)

            train_x, test_x, test_y, pipe, model_id, metrics_df, results = trainer.train_model(feature_df, categorical_columns, numeric_columns, merged_config, model_file)

            column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
            column_name_file = connector.join_file_path(f"{trainer.output_profiles_ml_model}_{model_id}_column_names.json")
            json.dump(column_dict, open(column_name_file,"w"))

            trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
            connector.save_file(session, model_file, stage_name, overwrite=True)
            connector.save_file(session, column_name_file, stage_name, overwrite=True)
            trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
            try:
                figure_file = os.path.join('tmp', figure_names['feature-importance-chart'])
                shap_importance = utils.plot_top_k_feature_importance(pipe, train_x, numeric_columns, categorical_columns, figure_file, top_k_features=5)
                connector.write_pandas(shap_importance, f"FEATURE_IMPORTANCE", session=session, auto_create_table=True, overwrite=True)
                connector.save_file(session, figure_file, stage_name, overwrite=True)
            except Exception as e:
                logger.error(f"Could not generate plots {e}")

            connector.write_pandas(metrics_df, table_name=f"{metrics_table}", session=session, auto_create_table=True, overwrite=False)
            return results


    #TODO: Remove this and use from trainer.figure_names after support for other warehouses.
    figure_names = {"roc-auc-curve": f"01-test-roc-auc-{trainer.output_profiles_ml_model}.png",
                    "pr-auc-curve": f"02-test-pr-auc-{trainer.output_profiles_ml_model}.png",
                    "lift-chart": f"03-test-lift-chart-{trainer.output_profiles_ml_model}.png",
                    "feature-importance-chart": f"04-feature-importance-chart-{trainer.output_profiles_ml_model}.png"}
    
    logger.info("Getting past data for training")
    material_table = connector.get_material_registry_name(session, material_registry_table_prefix)

    model_hash, creation_ts = connector.get_latest_material_hash(session, material_table, trainer.features_profiles_model)
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

    feature_table_name_remote = f"LTV_TEST_FEATURES_2k_users"
    # sorted_feature_table = feature_table.sort(col(trainer.entity_column).asc(), col(trainer.index_timestamp).desc()).drop([trainer.index_timestamp])
    # sorted_feature_table.write.mode("overwrite").save_as_table(feature_table_name_remote)
    logger.info("Training and fetching the results")
    
    try:
        train_results_json = connector.call_procedure(train_procedure,
                                            feature_table_name_remote,
                                            figure_names,
                                            merged_config,
                                            session=session,
                                            connector=connector,
                                            trainer=trainer)
    except Exception as e:
        logger.error(f"Error while training the model: {e}")
        raise e
    
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
            "model_info": {'file_location': {'stage': stage_name, 'file_name': f"{trainer.output_profiles_ml_model}_{model_file_name}"}, 
                                        'model_id': model_id,
                                        "threshold": train_results['prob_th']},
            "input_model_name": trainer.features_profiles_model}

    json.dump(results, open(output_filename,"w"))

    model_timestamp = datetime.utcfromtimestamp(int(model_id)).strftime('%Y-%m-%dT%H:%M:%SZ')
    summary = trainer.prepare_training_summary(train_results, model_timestamp)
    json.dump(summary, open(os.path.join(target_path, 'training_summary.json'), "w"))
    logger.info("Fetching visualisations to local")
    for figure_name in figure_names.values():
        try:
            connector.fetch_staged_file(session, stage_name, figure_name, target_path)
        except:
            print(f"Could not fetch {figure_name}")

if __name__ == "__main__":
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
    connection_parameters = utils.remap_credentials(creds)
    session = Session.builder.configs(connection_parameters).create()
    inputs = None
    output_folder = 'output/dev/seq_no/7'
    output_file_name = f"{output_folder}/train_output.json"
    from pathlib import Path
    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)
       
    train(session, inputs, output_file_name, None, "snowflake")
