#!/usr/bin/env python
# coding: utf-8
import os
import json
import time
import yaml
import joblib
import pandas as pd

from typing import List
from pathlib import Path
from logger import logger
from datetime import datetime
from dataclasses import asdict

import snowflake.snowpark
from snowflake.snowpark.functions import sproc

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

import utils
import constants
from SnowflakeConnector import SnowflakeConnector
from MLTrainer import MLTrainer, ClassificationTrainer, RegressionTrainer

warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

try:
    from RedshiftConnector import RedshiftConnector
except Exception as e:
        logger.warning(f"Could not import RedshiftConnector")

metrics_table = constants.METRICS_TABLE
model_file_name = constants.MODEL_FILE_NAME
stage_name = constants.STAGE_NAME

def train_model(trainer: MLTrainer, feature_df: pd.DataFrame,
                categorical_columns: List[str], numeric_columns: List[str], merged_config: dict, model_file: str):
    """Creates and saves the trained model pipeline after performing preprocessing and classification
    and returns the various variables required for further processing by training procesudres/functions.

    Args:
        train (MLTrainer): MLTrainer object which has all the configs and methods required for training
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
    train_config = merged_config['train']
    models = train_config["model_params"]["models"]
    model_id = str(int(time.time()))

    train_x, train_y, test_x, test_y, val_x, val_y = utils.split_train_test(feature_df=feature_df,
                                                                            label_column=trainer.label_column,
                                                                            entity_column=trainer.entity_column,
                                                                            train_size=trainer.prep.train_size,
                                                                            val_size=trainer.prep.val_size,
                                                                            test_size=trainer.prep.test_size,
                                                                            isStratify=True)

    train_x = utils.transform_null(train_x, numeric_columns, categorical_columns)
    val_x = utils.transform_null(val_x, numeric_columns, categorical_columns)

    preprocessor_pipe_x = trainer.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer.prep.numeric_pipeline.get("pipeline"), trainer.prep.categorical_pipeline.get("pipeline"))
    train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
    val_x_processed = preprocessor_pipe_x.transform(val_x)

    final_model = trainer.select_best_model(models, train_x_processed, train_y, val_x_processed, val_y)
    preprocessor_pipe_optimized = trainer.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer.prep.numeric_pipeline.get("pipeline"), trainer.prep.categorical_pipeline.get("pipeline"))
    pipe = trainer.get_model_pipeline(preprocessor_pipe_optimized, final_model)
    pipe.fit(train_x, train_y)

    joblib.dump(pipe, model_file)

    results = trainer.get_metrics(pipe, train_x, train_y, test_x, test_y, val_x, val_y, train_config)
    results["model_id"] = model_id
    metrics_df = pd.DataFrame({'model_id': [results["model_id"]],
                            'metrics': [results["metrics"]],
                            'output_model_name': [results["output_model_name"]]}).reset_index(drop=True)

    return train_x, test_x, test_y, pipe, model_id, metrics_df, results

def train_and_store_model_results_rs(feature_table_name: str,
            figure_names: dict,
            merged_config: dict, **kwargs) -> dict:
    """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

    Args:
        feature_table_name (str): name of the user feature table generated by profiles feature table model, and is input to training and prediction
        figure_names (dict): A dict with the file names to be generated as its values, and the keys as the names of the figures.
        merged_config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file
        From kwargs:
            session (redshift_connector.cursor.Cursor): valid redshift session to access data warehouse
            connector (RedshiftConnector): RedshiftConnector object
            trainer (MLTrainer): MLTrainer object which has all the configs and methods required for training

    Returns:
        dict: returns the model_id which is basically the time converted to key at which results were generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
    """
    local_folder = constants.LOCAL_STORAGE_DIR
    session = kwargs.get("session")
    connector = kwargs.get("connector")
    trainer = kwargs.get("trainer")
    if session == None or connector == None or trainer == None:
        raise ValueError("session, connector and trainer are required in kwargs for training in Redshift")
    model_file = connector.join_file_path(f"{trainer.output_profiles_ml_model}_{model_file_name}")
    feature_df_path = os.path.join(local_folder, f"{feature_table_name}.parquet.gzip")
    feature_df = pd.read_parquet(feature_df_path)
    feature_df.columns = [col.upper() for col in feature_df.columns]

    stringtype_features = connector.get_stringtype_features(feature_df, trainer.label_column, trainer.entity_column, session=session)
    categorical_columns = utils.merge_lists_to_unique(trainer.prep.categorical_pipeline['categorical_columns'], stringtype_features)

    non_stringtype_features = connector.get_non_stringtype_features(feature_df, trainer.label_column, trainer.entity_column, session=session)
    numeric_columns = utils.merge_lists_to_unique(trainer.prep.numeric_pipeline['numeric_columns'], non_stringtype_features)
    
    train_x, test_x, test_y, pipe, model_id, metrics_df, results = train_model(trainer, feature_df, categorical_columns, numeric_columns, merged_config, model_file)

    column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
    column_name_file = connector.join_file_path(f"{trainer.output_profiles_ml_model}_{model_id}_column_names.json")
    json.dump(column_dict, open(column_name_file,"w"))

    trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
    trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
    try:
        figure_file = connector.join_file_path(figure_names['feature-importance-chart'])
        shap_importance = utils.plot_top_k_feature_importance(pipe, train_x, numeric_columns, categorical_columns, figure_file, top_k_features=5)
        connector.write_pandas(shap_importance, f"FEATURE_IMPORTANCE", if_exists="replace")
    except Exception as e:
        logger.error(f"Could not generate plots {e}")

    database_dtypes = json.loads(constants.rs_dtypes)
    metrics_table_query = ""
    for col in metrics_df.columns:
        if metrics_df[col].dtype == 'object':
            metrics_df[col] = metrics_df[col].apply(lambda x: json.dumps(x))
            metrics_table_query += f"{col} {database_dtypes['text']},"
        elif metrics_df[col].dtype == 'float64' or metrics_df[col].dtype == 'int64':
            metrics_table_query += f"{col} {database_dtypes['num']},"
        elif metrics_df[col].dtype == 'bool':
            metrics_table_query += f"{col} {database_dtypes['bool']},"
        elif metrics_df[col].dtype == 'datetime64[ns]':
            metrics_table_query += f"{col} {database_dtypes['timestamp']},"
    metrics_table_query = metrics_table_query[:-1]
        
    connector.run_query(session, f"CREATE TABLE IF NOT EXISTS {metrics_table} ({metrics_table_query});")
    connector.write_pandas(metrics_df, f"{metrics_table}", if_exists="append")
    return results

def train(creds: dict, inputs: str, output_filename: str, config: dict, site_config_path: str=None, project_folder: str=None) -> None:
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
    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX
    positive_boolean_flags = constants.POSITIVE_BOOLEAN_FLAGS
    cardinal_feature_threshold = constants.CARDINAL_FEATURE_THRESOLD

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
    warehouse = creds['type']
    logger.info(f"Building session for {warehouse}")
    if warehouse == 'snowflake':
        train_procedure = 'train_and_store_model_results_sf'
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
        connector.create_stage(session, stage_name)
        connector.delete_import_files(session, stage_name, import_paths)
        connector.delete_procedures(session)

        @sproc(name=train_procedure, is_permanent=True, stage_location=stage_name, replace=True, imports= [current_dir]+import_paths, 
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
            stringtype_features = connector.get_stringtype_features(feature_table_name, trainer.label_column, trainer.entity_column, session=session)
            categorical_columns = utils.merge_lists_to_unique(trainer.prep.categorical_pipeline['categorical_columns'], stringtype_features)

            non_stringtype_features = connector.get_non_stringtype_features(feature_table_name, trainer.label_column, trainer.entity_column, session=session)
            numeric_columns = utils.merge_lists_to_unique(trainer.prep.numeric_pipeline['numeric_columns'], non_stringtype_features)

            train_x, test_x, test_y, pipe, model_id, metrics_df, results = train_model(trainer, feature_df, categorical_columns, numeric_columns, merged_config, model_file)

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
    elif warehouse == 'redshift':
        train_procedure = train_and_store_model_results_rs
        connector = RedshiftConnector()
        session = connector.build_session(creds)

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
    try:
        material_names, training_dates = connector.get_material_names(session, material_table, start_date, end_date, 
                                                                trainer.package_name, 
                                                                trainer.features_profiles_model, 
                                                                model_hash, 
                                                                material_table_prefix, 
                                                                trainer.prediction_horizon_days,
                                                                output_filename,
                                                                site_config_path,
                                                                project_folder,
                                                                trainer.inputs)
    except TypeError:
        raise Exception("Unable to fetch past material data. Ensure pb setup is correct and the profiles paths are setup correctly")

    if trainer.label_value is None and prediction_task == 'classification':
        label_value = connector.get_default_label_value(session, material_names[0][0], trainer.label_column, positive_boolean_flags)
        trainer.label_value = label_value

    feature_table = None
    for row in material_names:
        feature_table_name, label_table_name = row
        feature_table_instance = trainer.prepare_feature_table(connector, session,
                                                               feature_table_name, 
                                                               label_table_name,
                                                               cardinal_feature_threshold)
        if feature_table is None:
            feature_table = feature_table_instance
        else:
            feature_table = feature_table.unionAllByName(feature_table_instance)

    feature_table_name_remote = f"{trainer.output_profiles_ml_model}_features"
    filtered_feature_table = connector.filter_feature_table(feature_table, trainer.entity_column, trainer.index_timestamp, trainer.max_row_count)
    connector.write_table(filtered_feature_table, feature_table_name_remote, write_mode="overwrite", if_exists="replace")
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
    if not isinstance(train_results_json, dict):
        train_results = json.loads(train_results_json)
    else:
        train_results = train_results_json
    model_id = train_results["model_id"]
    
    results = {"config": {'training_dates': training_dates,
                        'material_names': material_names,
                        'material_hash': model_hash,
                        **asdict(trainer)},
            "model_info": {'file_location': {'stage': stage_name, 
                                             'file_name': f"{trainer.output_profiles_ml_model}_{model_file_name}"}, 
                                             'model_id': model_id,
                                             "threshold": train_results['prob_th']},
            "input_model_name": trainer.features_profiles_model}
    json.dump(results, open(output_filename,"w"))

    model_timestamp = datetime.utcfromtimestamp(int(model_id)).strftime('%Y-%m-%dT%H:%M:%SZ')
    summary = trainer.prepare_training_summary(train_results, model_timestamp)
    json.dump(summary, open(os.path.join(target_path, 'training_summary.json'), "w"))
    logger.debug("Fetching visualisations to local")
    for figure_name in figure_names.values():
        try:
            connector.fetch_staged_file(session, stage_name, figure_name, target_path)
        except:
            print(f"Could not fetch {figure_name}")

if __name__ == "__main__":
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
        # creds = yaml.safe_load(f)["connections"]["dev_wh_rs"]["outputs"]["dev"]
    inputs = None
    output_folder = 'output/dev/seq_no/7'
    output_file_name = f"{output_folder}/train_output.json"
    siteconfig_path = os.path.join(homedir, ".pb/siteconfig.yaml")

    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)
    # logger.setLevel(logger.logging.DEBUG)

    project_folder = '~/Desktop/Git_repos/rudderstack-profiles-shopify-churn'    #change path of project directory as per your system
       
    train(creds, inputs, output_file_name, None, siteconfig_path, project_folder)
