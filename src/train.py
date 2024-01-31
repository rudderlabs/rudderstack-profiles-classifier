#!/usr/bin/env python
# coding: utf-8
import os
import json
import yaml
import pandas as pd
import sys
import hashlib

import src.utils.logger as logger
from datetime import datetime
from dataclasses import asdict

import snowflake.snowpark
from snowflake.snowpark.functions import sproc

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

# Below lines make relative imports work in snowpark stored procedures
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import src.utils.utils as utils 
from src.constants import constants

from src.processors.AWSProcessor import AWSProcessor
from src.processors.LocalProcessor import LocalProcessor
from src.processors.SnowflakeProcessor import SnowflakeProcessor
from src.connectors.SnowflakeConnector import SnowflakeConnector
from src.trainers.MLTrainer import ClassificationTrainer, RegressionTrainer
from src.utils.preprocess_and_train import train_and_store_model_results_rs


warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

try:
    from connectors.RedshiftConnector import RedshiftConnector
except Exception as e:
    logger.warning(f"Could not import RedshiftConnector")

metrics_table = constants.METRICS_TABLE
model_file_name = constants.MODEL_FILE_NAME

def train(
    creds: dict,
    inputs: str,
    output_filename: str,
    config: dict,
    site_config_path: str = None,
    project_folder: str = None,
    runtime_info: dict = None,
) -> None:
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
    is_rudder_backend = utils.fetch_key_from_dict(
        runtime_info, "is_rudder_backend", False
    )
    stage_name = None

    current_dir = os.path.dirname(os.path.abspath(__file__))
    processor_mode_map = {
        "local": LocalProcessor,
        "native-warehouse": SnowflakeProcessor,
        "rudderstack-infra": AWSProcessor,
    }
    import_files = (
        "utils/",
        "constants/",
        "trainers/",
        "connectors/",
        "processors/",
    )
    import_paths = [current_dir]
    # for file in import_files:
    #     import_paths.append(os.path.join(current_dir, file))
    config_path = os.path.join(current_dir, "config", "model_configs.yaml")
    folder_path = os.path.dirname(output_filename)
    target_path = utils.get_output_directory(folder_path)

    """ Initialising trainer """
    logger.info("Initialising trainer")
    default_config = utils.load_yaml(config_path)
    _ = config["data"].pop("package_name", None) # For backward compatibility. Not using it anywhere else, hence deleting.
    _ = config["data"].pop("features_profiles_model", None) # For backward compatibility. Not using it anywhere else, hence deleting.
    merged_config = utils.combine_config(default_config, config)

    user_preference_order_infra = merged_config["data"].pop(
        "user_preference_order_infra", None
    )
    prediction_task = merged_config["data"].pop(
        "task", "classification"
    )  # Assuming default as classification

    prep_config = utils.PreprocessorConfig(**merged_config["preprocessing"])
    if prediction_task == "classification":
        trainer = ClassificationTrainer(**merged_config["data"], prep=prep_config)
    elif prediction_task == "regression":
        trainer = RegressionTrainer(**merged_config["data"], prep=prep_config)

    logger.debug(
        f"Started training for {trainer.output_profiles_ml_model} to predict {trainer.label_column}"
    )
    if trainer.eligible_users:
        logger.debug(
            f"Only following users are considered for training: {trainer.eligible_users}"
        )
    else:
        logger.debug(
            "Consider shortlisting the users through eligible_users flag to get better results for a specific user group - such as payers only, monthly active users etc."
        )

    """ Building session """
    warehouse = creds["type"]
    logger.debug(f"Building session for {warehouse}")
    if warehouse == "snowflake":
        run_id = hashlib.md5(
            f"{str(datetime.now())}_{project_folder}".encode()
        ).hexdigest()
        stage_name = f"@rs_{run_id}"
        train_procedure = f"train_and_store_model_results_sf_{run_id}"
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
        connector.create_stage(session, stage_name)
        connector.cleanup(
            session,
            stage_name=stage_name,
            stored_procedure_name=train_procedure,
            delete_files=import_paths,
        )

        @sproc(name=train_procedure, is_permanent=False, stage_location=stage_name, replace=True, imports=import_paths, 
            packages=["snowflake-snowpark-python>=0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "joblib==1.2.0", "PyYAML", "numpy==1.23.1", "pandas==1.4.3", "hyperopt", "shap==0.41.0", "matplotlib==3.7.1", "seaborn==0.12.0", "scikit-plot==0.3.7"])
        def train_and_store_model_results_sf(session: snowflake.snowpark.Session,
                    feature_table_name: str,
                    merged_config: dict) -> dict:
            """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

            Args:
                session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
                feature_table_name (str): name of the user feature table generated by profiles feature table model,
                    and is input to training and prediction
                merged_config (dict): configs from profiles.yaml which should overwrite corresponding values
                    from model_configs.yaml file

            Returns:
                dict: returns the model_id which is basically the time converted to key at which results were
                    generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
            """
            feature_df = connector.get_table_as_dataframe(session, feature_table_name)

            model_file = connector.join_file_path(
                f"{trainer.output_profiles_ml_model}_{model_file_name}"
            )
            categorical_columns_inferred = connector.get_stringtype_features(
                feature_table_name,
                trainer.label_column,
                trainer.entity_column,
                session=session,
            )
            categorical_columns_config = [
                col
                for col in trainer.prep.categorical_pipeline["categorical_columns"]
                if col.upper() in feature_df.columns
            ]
            categorical_columns = utils.merge_lists_to_unique(
                categorical_columns_config, categorical_columns_inferred
            )

            numeric_columns_inferred = connector.get_non_stringtype_features(
                feature_table_name,
                trainer.label_column,
                trainer.entity_column,
                session=session,
            )
            numeric_columns_config = [
                col
                for col in trainer.prep.numeric_pipeline["numeric_columns"]
                if col.upper() in feature_df.columns
            ]
            numeric_columns = utils.merge_lists_to_unique(
                numeric_columns_config, numeric_columns_inferred
            )

            (
                train_x,
                test_x,
                test_y,
                pipe,
                model_id,
                metrics_df,
                results,
            ) = trainer.train_model(
                feature_df,
                categorical_columns,
                numeric_columns,
                merged_config,
                model_file,
            )

            results['column_names'] = {
                'numeric_columns': numeric_columns,
                'categorical_columns': categorical_columns,
                'arraytype_columns' : [],
                'timestamp_columns' : []
            }


            trainer.plot_diagnostics(
                connector,
                session,
                pipe,
                stage_name,
                test_x,
                test_y,
                trainer.label_column,
            )

            connector.save_file(session, model_file, stage_name, overwrite=True)
       
            try:
                figure_file = os.path.join(
                    "tmp", trainer.figure_names["feature-importance-chart"]
                )
                shap_importance = utils.plot_top_k_feature_importance(
                    pipe,
                    train_x,
                    numeric_columns,
                    categorical_columns,
                    figure_file,
                    top_k_features=5,
                )
                connector.write_pandas(
                    shap_importance,
                    "FEATURE_IMPORTANCE",
                    session=session,
                    auto_create_table=True,
                    overwrite=True,
                )
                connector.save_file(session, figure_file, stage_name, overwrite=True)
            except Exception as e:
                logger.error(f"Could not generate plots {e}")

            connector.write_pandas(
                metrics_df,
                table_name=f"{metrics_table}",
                session=session,
                auto_create_table=True,
                overwrite=False,
            )
            return results

    elif warehouse == "redshift":
        train_procedure = train_and_store_model_results_rs
        connector = RedshiftConnector(folder_path)
        session = connector.build_session(creds)
        connector.cleanup(delete_local_data=True)
        connector.make_local_dir()

    material_table = connector.get_material_registry_name(
        session, material_registry_table_prefix
    )

    model_hash, features_profiles_model = connector.get_latest_material_hash(
        trainer.entity_key,
        constants.VAR_TABLE_SUFFIX,
        output_filename,
        site_config_path,
        project_folder,
    )

    creation_ts = connector.get_creation_ts(
        session,
        material_table,
        features_profiles_model,
        model_hash,
        trainer.entity_key,
    )

    start_date, end_date = trainer.train_start_dt, trainer.train_end_dt

    if start_date is None or end_date is None:
        start_date, end_date = utils.get_date_range(
            creation_ts, trainer.prediction_horizon_days
        )

    logger.info("Getting past data for training")
    try:
        #material_names, training_dates 
        train_table_pairs = connector.get_material_names(
            session,
            material_table,
            start_date,
            end_date,
            features_profiles_model,
            model_hash,
            material_table_prefix,
            trainer.prediction_horizon_days,
            output_filename,
            site_config_path,
            project_folder,
            trainer.inputs,
            inputs,
        )
    except TypeError:
        raise Exception(
            "Unable to fetch past material data. Ensure pb setup is correct and the profiles paths are setup correctly"
        )

    if trainer.label_value is None and prediction_task == "classification":
        sample_material_ = train_table_pairs[0]
        label_value = connector.get_default_label_value(
            session, sample_material_.label_table_name, trainer.label_column, positive_boolean_flags
        )
        trainer.label_value = label_value

    mode = connector.fetch_processor_mode(
        user_preference_order_infra, is_rudder_backend
    )
    processor = processor_mode_map[mode](trainer, connector, session)
    logger.debug(f"Using {mode} processor for training")
    train_results = processor.train(train_procedure, train_table_pairs, merged_config, prediction_task, creds)
    logger.debug("Training completed. Saving the artefacts")

    logger.info("Saving train results to file")
    model_id = train_results["model_id"]

    training_dates_ = []
    material_names_ = []
    for train_table_pair_ in train_table_pairs:
        material_names_.append([train_table_pair_.feature_table_name, train_table_pair_.label_table_name])
        training_dates_.append([train_table_pair_.feature_table_date, train_table_pair_.label_table_date])

    results = {
        "config": {
            "training_dates": training_dates_,
            "material_names": material_names_,
            "material_hash": model_hash,
            **asdict(trainer),
            "input_model_name": features_profiles_model,
        },
        "model_info": {
            "file_location": {
                "stage": stage_name,
                "file_name": f"{trainer.output_profiles_ml_model}_{model_file_name}",
            },
            "model_id": model_id,
            "threshold": train_results["prob_th"],
        },
        "column_names": train_results["column_names"]
    }
    json.dump(results, open(output_filename, "w"))

    model_timestamp = datetime.utcfromtimestamp(int(model_id)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    summary = trainer.prepare_training_summary(train_results, model_timestamp)
    json.dump(summary, open(os.path.join(target_path, "training_summary.json"), "w"))
    logger.debug("Fetching visualisations to local")

    for figure_name in trainer.figure_names.values():
        try:
            connector.fetch_staged_file(session, stage_name, figure_name, target_path)
        except:
            logger.warning(f"Could not fetch {figure_name}")
            
    logger.debug("Cleaning up the training session")
    connector.cleanup(
        session,
        stored_procedure_name=train_procedure,
        delete_files=import_paths,
        stage_name=stage_name,
        close_session=True
    )
    logger.debug("Training completed")
    
