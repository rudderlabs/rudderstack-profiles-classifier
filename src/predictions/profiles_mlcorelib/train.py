#!/usr/bin/env python
# coding: utf-8
import os
import json
import sys

from .wht.pythonWHT import PythonWHT

from functools import partial
from .processors.ProcessorFactory import ProcessorFactory

from .utils.logger import logger
from datetime import datetime, timezone
from dataclasses import asdict

import snowflake.snowpark
from snowflake.snowpark.functions import sproc

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

# Below lines make relative imports work in snowpark stored procedures
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from .utils import utils
from .utils import constants

from .connectors.ConnectorFactory import ConnectorFactory
from .trainers.TrainerFactory import TrainerFactory
from .ml_core.preprocess_and_train import train_and_store_model_results
from typing import List
from pathlib import Path


warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

model_file_name = constants.MODEL_FILE_NAME


def _train(
    creds: dict,
    inputs: List[dict],
    output_filename: str,
    config: dict,
    site_config_path: str,
    runtime_info: dict,
    whtService: PythonWHT,
    ml_core_path: str,
    metrics_table: str,
    material_directory=None,
) -> None:
    """Trains the model and saves the model with given output_filename.

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        inputs (List[dict]): list of input models
        output_filename (str): path to the file where the model details including model id etc are written. Used in prediction step.
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

    Raises:
        ValueError: If num_params_name is invalid for numeric pipeline
        ValueError: If cat_params_name is invalid for catagorical pipeline

    Returns:
        None: saves the model but returns nothing
    """
    is_rudder_backend = utils.fetch_key_from_dict(
        runtime_info, "is_rudder_backend", False
    )
    stage_name = None

    config_path = utils.get_model_configs_file_path()
    if material_directory is not None:
        # Pynative model
        folder_path = material_directory
        reports_directory = Path(material_directory, f"training_reports")
        reports_directory.mkdir(parents=True, exist_ok=True)
        # should be converted into string since snowpark doesn't support "Path" object
        reports_directory = str(reports_directory)
    else:
        # Python model
        folder_path = os.path.dirname(output_filename)
        reports_directory = utils.get_output_directory(folder_path)

    """ Initialising trainer """
    logger.get().info("Initialising trainer")
    default_config = utils.load_yaml(config_path)
    _ = config["data"].pop(
        "package_name", None
    )  # For backward compatibility. Not using it anywhere else, hence deleting.
    _ = config["data"].pop(
        "features_profiles_model", None
    )  # For backward compatibility. Not using it anywhere else, hence deleting.
    _ = config["data"].pop(
        "inputs", None
    )  # For backward compatibility. Not using it anywhere else, hence deleting.
    _ = config["data"].pop(
        "output_profiles_ml_model", None
    )  # For backward compatibility. It is a constant in case of python model and fetching it from wht context in case of pyNative.

    connector = ConnectorFactory.create(creds, folder_path)
    whtService.set_connector(connector)

    merged_config = utils.combine_config(default_config, config)
    merged_config = whtService.update_config_info(merged_config)
    whtService.validate_sql_table(inputs, merged_config["data"]["entity_column"])

    user_preference_order_infra = merged_config["data"].pop(
        "user_preference_order_infra", None
    )

    (
        model_hash,
        entity_var_model_name,
        creation_ts,
    ) = whtService.get_latest_entity_var_table(
        merged_config["data"]["entity_key"],
    )

    trainer = TrainerFactory.create(merged_config, connector, entity_var_model_name)

    logger.get().debug(
        f"Started training for {trainer.output_profiles_ml_model} to predict {trainer.label_column}"
    )
    if trainer.eligible_users:
        logger.get().debug(
            f"Only following users are considered for training: {trainer.eligible_users}"
        )
    else:
        logger.get().debug(
            "Consider shortlisting the users through eligible_users flag to get better results for a specific user group - such as payers only, monthly active users etc."
        )
    pkl_model_file_name = f"{trainer.output_profiles_ml_model}_{model_file_name}"

    """ Building session """
    warehouse = creds["type"]
    logger.get().debug(f"Building session for {warehouse}")
    if warehouse == "snowflake":
        stage_name = connector.stage_name
        train_procedure = connector.stored_procedure_name
        import_paths = connector.delete_files

        connector.create_stage()
        connector.pre_job_cleanup()

        new_session = connector.session

        # This is to avoid the pickling error in snowpark - TypeError: cannot pickle '_thread.lock' object
        # The implication of this is that the "self.session" is not available in Snowpark stored procedures
        connector.session = None

        @sproc(
            name=train_procedure,
            is_permanent=False,
            stage_location=stage_name,
            replace=True,
            imports=import_paths,
            packages=constants.SNOWFLAKE_TRAINING_PACKAGES,
            # Pass session object to avoid the error - More than one active session is detected
            # This error is because pywht is also creating a session object
            session=new_session,
        )

        # This function is called from connector.call_procedure in preprocess_and_train.py
        def train_and_store_model_results_sf(
            session: snowflake.snowpark.Session,
            feature_table_name: str,
            input_column_types: dict,
            train_config: dict,
            metrics_table: str,
        ) -> dict:
            """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

            Args:
                session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
                feature_table_name (str): name of the user feature table generated by profiles feature table model,
                    and is input to training and prediction
                train_config (dict): configs from profiles.yaml which should overwrite corresponding values
                    from model_configs.yaml file
\
            Returns:
                dict: returns the model_id which is basically the time converted to key at which results were
                    generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
            """

            feature_df = connector.get_table_as_dataframe(session, feature_table_name)

            model_file = connector.join_file_path(pkl_model_file_name)

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
                input_column_types,
                train_config,
                model_file,
            )

            trainer.plot_diagnostics(
                connector,
                session,
                pipe,
                stage_name,
                test_x,
                test_y,
                trainer.label_column,
            )

            model_file = model_file + ".pkl"
            connector.save_file(session, model_file, stage_name, overwrite=True)

            try:
                figure_file = os.path.join(
                    "tmp", trainer.figure_names["feature-importance-chart"]
                )
                # Can't use logger inside snowpark stored procedures since snowpark can't pickle it
                print(f"Generating feature importance plot")
                utils.plot_top_k_feature_importance(
                    pipe,
                    train_x,
                    figure_file,
                    top_k_features=20,
                )
                connector.save_file(session, figure_file, stage_name, overwrite=True)
            except Exception as e:
                print(f"Could not generate plots {e}")

            connector.write_pandas(
                metrics_df,
                table_name=f"{metrics_table}",
                session=session,
                auto_create_table=True,
                overwrite=False,
            )
            return results

        # Recomputing the session object since it was reset
        connector.session = new_session

    elif warehouse in ("redshift", "bigquery"):
        train_procedure = train_and_store_model_results
        connector.delete_local_data_folder()
        connector.make_local_dir()

    latest_seq_no = whtService.get_latest_seq_no(inputs)
    latest_entity_var_table = whtService.compute_material_name(
        entity_var_model_name, model_hash, latest_seq_no
    )

    start_date, end_date = whtService.get_date_range(
        creation_ts, trainer.prediction_horizon_days
    )

    logger.get().info(
        f"Getting input column types from table: {latest_entity_var_table}"
    )

    input_columns = connector.get_input_columns(trainer, inputs)

    joined_input_table_name = f"{connector.feature_table_name}_for_training"
    connector.join_input_tables(
        inputs, input_columns, trainer.entity_column, joined_input_table_name
    )

    input_column_types = connector.get_input_column_types(
        trainer,
        input_columns,
        inputs,
        joined_input_table_name,
    )
    logger.get().debug(f"Input column types detected: {input_column_types}")

    try:
        feature_data_min_date_diff = trainer.feature_data_min_date_diff
    except AttributeError:
        # Default feature dates minimum difference
        feature_data_min_date_diff = 3

    logger.get().info("Getting past data for training")
    get_material_names_partial = partial(
        whtService.get_material_names,
        end_date=end_date,
        prediction_horizon_days=trainer.prediction_horizon_days,
        inputs=inputs,
        input_columns=input_columns,
        entity_column=trainer.entity_column,
        feature_data_min_date_diff=feature_data_min_date_diff,
    )
    # material_names, training_dates
    train_table_pairs = get_material_names_partial(start_date=start_date)
    # Generate new materials for training data
    try:
        train_table_pairs = trainer.check_and_generate_more_materials(
            get_material_names_partial,
            train_table_pairs,
            inputs,
            whtService,
            connector,
        )
    except Exception as e:
        logger.get().error(f"Error while generating new materials, {str(e)}")

    logger.get().info(
        f"Training data fetched successfully. Train Table Pairs: {train_table_pairs}"
    )

    mode = connector.fetch_processor_mode(
        user_preference_order_infra, is_rudder_backend
    )
    processor = ProcessorFactory.create(mode, trainer, connector, ml_core_path)
    logger.get().debug(f"Using {mode} processor for training")
    train_results = processor.train(
        train_procedure,
        train_table_pairs,
        merged_config,
        input_column_types,
        input_columns,
        metrics_table,
        creds,
        utils.load_yaml(site_config_path),
        pkl_model_file_name,
    )
    logger.get().debug("Training completed. Saving the artefacts")

    logger.get().info("Saving train results to file")
    model_id = train_results["model_id"]

    training_dates_ = []
    material_names_ = []
    past_joined_input_tables_set = set()
    for train_table_pair_ in train_table_pairs:
        material_name_pair = [
            train_table_pair_.feature_table_name,
            train_table_pair_.label_table_name,
        ]
        material_names_.append(material_name_pair)
        past_joined_input_tables_set.update(material_name_pair)

        material_date_pair = [
            train_table_pair_.feature_table_date,
            train_table_pair_.label_table_date,
        ]
        training_dates_.append(material_date_pair)

    results = {
        "config": {
            "training_dates": training_dates_,
            "material_names": material_names_,
            "entity_var_model_hash": model_hash,
            **asdict(trainer),
            "entity_var_model_name": entity_var_model_name,
        },
        "model_info": {
            "model_name": train_results["model_class_name"],
            "file_location": {
                "stage": stage_name,
                "file_name": pkl_model_file_name,
            },
            "model_id": model_id,
            "threshold": 0,
        },
        "column_names": train_results["column_names"],
    }
    json.dump(results, open(output_filename, "w"))

    model_timestamp = datetime.fromtimestamp(int(model_id), tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    summary = trainer.prepare_training_summary(train_results, model_timestamp)
    json.dump(
        summary, open(os.path.join(reports_directory, "training_summary.json"), "w")
    )
    logger.get().debug("Fetching visualisations to local")

    for figure_name in trainer.figure_names.values():
        try:
            connector.fetch_staged_file(stage_name, figure_name, reports_directory)
        except Exception as e:
            logger.get().error(f"Could not fetch {figure_name} {e}")

    logger.get().debug("Cleaning up the training session")
    connector.drop_joined_tables(
        table_list=[joined_input_table_name] + list(past_joined_input_tables_set)
    )
    connector.post_job_cleanup()
    logger.get().debug("Training completed")
