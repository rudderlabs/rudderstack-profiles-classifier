import os
import sys
import json
import logging
import pandas as pd
from typing import List

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

from ..utils import utils
from ..utils.logger import logger
from ..utils import constants
from ..utils.S3Utils import S3Utils

from ..trainers.MLTrainer import ClassificationTrainer, RegressionTrainer
from ..connectors.ConnectorFactory import ConnectorFactory

metrics_table = constants.METRICS_TABLE
model_file_name = constants.MODEL_FILE_NAME


def train_and_store_model_results(
    feature_df: pd.DataFrame,
    train_config: dict,
    feature_table_column_types: dict,
    **kwargs,
) -> dict:
    """Creates and saves the trained model pipeline after performing preprocessing and classification and
    returns the model id attached with the results generated.

    Args:
        feature_df (pd.DataFrame): feature table generated by profiles feature table model,
        and is input to training and prediction train_config (dict): configs from profiles.yaml which should
        overwrite corresponding values from model_configs.yaml file
        From kwargs:
            session (redshift_connector.cursor.Cursor): valid redshift session to access data warehouse
            connector (RedshiftConnector): RedshiftConnector object
            trainer (MLTrainer): MLTrainer object which has all the configs and methods required for training

    Returns:
        dict: returns the model_id which is basically the time converted to key at which results were generated
        along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
    """
    session = kwargs.get("session")
    connector = kwargs.get("connector")
    trainer = kwargs.get("trainer")
    if session is None or connector is None or trainer is None:
        raise ValueError(
            "session, connector and trainer are required in kwargs for training in Redshift"
        )
    numeric_columns = feature_table_column_types["numeric"]
    categorical_columns = feature_table_column_types["categorical"]

    model_file = connector.join_file_path(
        f"{trainer.output_profiles_ml_model}_{model_file_name}"
    )
    feature_df.columns = [col.upper() for col in feature_df.columns]

    train_x, test_x, test_y, pipe, model_id, metrics_df, results = trainer.train_model(
        feature_df, categorical_columns, numeric_columns, train_config, model_file
    )

    logger.info(f"Generating plots and saving them to the output directory.")
    trainer.plot_diagnostics(
        connector, session, pipe, None, test_x, test_y, trainer.label_column
    )
    figure_file = connector.join_file_path(
        trainer.figure_names["feature-importance-chart"]
    )
    shap_importance = utils.plot_top_k_feature_importance(
        pipe,
        train_x,
        numeric_columns,
        categorical_columns,
        figure_file,
        top_k_features=5,
    )
    connector.write_pandas(shap_importance, "FEATURE_IMPORTANCE", if_exists="replace")
    metrics_df, create_metrics_table_query = connector.fetch_create_metrics_table_query(
        metrics_df
    )
    connector.run_query(
        session,
        create_metrics_table_query,
        response=False,
    )
    connector.write_pandas(metrics_df, f"{metrics_table}", if_exists="append")
    return results


def prepare_feature_table(
    train_table_pair: constants.TrainTablesInfo,
    timestamp_columns: List[str],
    **kwargs,
):
    """Combines Feature table and Label table pairs, while converting all timestamp cols
    to numeric for creating a single table with user features and label."""
    session = kwargs.get("session", None)
    connector = kwargs.get("connector", None)
    trainer = kwargs.get("trainer", None)
    try:
        feature_table_name = train_table_pair.feature_table_name
        label_table_name = train_table_pair.label_table_name
        feature_table_dt = utils.convert_ts_str_to_dt_str(
            train_table_pair.feature_table_date
        )
        if trainer.eligible_users:
            feature_table = connector.get_table(
                session, feature_table_name, filter_condition=trainer.eligible_users
            )
        else:
            default_user_shortlisting = (
                f"{trainer.label_column} != {trainer.label_value}"
            )
            feature_table = connector.get_table(
                session, feature_table_name, filter_condition=default_user_shortlisting
            )

        feature_table = connector.drop_cols(feature_table, [trainer.label_column])

        for col in timestamp_columns:
            feature_table = connector.add_days_diff(
                feature_table, col, col, feature_table_dt
            )
        label_table = trainer.prepare_label_table(connector, session, label_table_name)
        feature_table = connector.join_feature_table_label_table(
            feature_table, label_table, trainer.entity_column, "inner"
        )
        return feature_table
    except Exception as e:
        print(
            "Exception occured while preparing feature table. Please check the logs for more details"
        )
        raise e


def preprocess_and_train(
    train_procedure,
    train_table_pairs: List[constants.TrainTablesInfo],
    merged_config: dict,
    input_column_types: dict,
    **kwargs,
):
    session = kwargs.get("session", None)
    connector = kwargs.get("connector", None)
    trainer = kwargs.get("trainer", None)
    min_sample_for_training = constants.MIN_SAMPLES_FOR_TRAINING
    cardinal_feature_threshold = constants.CARDINAL_FEATURE_THRESOLD
    train_config = merged_config["train"]

    feature_table = None
    for train_table_pair in train_table_pairs:
        logger.info(
            f"Preparing training dataset using {train_table_pair.feature_table_name} and {train_table_pair.label_table_name} as feature and label tables respectively"
        )
        feature_table_instance = prepare_feature_table(
            train_table_pair,
            input_column_types["timestamp"],
            session=session,
            connector=connector,
            trainer=trainer,
        )
        feature_table = connector.get_merged_table(
            feature_table, feature_table_instance
        )
        break

    high_cardinal_features = connector.get_high_cardinal_features(
        feature_table,
        input_column_types["categorical"],
        trainer.label_column,
        trainer.entity_column,
        cardinal_feature_threshold,
    )
    logger.debug(f"High cardinality features detected: {high_cardinal_features}")

    ignore_features = utils.get_all_ignore_features(
        feature_table,
        input_column_types,
        trainer.prep.ignore_features,
        high_cardinal_features,
    )
    logger.debug(f"Ignore features detected: {ignore_features}")
    feature_table = connector.drop_cols(feature_table, ignore_features)

    feature_table_column_types = utils.get_feature_table_column_types(
        feature_table, input_column_types, trainer.label_column, trainer.entity_column
    )
    logger.debug(f"Feature_table column types detected: {feature_table_column_types}")

    task_type = trainer.get_name()
    logger.info(f"Performing data validation for {task_type}")

    trainer.validate_data(connector, feature_table)
    logger.info("Data validation is completed")

    filtered_feature_table = connector.filter_feature_table(
        feature_table,
        trainer.entity_column,
        trainer.max_row_count,
        min_sample_for_training,
    )
    connector.send_table_to_train_env(
        filtered_feature_table,
        write_mode="overwrite",
        if_exists="replace",
    )

    logger.info("Training and fetching the results")
    try:
        train_results_json = connector.call_procedure(
            train_procedure,
            filtered_feature_table,
            train_config,
            feature_table_column_types,
            session=session,
            connector=connector,
            trainer=trainer,
        )
    except Exception as e:
        logger.error(f"Error while training the model: {e}")
        raise e

    if not isinstance(train_results_json, dict):
        train_results_json = json.loads(train_results_json)

    logger.info("Saving column names info. to the output json.")
    train_results_json["column_names"] = {}
    train_results_json["column_names"]["input_column_types"] = input_column_types
    train_results_json["column_names"]["ignore_features"] = ignore_features
    train_results_json["column_names"][
        "feature_table_column_types"
    ] = feature_table_column_types

    return train_results_json


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--s3_bucket", type=str)
    parser.add_argument("--aws_region_name", type=str)
    parser.add_argument("--s3_path", type=str)
    parser.add_argument("--ec2_temp_output_json", type=str)
    parser.add_argument("--material_names", type=json.loads)
    parser.add_argument("--merged_config", type=json.loads)
    parser.add_argument("--input_column_types", type=json.loads)
    parser.add_argument("--prediction_task", type=str)
    parser.add_argument("--wh_creds", type=json.loads)
    parser.add_argument("--output_path", type=str)
    parser.add_argument("--mode", type=str)
    args = parser.parse_args()

    if args.mode == constants.CI_MODE:
        sys.exit(0)
    wh_creds = utils.parse_warehouse_creds(args.wh_creds, args.mode)

    if args.prediction_task == "classification":
        trainer = ClassificationTrainer(**args.merged_config)
    elif args.prediction_task == "regression":
        trainer = RegressionTrainer(**args.merged_config)

    # Creating the Redshift connector and session bcoz this case of code will only be triggerred for Redshift
    output_dir = (
        args.output_path
        if args.mode == constants.LOCAL_MODE
        else os.path.dirname(os.path.abspath(__file__))
    )

    file_handler = logging.FileHandler(
        os.path.join(output_dir, "preprocess_and_train.log")
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    warehouse = wh_creds["type"]
    train_procedure = train_and_store_model_results
    connector = ConnectorFactory.create(warehouse, output_dir)
    session = connector.build_session(wh_creds)
    local_folder = connector.get_local_dir()

    material_info_ = args.material_names
    # converting material info back to named tuple after serialisation and deserialisation
    material_info = []
    if isinstance(material_info_[0], list):
        for material in material_info_:
            material_info.append(constants.TrainTablesInfo(*material))

    train_results_json = preprocess_and_train(
        train_procedure,
        material_info,
        args.merged_config,
        args.input_column_types,
        session=session,
        connector=connector,
        trainer=trainer,
    )
    with open(os.path.join(local_folder, args.ec2_temp_output_json), "w") as file:
        json.dump(train_results_json, file)

    if args.mode in (constants.RUDDERSTACK_MODE, constants.K8S_MODE):
        logger.debug(f"Uploading trained files to s3://{args.s3_bucket}/{args.s3_path}")

        train_upload_whitelist = utils.merge_lists_to_unique(
            list(trainer.figure_names.values()),
            [
                f"{trainer.output_profiles_ml_model}_{model_file_name}",
                args.ec2_temp_output_json,
            ],
        )
        S3Utils.upload_directory_to_S3(
            args.s3_bucket,
            args.aws_region_name,
            args.s3_path,
            local_folder,
            train_upload_whitelist,
            args.mode,
        )

        logger.debug(f"Deleting additional local directory from {args.mode} mode.")
        connector.delete_local_data_folder()
