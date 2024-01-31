import os
import json
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import WaiterError
import pandas as pd
from typing import List, Tuple, Union, Dict
from src.utils.S3Utils import S3Utils

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

import src.utils.utils as utils
import src.utils.logger as logger
from src.constants import constants
metrics_table = constants.METRICS_TABLE
model_file_name = constants.MODEL_FILE_NAME


def train_and_store_model_results_rs(
    feature_table_name: str, merged_config: dict, **kwargs
) -> dict:
    """Creates and saves the trained model pipeline after performing preprocessing and classification and
    returns the model id attached with the results generated.

    Args:
        feature_table_name (str): name of the user feature table generated by profiles feature table model,
        and is input to training and prediction merged_config (dict): configs from profiles.yaml which should
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
    model_file = connector.join_file_path(
        f"{trainer.output_profiles_ml_model}_{model_file_name}"
    )
    feature_df_path = connector.fetch_feature_df_path(feature_table_name)
    feature_df = pd.read_parquet(feature_df_path)
    feature_df.columns = [col.upper() for col in feature_df.columns]

    stringtype_features = connector.get_stringtype_features(
        feature_df, trainer.label_column, trainer.entity_column, session=session
    )
    categorical_columns = utils.merge_lists_to_unique(
        trainer.prep.categorical_pipeline["categorical_columns"], stringtype_features
    )

    non_stringtype_features = connector.get_non_stringtype_features(
        feature_df, trainer.label_column, trainer.entity_column, session=session
    )
    numeric_columns = utils.merge_lists_to_unique(
        trainer.prep.numeric_pipeline["numeric_columns"], non_stringtype_features
    )

    train_x, test_x, test_y, pipe, model_id, metrics_df, results = trainer.train_model(
        feature_df, categorical_columns, numeric_columns, merged_config, model_file
    )

    results['column_names'] = {
        'numeric_columns': numeric_columns,
        'categorical_columns': categorical_columns
    }

    trainer.plot_diagnostics(
        connector, session, pipe, None, test_x, test_y, trainer.label_column
    )
    try:
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
        connector.write_pandas(
            shap_importance, "FEATURE_IMPORTANCE", if_exists="replace"
        )
    except Exception as e:
        logger.error(f"Could not generate plots {e}")

    database_dtypes = json.loads(constants.rs_dtypes)
    metrics_table_query = ""
    for col in metrics_df.columns:
        if metrics_df[col].dtype == "object":
            metrics_df[col] = metrics_df[col].apply(lambda x: json.dumps(x))
            metrics_table_query += f"{col} {database_dtypes['text']},"
        elif metrics_df[col].dtype == "float64" or metrics_df[col].dtype == "int64":
            metrics_table_query += f"{col} {database_dtypes['num']},"
        elif metrics_df[col].dtype == "bool":
            metrics_table_query += f"{col} {database_dtypes['bool']},"
        elif metrics_df[col].dtype == "datetime64[ns]":
            metrics_table_query += f"{col} {database_dtypes['timestamp']},"
    metrics_table_query = metrics_table_query[:-1]

    connector.run_query(
        session,
        f"CREATE TABLE IF NOT EXISTS {metrics_table} ({metrics_table_query});",
        response=False,
    )
    connector.write_pandas(metrics_df, f"{metrics_table}", if_exists="append")
    return results


def prepare_feature_table(
    train_table_pair: constants.TrainTablesInfo,
    cardinal_feature_threshold: float,
    **kwargs,
) -> tuple:
    """This function creates a feature table as per the requirement of customer that is further used for training and prediction.

    Args:
        train_table_pair (constants.TrainTablesInfo): 
        cardinal_feature_threshold (float): The threshold value for the cardinality of the feature. Any feature with cardinality higher than this will be dropped.
    Returns:
        Tuple of feature_table, arraytype_columns, timestamp_columns
    """
    session = kwargs.get("session", None)
    connector = kwargs.get("connector", None)
    trainer = kwargs.get("trainer", None)
    try:
        feature_table_name = train_table_pair.feature_table_name
        feature_table_dt = train_table_pair.feature_table_date
        label_table_name = train_table_pair.label_table_name
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

        arraytype_columns = connector.get_arraytype_columns(session, feature_table_name)
        ignore_features = utils.merge_lists_to_unique(
            trainer.prep.ignore_features, arraytype_columns
        )
        high_cardinal_features = connector.get_high_cardinal_features(
            feature_table,
            trainer.label_column,
            trainer.entity_column,
            cardinal_feature_threshold,
        )
        ignore_features = utils.merge_lists_to_unique(
            ignore_features, high_cardinal_features
        )

        feature_table = connector.drop_cols(feature_table, [trainer.label_column])
        timestamp_columns = trainer.prep.timestamp_columns
        if len(timestamp_columns) == 0:
            timestamp_columns = connector.get_timestamp_columns(
                session, feature_table_name
            )
        for col in timestamp_columns:
            feature_table = connector.add_days_diff(
                feature_table, col, col, feature_table_dt
            )
        label_table = trainer.prepare_label_table(
            connector, session, label_table_name
        )
        uppercase_list = lambda names: [name.upper() for name in names]
        lowercase_list = lambda names: [name.lower() for name in names]
        ignore_features_ = [
            col
            for col in feature_table.columns
            if col in uppercase_list(ignore_features)
            or col in lowercase_list(ignore_features)
        ]
        trainer.prep.ignore_features = ignore_features_
        trainer.prep.timestamp_columns = timestamp_columns
        feature_table = connector.join_feature_table_label_table(
            feature_table, label_table, trainer.entity_column, "inner"
        )
        feature_table = connector.drop_cols(feature_table, ignore_features_)
        return feature_table, arraytype_columns, timestamp_columns
    except Exception as e:
        print(
            "Exception occured while preparing feature table. Please check the logs for more details"
        )
        raise e


def preprocess_and_train(
    train_procedure, train_table_pairs: List[constants.TrainTablesInfo], merged_config: dict, **kwargs
):
    session = kwargs.get("session", None)
    connector = kwargs.get("connector", None)
    trainer = kwargs.get("trainer", None)
    min_sample_for_training = constants.MIN_SAMPLES_FOR_TRAINING
    cardinal_feature_threshold = constants.CARDINAL_FEATURE_THRESOLD

    feature_table = None
    for train_table_pair in train_table_pairs:
        logger.info(
            f"Preparing training dataset using {train_table_pair.feature_table_name} and {train_table_pair.label_table_name} as feature and label tables respectively"
        )
        (
            feature_table_instance,
            arraytype_columns,
            timestamp_columns,
        ) = prepare_feature_table(
            train_table_pair,
            cardinal_feature_threshold,
            session=session,
            connector=connector,
            trainer=trainer,
        )
        feature_table = connector.get_merged_table(
            feature_table, feature_table_instance
        )

        break

    task_type = trainer.get_name()
    logger.info(f"Performing data validation for {task_type}")

    connector.do_data_validation(feature_table, trainer.label_column, task_type)
    logger.info("Data validation is completed")

    feature_table_name_remote = f"{trainer.output_profiles_ml_model}_features"
    filtered_feature_table = connector.filter_feature_table(
        feature_table,
        trainer.entity_column,
        trainer.max_row_count,
        min_sample_for_training,
    )
    connector.write_table(
        filtered_feature_table,
        feature_table_name_remote,
        write_mode="overwrite",
        if_exists="replace",
    )
    logger.info("Training and fetching the results")

    try:
        train_results_json = connector.call_procedure(
            train_procedure,
            feature_table_name_remote,
            merged_config,
            session=session,
            connector=connector,
            trainer=trainer,
        )
    except Exception as e:
        logger.error(f"Error while training the model: {e}")
        raise e

    if not isinstance(train_results_json, dict):
        train_results_json = json.loads(train_results_json)

    train_results_json['column_names']['arraytype_columns'] = arraytype_columns
    train_results_json['column_names']['timestamp_columns'] = timestamp_columns

    return train_results_json


if __name__ == "__main__":
    import argparse
    from src.trainers.MLTrainer import ClassificationTrainer, RegressionTrainer

    try:
        from src.connectors.RedshiftConnector import RedshiftConnector
    except ImportError:
        raise Exception("Could not import RedshiftConnector")

    parser = argparse.ArgumentParser()

    parser.add_argument("--s3_bucket", type=str)
    parser.add_argument("--aws_region_name", type=str)
    parser.add_argument("--s3_path", type=str)
    parser.add_argument("--ec2_temp_output_json", type=str)
    parser.add_argument("--material_names", type=json.loads)
    parser.add_argument("--merged_config", type=json.loads)
    parser.add_argument("--prediction_task", type=str)
    parser.add_argument("--wh_creds", type=json.loads)
    parser.add_argument("--mode", type=str)
    args = parser.parse_args()

    if args.mode == constants.K8S_MODE:
        wh_creds_str = os.environ[constants.K8S_WH_CREDS_KEY]
        wh_creds = json.loads(wh_creds_str)
    else:
        wh_creds = args.wh_creds

    prep_config = utils.PreprocessorConfig(**args.merged_config["preprocessing"])
    if args.prediction_task == "classification":
        trainer = ClassificationTrainer(**args.merged_config["data"], prep=prep_config)
    elif args.prediction_task == "regression":
        trainer = RegressionTrainer(**args.merged_config["data"], prep=prep_config)
    # Creating the Redshift connector and session bcoz this case of code will only be triggerred for Redshift
    train_procedure = train_and_store_model_results_rs
    connector = RedshiftConnector("./")
    session = connector.build_session(wh_creds)
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
        session=session,
        connector=connector,
        trainer=trainer,
    )
    with open(
        os.path.join(connector.get_local_dir(), args.ec2_temp_output_json), "w"
    ) as file:
        json.dump(train_results_json, file)


    logger.debug(f"Uploading trained files to s3://{args.s3_bucket}/{args.s3_path}")
    model_id = train_results_json["model_id"]
    S3_UPLOAD_WHITELIST = [trainer.figure_names["feature-importance-chart"],
                            trainer.figure_names["lift-chart"],
                            trainer.figure_names["pr-auc-curve"],
                            trainer.figure_names["roc-auc-curve"],
                            f"{trainer.output_profiles_ml_model}_{model_id}_column_names.json",
                            f"{trainer.output_profiles_ml_model}_{model_file_name}",
                            "train_results.json"]
    if args.mode == constants.K8S_MODE:
        S3Utils.upload_directory(args.s3_bucket, args.aws_region_name, args.s3_path, connector.get_local_dir(), S3_UPLOAD_WHITELIST)
    else:
        S3Utils.upload_directory_using_keys(args.s3_bucket, args.aws_region_name, args.s3_path, connector.get_local_dir(), S3_UPLOAD_WHITELIST)
    logger.debug(f"Deleting local directory from ec2 machine")
    connector.cleanup(delete_local_data=True)
