import os
import sys
import json
import joblib
import warnings
import cachetools
import numpy as np
import pandas as pd

from typing import Any , List
from logger import logger
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
from S3Utils import S3Utils

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import utils
import constants
from SnowflakeConnector import SnowflakeConnector

warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

try:
    from RedshiftConnector import RedshiftConnector
except Exception as e:
    logger.warning(f"Could not import RedshiftConnector")


def predict(
    creds: dict,
    aws_config: dict,
    model_path: str,
    inputs: str,
    output_tablename: str,
    config: dict,
) -> None:
    """Generates the prediction probabilities and save results for given model_path

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        aws_config (dict): aws credentials - not required for snowflake. only used for redshift
        model_path (str): path to the file where the model details including model id etc are present. Created in training step
        inputs: (List[str]), containing sql queries such as "select * from <feature_table_name>" from which the script infers input tables        output_tablename (str): name of output table where prediction results are written
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

    Returns:
        None: save the prediction results but returns nothing
    """
    logger.debug("Starting Predict job")
    model_file_name = constants.MODEL_FILE_NAME
    current_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.dirname(model_path)

    default_config = utils.load_yaml(os.path.join(current_dir, "config/model_configs.yaml"))
    _ = config["data"].pop("package_name", None) # For backward compatibility. Not using it anywhere else, hence deleting.
    merged_config = utils.combine_config(default_config, config)

    with open(model_path, "r") as f:
        results = json.load(f)

    train_model_id = results["model_info"]["model_id"]
    prob_th = results["model_info"].get("threshold")
    stage_name = results["model_info"]["file_location"]["stage"]
    model_hash = results["config"]["material_hash"]
    input_model_name = results["input_model_name"]

    score_column_name = merged_config["outputs"]["column_names"]["score"]
    percentile_column_name = merged_config["outputs"]["column_names"]["percentile"]
    output_label_column = merged_config["outputs"]["column_names"].get(
        "output_label_column"
    )
    output_profiles_ml_model = merged_config["data"]["output_profiles_ml_model"]
    label_column = merged_config["data"]["label_column"]
    index_timestamp = merged_config["data"]["index_timestamp"]
    eligible_users = merged_config["data"]["eligible_users"]
    ignore_features = merged_config["preprocessing"]["ignore_features"]
    timestamp_columns = merged_config["preprocessing"]["timestamp_columns"]
    entity_column = merged_config["data"]["entity_column"]
    task = merged_config["data"].pop("task", "classification")

    model_name = f"{output_profiles_ml_model}_{model_file_name}"
    seq_no = None

    try:
        seq_no = int(inputs[0].split("_")[-1])
    except Exception as e:
        raise Exception(
            f"Error while parsing seq_no from inputs: {inputs}. Error: {e}"
        )

    feature_table_name = f"{constants.MATERIAL_TABLE_PREFIX}{input_model_name}_{model_hash}_{seq_no}"

    udf_name = None
    if creds["type"] == "snowflake":
        udf_name = f"prediction_score_{stage_name.replace('@','')}"
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
        connector.cleanup(session, udf_name=udf_name)
    elif creds["type"] == "redshift":
        connector = RedshiftConnector(folder_path)
        session = connector.build_session(creds)
        local_folder = connector.get_local_dir()

    column_names_file = f"{output_profiles_ml_model}_{train_model_id}_column_names.json"
    column_names_path = connector.join_file_path(column_names_file)
    features_path = connector.join_file_path(
        f"{output_profiles_ml_model}_{train_model_id}_array_time_feature_names.json"
    )

    material_table = connector.get_material_registry_name(
        session, constants.MATERIAL_REGISTRY_TABLE_PREFIX
    )

    end_ts = connector.get_end_ts(
        session, material_table, input_model_name, model_hash, seq_no
    )
    logger.debug(f"Pulling data from Feature table - {feature_table_name}")
    raw_data = connector.get_table(
        session, feature_table_name, filter_condition=eligible_users
    )
    
    arraytype_columns = connector.get_arraytype_columns_from_table(raw_data, features_path=features_path)
    ignore_features = utils.merge_lists_to_unique(ignore_features, arraytype_columns)
    predict_data = connector.drop_cols(raw_data, ignore_features)

    if len(timestamp_columns) == 0:
        timestamp_columns = connector.get_timestamp_columns_from_table(
            predict_data, features_path=features_path
        )
    for col in timestamp_columns:
        predict_data = connector.add_days_diff(predict_data, col, col, end_ts)

    input = connector.drop_cols(
        predict_data, [label_column, entity_column]
    )
    types = connector.generate_type_hint(input)

    predict_data = connector.add_index_timestamp_colum_for_predict_data(
        predict_data, index_timestamp, end_ts
    )

    @cachetools.cached(cache={})
    def load_model(filename: str):
        """session.import adds the staged model file to an import directory. We load the model file from this location"""
        import_dir = sys._xoptions.get("snowflake_import_directory")

        if import_dir:
            assert import_dir.startswith("/home/udf/")
            filename = os.path.join(import_dir, filename)
        else:
            filename = os.path.join(local_folder, filename)

        with open(filename, "rb") as file:
            m = joblib.load(file)
            return m

    @cachetools.cached(cache={})
    def load_column_names(filename: str):
        """session.import adds the staged model file to an import directory. We load the model file from this location"""
        import_dir = sys._xoptions.get("snowflake_import_directory")

        if import_dir:
            assert import_dir.startswith("/home/udf/")
            filename = os.path.join(import_dir, filename)

        with open(filename, "r") as file:
            column_names = json.load(file)
            return column_names

    def predict_helper(df, model_name: str, **kwargs) -> Any:
        trained_model = load_model(model_name)
        df.columns = [x.upper() for x in df.columns]
        column_names_path = kwargs.get("column_names_path", None)
        model_task = kwargs.get("model_task", task)
        column_names = load_column_names(column_names_path)
        categorical_columns = column_names["categorical_columns"]
        numeric_columns = column_names["numeric_columns"]
        df[numeric_columns] = df[numeric_columns].replace({pd.NA: np.nan})
        df[categorical_columns] = df[categorical_columns].replace({pd.NA: None})
        if model_task == "classification":
            return trained_model.predict_proba(df)[:, 1]
        elif model_task == "regression":
            return trained_model.predict(df)

    features = input.columns

    if creds["type"] == "snowflake":

        @F.pandas_udf(
            session=session,
            max_batch_size=10000,
            is_permanent=True,
            replace=True,
            stage_location=stage_name,
            name=udf_name,
            imports=[f"{stage_name}/{model_name}", f"{stage_name}/{column_names_file}"],
            packages=[
                "snowflake-snowpark-python>=0.10.0",
                "typing",
                "scikit-learn==1.1.1",
                "xgboost==1.5.0",
                "numpy==1.23.1",
                "pandas==1.5.3",
                "joblib",
                "cachetools",
                "PyYAML",
                "simplejson",
            ],
        )
        def predict_scores(df: types) -> T.PandasSeries[float]:
            df.columns = features
            predictions = predict_helper(
                df, model_name, column_names_path=column_names_file, model_task=task
            )
            return predictions.round(4)

        prediction_udf = predict_scores
    elif creds["type"] == "redshift":

        def predict_scores_rs(df: pd.DataFrame, column_names_path: str) -> pd.Series:
            df.columns = features
            predictions = predict_helper(
                df, model_name, column_names_path=column_names_path, model_task=task
            )
            return predictions.round(4)

        prediction_udf = predict_scores_rs
    
    logger.debug("Creating predictions on the feature data")
    preds_with_percentile = connector.call_prediction_udf(
        predict_data,
        prediction_udf,
        entity_column,
        index_timestamp,
        score_column_name,
        percentile_column_name,
        output_label_column,
        train_model_id,
        column_names_path,
        prob_th,
        input,
    )
    logger.debug("Writing predictions to warehouse")
    # TODO - Get role, bucket, path from site config
    if aws_config is not None:
        s3_creds = S3Utils.get_temporary_credentials("arn:aws:iam::454531037350:role/profiles-ml-s3")
        aws_config["bucket"] = constants.S3_BUCKET
        aws_config["path"] = constants.S3_PATH
        aws_config["access_key_id"] = s3_creds["access_key_id"]
        aws_config["access_key_secret"] = s3_creds["access_key_secret"]
        aws_config["aws_session_token"] = s3_creds["aws_session_token"]
    connector.write_table(
        preds_with_percentile, output_tablename, write_mode="overwrite", local=False , if_exists="replace"
    )
    logger.debug("Closing the session")    
    connector.cleanup(session, udf_name=udf_name,close_session=True)
    logger.debug("Finished Predict job")
