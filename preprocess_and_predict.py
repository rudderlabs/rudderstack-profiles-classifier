import os
import sys
import json
import joblib
import warnings
import cachetools
import numpy as np
import pandas as pd
from typing import Any , List

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

import utils
import constants
from logger import logger

from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

def preprocess_and_predict(
    creds, 
    aws_config, 
    model_path, 
    inputs, 
    output_tablename, 
    prediction_task, 
    udf_name,
    **kwargs,
):
    """
    This function is responsible for preprocessing 
    and predicting on the data.
    """
    session = kwargs.get("session", None)
    connector = kwargs.get("connector", None)
    trainer = kwargs.get("trainer", None)

    model_file_name = constants.MODEL_FILE_NAME

    with open(model_path, "r") as f:
        results = json.load(f)
    train_model_id = results["model_info"]["model_id"]
    prob_th = results["model_info"].get("threshold")
    stage_name = results["model_info"]["file_location"]["stage"]
    model_hash = results["config"]["material_hash"]
    input_model_name = results["input_model_name"]

    model_name = f"{trainer.output_profiles_ml_model}_{model_file_name}"
    seq_no = None

    try:
        seq_no = int(inputs[0].split("_")[-1])
    except Exception as e:
        raise Exception(
            f"Error while parsing seq_no from inputs: {inputs}. Error: {e}"
        )

    feature_table_name = f"{constants.MATERIAL_TABLE_PREFIX}{input_model_name}_{model_hash}_{seq_no}"
    column_names_file = f"{trainer.output_profiles_ml_model}_{train_model_id}_column_names.json"
    column_names_path = connector.join_file_path(column_names_file)
    features_path = connector.join_file_path(
        f"{trainer.output_profiles_ml_model}_{train_model_id}_array_time_feature_names.json"
    )

    material_table = connector.get_material_registry_name(
        session, constants.MATERIAL_REGISTRY_TABLE_PREFIX
    )

    end_ts = connector.get_end_ts(
        session, material_table, input_model_name, model_hash, seq_no
    )
    logger.debug(f"Pulling data from Feature table - {feature_table_name}")
    raw_data = connector.get_table(
        session, feature_table_name, filter_condition=trainer.eligible_users
    )
    
    arraytype_columns = connector.get_arraytype_columns_from_table(raw_data, features_path=features_path)
    ignore_features = utils.merge_lists_to_unique(trainer.prep.ignore_features, arraytype_columns)
    predict_data = connector.drop_cols(raw_data, ignore_features)

    if len(trainer.prep.timestamp_columns) == 0:
        timestamp_columns = connector.get_timestamp_columns_from_table(
            predict_data, features_path=features_path
        )
    for col in timestamp_columns:
        predict_data = connector.add_days_diff(predict_data, col, col, end_ts)

    input = connector.drop_cols(
        predict_data, [trainer.label_column, trainer.entity_column]
    )
    types = connector.generate_type_hint(input)

    predict_data = connector.add_index_timestamp_colum_for_predict_data(
        predict_data, trainer.index_timestamp, end_ts
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
        model_task = kwargs.get("model_task", prediction_task)
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
                df, model_name, column_names_path=column_names_file, model_task=prediction_task
            )
            return predictions

        prediction_udf = predict_scores
    elif creds["type"] == "redshift":
        local_folder = connector.get_local_dir()
        def predict_scores_rs(df: pd.DataFrame, column_names_path: str) -> pd.Series:
            df.columns = features
            predictions = predict_helper(
                df, model_name, column_names_path=column_names_path, model_task=prediction_task
            )
            return predictions

        prediction_udf = predict_scores_rs
    
    logger.debug("Creating predictions on the feature data")
    preds_with_percentile = connector.call_prediction_udf(
        predict_data,
        prediction_udf,
        trainer.entity_column,
        trainer.index_timestamp,
        trainer.outputs.column_names.get("score"),
        trainer.outputs.column_names.get("percentile"),
        trainer.outputs.column_names.get("output_label_column"),
        train_model_id,
        column_names_path,
        prob_th,
        input,
    )
    logger.debug("Writing predictions to warehouse")
    connector.write_table(
        preds_with_percentile, output_tablename, write_mode="overwrite", local=False , if_exists="replace"
    )
    logger.debug("Closing the session")    
    connector.cleanup(session, udf_name=udf_name,close_session=True)
    logger.debug("Finished Predict job")
