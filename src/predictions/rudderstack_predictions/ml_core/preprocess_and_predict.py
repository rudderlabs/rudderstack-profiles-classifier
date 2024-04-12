import os
import sys
import json
import joblib
import logging
import warnings
import cachetools
import numpy as np
import pandas as pd
from typing import Any

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

from ..trainers.TrainerFactory import TrainerFactory

from ..wht.pyNativeWHT import PyNativeWHT

from ..utils import utils
from ..utils.logger import logger
from ..utils import constants
from ..utils.S3Utils import S3Utils

from ..trainers.MLTrainer import MLTrainer
from ..connectors.ConnectorFactory import ConnectorFactory

from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)


def preprocess_and_predict(
    creds,
    s3_config,
    model_path,
    inputs,
    output_tablename,
    session,
    connector,
    trainer: MLTrainer,
):
    """
    This function is responsible for preprocessing
    and predicting on the data.
    """

    model_file_name = constants.MODEL_FILE_NAME
    connector.compute_udf_name(model_path)

    with open(model_path, "r") as f:
        results = json.load(f)
    train_model_id = results["model_info"]["model_id"]
    prob_th = results["model_info"].get("threshold")
    stage_name = results["model_info"]["file_location"]["stage"]
    model_hash = results["config"]["material_hash"]
    input_model_name = results["config"]["input_model_name"]

    numeric_columns = results["column_names"]["feature_table_column_types"]["numeric"]
    categorical_columns = results["column_names"]["feature_table_column_types"][
        "categorical"
    ]
    arraytype_columns = results["column_names"]["input_column_types"]["arraytype"]
    timestamp_columns = results["column_names"]["input_column_types"]["timestamp"]
    ignore_features = results["column_names"]["ignore_features"]

    model_name = f"{trainer.output_profiles_ml_model}_{model_file_name}"
    seq_no = None

    try:
        seq_no = int(inputs[0].split("_")[-1])
    except Exception as e:
        raise Exception(f"Error while parsing seq_no from inputs: {inputs}. Error: {e}")

    whtService = PyNativeWHT(None)
    whtService.init(connector, session, "", "")

    entity_var_table_name = whtService.compute_material_name(
        input_model_name, model_hash, seq_no
    )

    end_ts = connector.get_end_ts(
        session,
        whtService.get_registry_table_name(),
        input_model_name,
        model_hash,
        seq_no,
    )

    logger.debug(f"Pulling data from Entity-Var table - {entity_var_table_name}")
    raw_data = connector.get_table(
        session, entity_var_table_name, filter_condition=trainer.eligible_users
    )

    logger.debug("Transforming timestamp columns.")
    for col in timestamp_columns:
        raw_data = connector.add_days_diff(raw_data, col, col, end_ts)

    logger.debug(f"Transforming arraytype columns.")
    _, raw_data = connector.transform_arraytype_features(raw_data, arraytype_columns)

    predict_data = connector.drop_cols(raw_data, ignore_features)

    required_features_upper_case = set(
        [
            col.upper()
            for cols in results["column_names"]["feature_table_column_types"].values()
            for col in cols
            if col not in ignore_features
        ]
    )
    input_df = connector.select_relevant_columns(
        predict_data, required_features_upper_case
    )
    types = connector.generate_type_hint(
        input_df, results["column_names"]["feature_table_column_types"]
    )

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

    def predict_helper(df, model_name: str) -> Any:
        trained_model = load_model(model_name)
        df.columns = [x.upper() for x in df.columns]

        df[numeric_columns] = df[numeric_columns].replace({pd.NA: np.nan})
        df[categorical_columns] = df[categorical_columns].replace({pd.NA: None})
        return trainer.predict(trained_model, df)

    features = input_df.columns

    if creds["type"] == "snowflake":
        udf_name = connector.udf_name

        @F.pandas_udf(
            session=session,
            max_batch_size=10000,
            is_permanent=True,
            replace=True,
            stage_location=stage_name,
            name=udf_name,
            imports=[f"{stage_name}/{model_name}"] + connector.delete_files,
            packages=constants.SNOWFLAKE_TRAINING_PACKAGES + ["cachetools==4.2.2"],
        )
        def predict_scores(df: types) -> T.PandasSeries[float]:
            df.columns = features
            predictions = predict_helper(df, model_name)
            return predictions.round(4)

        prediction_udf = predict_scores
    elif creds["type"] in ("redshift", "bigquery"):
        local_folder = connector.get_local_dir()

        def predict_scores_rs(df: pd.DataFrame) -> pd.Series:
            df.columns = features
            predictions = predict_helper(df, model_name)
            return predictions.round(4)

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
        prob_th,
        input_df,
    )
    logger.debug("Writing predictions to warehouse")
    connector.write_table(
        preds_with_percentile,
        output_tablename,
        write_mode="overwrite",
        local=False,
        if_exists="replace",
        s3_config=s3_config,
    )
    logger.debug("Closing the session")

    connector.post_job_cleanup(session)
    logger.debug("Finished Predict job")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--wh_creds", type=json.loads)
    parser.add_argument("--s3_config", type=json.loads)
    parser.add_argument("--json_output_filename", type=str)
    parser.add_argument("--inputs", type=json.loads)
    parser.add_argument("--output_tablename", type=str)
    parser.add_argument("--merged_config", type=json.loads)
    parser.add_argument("--output_path", type=str)
    parser.add_argument("--mode", type=str)
    args = parser.parse_args()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = (
        args.output_path
        if args.mode == constants.LOCAL_MODE
        else os.path.join(current_dir, "output")
    )

    file_handler = logging.FileHandler(
        os.path.join(current_dir, "preprocess_and_predict.log")
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if args.mode == constants.RUDDERSTACK_MODE:
        logger.debug(f"Downloading files from S3 in {args.mode} mode.")
        S3Utils.download_directory(args.s3_config, output_dir, args.mode)
    trainer = TrainerFactory.create(args.merged_config)

    if args.mode == constants.CI_MODE:
        sys.exit(0)

    wh_creds = utils.parse_warehouse_creds(args.wh_creds, args.mode)
    connector = ConnectorFactory.create(wh_creds["type"], output_dir)
    session = connector.build_session(wh_creds)

    model_path = os.path.join(output_dir, args.json_output_filename)

    _ = preprocess_and_predict(
        wh_creds,
        args.s3_config,
        model_path,
        args.inputs,
        args.output_tablename,
        session=session,
        connector=connector,
        trainer=trainer,
    )

    if args.mode == constants.RUDDERSTACK_MODE:
        logger.debug(f"Deleting files in {output_dir}")
        utils.delete_folder(output_dir)
