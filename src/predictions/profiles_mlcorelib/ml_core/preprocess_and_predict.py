import os
import json
import time
import logging
import warnings
import cachetools
import pandas as pd
from typing import Any, List, Dict, Generator, Tuple
from tqdm import tqdm

from snowflake.snowpark.types import *
import snowflake.snowpark.functions as F

from ..trainers.TrainerFactory import TrainerFactory

from ..wht.pyNativeWHT import PyNativeWHT

from ..utils import utils
from ..utils.logger import logger
from ..utils import constants
from ..utils.S3Utils import S3Utils

from ..trainers.MLTrainer import MLTrainer
from ..connectors.Connector import Connector
from ..connectors.ConnectorFactory import ConnectorFactory

from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

DEFAULT_BATCH_SIZE = 100


def setup_prediction_environment(
    model_path: str, end_ts: Any, connector: Connector, trainer: MLTrainer
):
    """Sets up the prediction environment by loading model info and configuring necessary parameters."""
    connector.compute_udf_name(model_path)

    with open(model_path, "r") as f:
        results = json.load(f)

    train_model_id = results["model_info"]["model_id"]
    stage_name = results["model_info"]["file_location"]["stage"]
    pkl_model_file_name = results["model_info"]["file_location"]["file_name"]

    score_column = trainer.outputs.column_names.get("score")
    percentile_column = trainer.outputs.column_names.get("percentile")
    output_column = trainer.outputs.column_names.get("output_label_column")
    model_id_column = "model_id"

    input_column_types = results["column_names"]["input_column_types"]
    numeric_columns = results["column_names"]["input_column_types"]["numeric"]
    categorical_columns = results["column_names"]["input_column_types"]["categorical"]
    arraytype_columns = results["column_names"]["input_column_types"]["arraytype"]
    timestamp_columns = results["column_names"]["input_column_types"]["timestamp"]
    booleantype_columns = results["column_names"]["input_column_types"]["booleantype"]
    ignore_features = results["column_names"]["ignore_features"]
    required_features_upper_case = set(
        [col.upper() for col in results["column_names"]["feature_table_column_types"]]
    )

    input_columns = utils.extract_unique_values(input_column_types)
    transformed_arraytype_columns = {
        word: [item for item in numeric_columns if item.startswith(word)]
        for word in arraytype_columns
    }
    end_ts = utils.parse_timestamp(end_ts)

    # No need to decide whether to create PyNativeWHT or PythonWHT since all the methods being called
    # here have the same implementation in both classes.
    whtService = PyNativeWHT(None, None, None)
    whtService.set_connector(connector)

    return {
        "train_model_id": train_model_id,
        "stage_name": stage_name,
        "pkl_model_file_name": pkl_model_file_name,
        "score_column": score_column,
        "percentile_column": percentile_column,
        "output_column": output_column,
        "model_id_column": model_id_column,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "arraytype_columns": arraytype_columns,
        "timestamp_columns": timestamp_columns,
        "booleantype_columns": booleantype_columns,
        "ignore_features": ignore_features,
        "required_features_upper_case": required_features_upper_case,
        "input_columns": input_columns,
        "transformed_arraytype_columns": transformed_arraytype_columns,
        "end_ts": end_ts,
        "whtService": whtService,
    }


def setup_joined_input_table(
    connector: Connector,
    inputs: List[utils.InputsConfig],
    input_columns: List[str],
    trainer: MLTrainer,
) -> str:
    """Creates a joined input table from the input configurations."""
    joined_input_table_name = (
        f"prediction_joined_table_{utils.generate_random_string(5)}"
    )
    connector.join_input_tables(
        inputs, input_columns, trainer.entity_column, joined_input_table_name
    )
    return joined_input_table_name


def preprocess_and_predict(
    creds,
    s3_config,
    model_path,
    inputs,
    end_ts,
    output_tablename,
    connector: Connector,
    trainer: MLTrainer,
    model_hash: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    """Main function to preprocess data and generate predictions."""
    env_setup = setup_prediction_environment(model_path, end_ts, connector, trainer)
    env_setup["joined_input_table_name"] = setup_joined_input_table(
        connector, inputs, env_setup["input_columns"], trainer
    )

    if creds["type"] == "snowflake":
        logger.get().debug("Using Snowflake native processing")
        prediction_udf, predict_data, input_df = (
            connector.execute_snowflake_predictions(
                trainer, env_setup["joined_input_table_name"], env_setup, end_ts
            )
        )

        preds_with_percentile = connector.call_prediction_udf(
            predict_data,
            prediction_udf,
            trainer.entity_column,
            trainer.index_timestamp,
            env_setup["score_column"],
            env_setup["percentile_column"],
            env_setup["output_column"],
            env_setup["train_model_id"],
            input_df,
            trainer.pred_output_df_columns,
        )

        logger.get().debug("Writing predictions to warehouse")
        connector.write_table(
            preds_with_percentile,
            output_tablename,
            write_mode="overwrite",
            local=False,
            if_exists="replace",
            s3_config=s3_config,
        )

    elif creds["type"] in ("redshift", "bigquery"):
        logger.get().debug("Using batch processing for BigQuery/Redshift")
        connector.execute_batch_predictions(
            trainer, env_setup, output_tablename, s3_config, batch_size
        )
        connector.calculate_percentiles(
            output_tablename,
            trainer,
            env_setup["score_column"],
            env_setup["percentile_column"],
            creds["type"],
        )

        logger.get().info("Batch processing completed successfully")

    try:
        prev_prediction_table = connector.get_old_prediction_table(
            trainer.prediction_horizon_days,
            str(end_ts.date()),
            trainer.output_profiles_ml_model,
            model_hash,
            env_setup["whtService"].get_registry_table_name(),
        )

        logger.get().debug(
            f"Fetching previous prediction table: {prev_prediction_table}"
        )
        prev_predictions = connector.get_table(prev_prediction_table)

        label_table = trainer.prepare_label_table(
            connector, env_setup["joined_input_table_name"]
        )
        prev_pred_ground_truth_table = connector.join_feature_table_label_table(
            prev_predictions, label_table, trainer.entity_column, "inner"
        )

        (
            scores_and_gt_df,
            model_id,
            prediction_date,
        ) = connector.get_previous_predictions_info(
            prev_pred_ground_truth_table,
            trainer.outputs.column_names.get("score"),
            trainer.label_column,
        )

        scores_and_gt_df.columns = [x.upper() for x in scores_and_gt_df.columns]
        y_true = scores_and_gt_df[trainer.label_column.upper()]
        y_pred = scores_and_gt_df[trainer.outputs.column_names.get("score").upper()]

        metrics = trainer.get_prev_pred_metrics(y_true, y_pred.to_numpy())

        metrics_df = pd.DataFrame(
            {
                "model_name": [trainer.output_profiles_ml_model],
                env_setup["model_id_column"]: [model_id],
                "prediction_date": [prediction_date],
                "label_date": [end_ts],
                "prediction_table_name": [prev_prediction_table],
                "label_table_name": [env_setup["joined_input_table_name"]],
                "metrics": [metrics],
            }
        ).reset_index(drop=True)

        metrics_table = "RS_PROFILES_PREDICTIONS_METRICS"

        if creds["type"] == "snowflake":
            connector.write_pandas(
                metrics_df,
                table_name=f"{metrics_table}",
                session=connector.session,
                auto_create_table=True,
                overwrite=False,
            )
        elif creds["type"] in ("redshift", "bigquery"):
            if not connector.is_valid_table(metrics_table):
                (
                    metrics_df,
                    create_metrics_table_query,
                ) = connector.fetch_create_metrics_table_query(
                    metrics_df,
                    metrics_table,
                )
                connector.run_query(
                    create_metrics_table_query,
                    response=False,
                )

            connector.write_pandas(metrics_df, f"{metrics_table}", if_exists="append")
    except Exception as e:
        logger.get().error(f"Error while fetching previous prediction table: {e}")

    logger.get().debug("Closing the session")
    connector.drop_joined_tables(table_list=[env_setup["joined_input_table_name"]])
    connector.post_job_cleanup()
    logger.get().debug("Finished Predict job")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--wh_creds", type=json.loads)
    parser.add_argument("--s3_config", type=json.loads)
    parser.add_argument("--json_output_filename", type=str)
    parser.add_argument("--inputs", type=json.loads)
    parser.add_argument("--end_ts", type=str)
    parser.add_argument("--output_tablename", type=str)
    parser.add_argument("--merged_config", type=json.loads)
    parser.add_argument("--output_path", type=str)
    parser.add_argument("--mode", type=str)
    parser.add_argument("--model_hash", type=str)
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
    logger.get().addHandler(file_handler)

    if args.mode == constants.RUDDERSTACK_MODE:
        logger.get().debug(f"Downloading files from S3 in {args.mode} mode.")
        S3Utils.download_directory(args.s3_config, output_dir)
    if args.mode == constants.CI_MODE:
        sys.exit(0)
    trainer = TrainerFactory.create(args.merged_config)

    wh_creds = utils.parse_warehouse_creds(args.wh_creds, args.mode)
    connector = ConnectorFactory.create(wh_creds, output_dir)

    model_path = os.path.join(output_dir, args.json_output_filename)

    inputs_info: List[utils.InputsConfig] = []
    try:
        for input_ in args.inputs:
            inputs_info.append(utils.InputsConfig(**input_))
    except Exception as e:
        logger.get().error(f"Error while parsing inputs: {e}")
        raise Exception(f"Error while parsing inputs: {e}")

    if args.model_hash is None:
        raise Exception("model_hash is required")

    _ = preprocess_and_predict(
        wh_creds,
        args.s3_config,
        model_path,
        inputs_info,
        args.end_ts,
        args.output_tablename,
        connector=connector,
        trainer=trainer,
        model_hash=args.model_hash,
    )

    if args.mode == constants.RUDDERSTACK_MODE:
        logger.get().debug(f"Deleting files in {output_dir}")
        utils.delete_folder(output_dir)
