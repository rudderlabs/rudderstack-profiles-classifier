import os
import json
import logging
import warnings
import cachetools
import numpy as np
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

DEFAULT_BATCH_SIZE = 100  # Process 100 rows at a time by default


def get_batch_iterator(
    connector: Connector,
    joined_input_table_name: str,
    batch_size: int,
    filter_condition: str = None,
) -> Generator[pd.DataFrame, None, None]:
    """
    Creates a generator that yields batches of data from the input table.
    For BigQuery and Redshift, this uses LIMIT and OFFSET to fetch data in batches.
    """
    count_query = f"SELECT COUNT(*) as count FROM {joined_input_table_name}"
    if filter_condition:
        count_query += f" WHERE {filter_condition}"

    total_rows = connector.run_query(count_query)[0].count
    logger.get().info(f"Total rows to process: {total_rows}")

    for offset in range(0, total_rows, batch_size):
        query = f"SELECT * FROM {joined_input_table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += f" LIMIT {batch_size} OFFSET {offset}"

        batch_df = connector.run_query(query)
        yield pd.DataFrame(batch_df)


def process_batch(
    batch_df: pd.DataFrame,
    numeric_columns: List[str],
    categorical_columns: List[str],
    arraytype_columns: List[str],
    booleantype_columns: List[str],
    timestamp_columns: List[str],
    ignore_features: List[str],
    required_features_upper_case: set,
    transformed_arraytype_columns: Dict[str, List[str]],
    trainer: MLTrainer,
    connector: Connector,
    end_ts: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process a single batch of data for preprocessing."""
    logger.get().debug(f"Processing batch of size {len(batch_df)}")

    # Transform arraytype columns
    _, batch_df = connector.transform_arraytype_features(
        batch_df,
        arraytype_columns,
        trainer.prep.top_k_array_categories,
        predict_arraytype_features=transformed_arraytype_columns,
    )

    # Transform boolean columns
    batch_df = connector.transform_booleantype_features(batch_df, booleantype_columns)

    # Drop ignore features
    predict_data = connector.drop_cols(batch_df, ignore_features)

    # Select relevant columns
    input_df = connector.select_relevant_columns(
        predict_data, required_features_upper_case
    )

    # Add timestamp column
    predict_data = connector.add_index_timestamp_colum_for_predict_data(
        predict_data, trainer.index_timestamp, end_ts
    )

    return predict_data, input_df


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
    """
    This function is responsible for preprocessing and predicting on the data.
    For BigQuery and Redshift, it processes data in batches from the start
    to handle large datasets efficiently.
    """
    connector.compute_udf_name(model_path)

    with open(model_path, "r") as f:
        results = json.load(f)
    train_model_id = results["model_info"]["model_id"]
    stage_name = results["model_info"]["file_location"]["stage"]
    pkl_model_file_name = results["model_info"]["file_location"]["file_name"]

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

    joined_input_table_name = (
        f"prediction_joined_table_{utils.generate_random_string(5)}"
    )
    connector.join_input_tables(
        inputs, input_columns, trainer.entity_column, joined_input_table_name
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

        model = trainer.load_model(filename)
        return model

    def predict_helper(df, pkl_model_file_name: str) -> Any:
        df.columns = features
        df = utils.transform_null(
            df, numeric_columns, categorical_columns, timestamp_columns
        )

        trained_model = load_model(pkl_model_file_name)
        df.columns = [x.upper() for x in df.columns]
        return trainer.predict(trained_model, df)

    if creds["type"] == "snowflake":
        # Existing Snowflake logic remains unchanged since it handles large datasets efficiently
        logger.get().debug("Using Snowflake native processing")
        raw_data = connector.get_table(
            joined_input_table_name, filter_condition=trainer.eligible_users
        )
        _, raw_data = connector.transform_arraytype_features(
            raw_data,
            arraytype_columns,
            trainer.prep.top_k_array_categories,
            predict_arraytype_features=transformed_arraytype_columns,
        )
        raw_data = connector.transform_booleantype_features(
            raw_data, booleantype_columns
        )
        predict_data = connector.drop_cols(raw_data, ignore_features)
        input_df = connector.select_relevant_columns(
            predict_data, required_features_upper_case
        )
        types = connector.generate_type_hint(input_df)
        predict_data = connector.add_index_timestamp_colum_for_predict_data(
            predict_data, trainer.index_timestamp, end_ts
        )
        features = input_df.columns

        # Snowflake UDF setup
        udf_name = connector.udf_name

        pycaret_score_column = trainer.pred_output_df_columns["score"]
        pycaret_label_column = trainer.pred_output_df_columns.get(
            "label", "prediction_score"
        )  # To make up for the missing column in case of regression

        @F.pandas_udtf(
            session=connector.session,
            is_permanent=False,
            replace=True,
            stage_location=stage_name,
            name=udf_name,
            output_schema=PandasDataFrameType(
                [FloatType(), FloatType()], [pycaret_score_column, pycaret_label_column]
            ),
            input_types=[PandasDataFrameType(types)],
            input_names=features,
            imports=[f"{stage_name}/{pkl_model_file_name}.pkl"]
            + connector.delete_files,
            packages=constants.SNOWFLAKE_TRAINING_PACKAGES + ["cachetools==4.2.2"],
        )
        class predict_scores:
            def end_partition(self, df):
                predictions = predict_helper(df, pkl_model_file_name)

                # Create a new DataFrame with the extracted column names
                prediction_df = pd.DataFrame()
                prediction_df[pycaret_score_column] = predictions[pycaret_score_column]

                # Check if 'label' is present in pred_output_df_columns
                # Had to add a dummy label column in case of regression to the output dataframe as the UDTF expects the two columns in output
                if "label" in trainer.pred_output_df_columns:
                    prediction_df[pycaret_label_column] = predictions[
                        pycaret_label_column
                    ]
                else:
                    prediction_df[pycaret_label_column] = np.nan

                yield prediction_df

        prediction_udf = predict_scores

        # Call prediction UDF
        preds_with_percentile = connector.call_prediction_udf(
            predict_data,
            prediction_udf,
            trainer.entity_column,
            trainer.index_timestamp,
            trainer.outputs.column_names.get("score"),
            trainer.outputs.column_names.get("percentile"),
            trainer.outputs.column_names.get("output_label_column"),
            train_model_id,
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
        local_folder = connector.get_local_dir()

        def predict_scores_rs(df: pd.DataFrame) -> pd.DataFrame:
            predictions = predict_helper(df, pkl_model_file_name)
            return predictions

        prediction_udf = predict_scores_rs
        features = None  # Will be set in first batch
        first_batch = True

        # Process data in batches from the start
        batch_iterator = get_batch_iterator(
            connector,
            joined_input_table_name,
            batch_size,
            filter_condition=trainer.eligible_users,
        )

        for batch_df in tqdm(batch_iterator, desc="Processing batches"):
            # Process batch
            batch_predict_data, batch_input_df = process_batch(
                batch_df,
                numeric_columns,
                categorical_columns,
                arraytype_columns,
                booleantype_columns,
                timestamp_columns,
                ignore_features,
                required_features_upper_case,
                transformed_arraytype_columns,
                trainer,
                connector,
                end_ts,
            )

            # Set features on first batch
            if features is None:
                features = batch_input_df.columns

            # Get predictions
            batch_predictions = predict_scores_rs(batch_input_df)

            # Create batch result DataFrame
            batch_result = pd.DataFrame()
            batch_result[trainer.entity_column] = batch_predict_data[
                trainer.entity_column
            ]
            batch_result[trainer.index_timestamp] = batch_predict_data[
                trainer.index_timestamp
            ]
            batch_result["model_id"] = train_model_id

            # Add predictions
            batch_result[trainer.outputs.column_names.get("score")] = batch_predictions[
                trainer.pred_output_df_columns["score"]
            ]
            if "label" in trainer.pred_output_df_columns:
                batch_result[
                    trainer.outputs.column_names.get("output_label_column")
                ] = batch_predictions[trainer.pred_output_df_columns["label"]]

            logger.get().debug("Writing predictions to warehouse")
            if first_batch:
                connector.write_table(
                    batch_result,
                    output_tablename,
                    write_mode="overwrite",
                    local=False,
                    if_exists="replace",
                    s3_config=s3_config,
                )
                first_batch = False
            else:
                columns = ", ".join(batch_result.columns)
                values_list = []
                for _, row in batch_result.iterrows():
                    formatted_values = []
                    for val in row:
                        if isinstance(val, str):
                            formatted_values.append(f"'{val}'")
                        elif isinstance(
                            val, (pd.Timestamp, datetime.datetime, datetime.date)
                        ):
                            formatted_values.append(f"'{val}'")
                        elif isinstance(val, bool):
                            formatted_values.append(str(val).upper())
                        elif pd.isna(val) or val is None:
                            formatted_values.append("NULL")
                        else:
                            formatted_values.append(str(val))

                    row_values = f"({', '.join(formatted_values)})"
                    values_list.append(row_values)

                values_clause = ",\n    ".join(values_list)

                insert_query = f"""INSERT INTO {output_tablename} ({columns})
                VALUES 
                    {values_clause}"""
                connector.run_query(insert_query, response=False)

            del (
                batch_df,
                batch_predict_data,
                batch_input_df,
                batch_predictions,
                batch_result,
            )

        # Calculate percentiles using SQL directly in the warehouse
        score_column = trainer.outputs.column_names.get("score")
        percentile_column = trainer.outputs.column_names.get("percentile")
        percentile_column_type = "FLOAT" if creds["type"] == "redshift" else "FLOAT64"

        percentile_column_create_query = f"""
        ALTER TABLE {output_tablename} 
        ADD COLUMN {percentile_column} {percentile_column_type}
        """

        percentile_query = f"""
        UPDATE {output_tablename} t1
        SET {percentile_column} = ROUND(t2.percentile_rank * 100, 5)
        FROM (
            SELECT 
                {trainer.entity_column},
                PERCENT_RANK() OVER (ORDER BY {score_column}) as percentile_rank
            FROM {output_tablename}
        ) t2
        WHERE t1.{trainer.entity_column} = t2.{trainer.entity_column}
        """

        for query in (percentile_column_create_query, percentile_query):
            connector.run_query(query, response=False)

        logger.get().info("Batch processing completed successfully")

    try:
        prev_prediction_table = connector.get_old_prediction_table(
            trainer.prediction_horizon_days,
            str(end_ts.date()),
            trainer.output_profiles_ml_model,
            model_hash,
            whtService.get_registry_table_name(),
        )

        logger.get().debug(
            f"Fetching previous prediction table: {prev_prediction_table}"
        )
        prev_predictions = connector.get_table(prev_prediction_table)

        label_table = trainer.prepare_label_table(connector, joined_input_table_name)
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
                "model_id": [model_id],
                "prediction_date": [prediction_date],
                "label_date": [end_ts],
                "prediction_table_name": [prev_prediction_table],
                "label_table_name": [joined_input_table_name],
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
    connector.drop_joined_tables(table_list=[joined_input_table_name])
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
