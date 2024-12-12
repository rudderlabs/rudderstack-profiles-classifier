import os
import json
from typing import List
from dataclasses import asdict
import sys

from ..utils import utils
from ..utils.logger import logger
from ..utils import constants
from ..processors.Processor import Processor


class LocalProcessor(Processor):
    def train(
        self,
        train_procedure,
        materials: List[constants.TrainTablesInfo],
        merged_config: dict,
        input_column_types: dict,
        input_columns: List[str],
        metrics_table: str,
        wh_creds: dict,
        site_config: dict,
        pkl_model_file_name: str,
    ):
        local_dir = self.connector.get_local_dir()
        output_path = os.path.dirname(local_dir)
        commands = [
            sys.executable,
            "-u",
            "-m",
            f"{self.ml_core_path}.preprocess_and_train",
            "--material_names",
            json.dumps(materials),
            "--merged_config",
            json.dumps(merged_config),
            "--input_column_types",
            json.dumps(input_column_types),
            "--input_columns",
            json.dumps(input_columns),
            "--connector_feature_table_name",
            self.connector.feature_table_name,
            "--wh_creds",
            json.dumps(wh_creds),
            "--output_path",
            output_path,
            "--mode",
            constants.LOCAL_MODE,
            "--metrics_table",
            metrics_table,
            "--pkl_model_file_name",
            pkl_model_file_name,
        ]
        response_for_train = utils.subprocess_run(commands)
        if response_for_train.returncode != 0:
            raise Exception(
                f"Error occurred while running train script in local processing mode. Error: {response_for_train.stderr}"
            )
        train_results_json = self.connector.load_and_delete_json(
            constants.TRAIN_JSON_RESULT_FILE
        )
        return train_results_json

    def predict(
        self,
        creds,
        s3_config,
        model_path,
        inputs,
        end_ts,
        output_tablename,
        merged_config,
        site_config: dict,
        model_hash: str,
    ):
        output_path = os.path.dirname(model_path)
        json_output_filename = model_path.split("/")[-1]

        logger.get().debug("Starting prediction on local processing mode")
        commands = [
            sys.executable,
            "-u",
            "-m",
            f"{self.ml_core_path}.preprocess_and_predict",
            "--wh_creds",
            json.dumps(creds),
            "--s3_config",
            json.dumps(s3_config),
            "--json_output_filename",
            json_output_filename,
            "--inputs",
            json.dumps(inputs, default=asdict),
            "--end_ts",
            end_ts,
            "--output_tablename",
            output_tablename,
            "--merged_config",
            json.dumps(merged_config),
            "--output_path",
            output_path,
            "--mode",
            constants.LOCAL_MODE,
            "--model_hash",
            model_hash,
        ]
        response_for_predict = utils.subprocess_run(commands)
        if response_for_predict.returncode != 0:
            raise Exception(
                f"Error occurred while running predict script in local processing mode. Error: {response_for_predict.stderr}"
            )
        logger.get().debug("Done predicting")
