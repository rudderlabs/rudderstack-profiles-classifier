import os
import json
from typing import List

import src.utils.utils as utils
from src.utils.logger import logger
import src.utils.constants as constants
from src.processors.Processor import Processor


class LocalProcessor(Processor):
    def train(
        self,
        train_procedure,
        materials: List[constants.TrainTablesInfo],
        merged_config: dict,
        prediction_task: str,
        wh_creds: dict,
        site_config: dict,
    ):
        ec2_temp_output_json = constants.EC2_TEMP_OUTPUT_JSON
        local_dir = self.connector.get_local_dir()
        output_path = os.path.dirname(local_dir)
        commands = [
            f"python3",
            "-m",
            "src.ml_core.preprocess_and_train",
            "--ec2_temp_output_json",
            ec2_temp_output_json,
            "--material_names",
            json.dumps(materials),
            "--merged_config",
            json.dumps(merged_config),
            "--prediction_task",
            prediction_task,
            "--wh_creds",
            json.dumps(wh_creds),
            "--output_path",
            output_path,
            "--mode",
            constants.LOCAL_MODE,
        ]
        response_for_train = utils.subprocess_run(commands)
        if response_for_train.returncode != 0:
            raise Exception(
                f"Error occurred while running train script in local processing mode. Error: {response_for_train.stderr}"
            )
        try:
            train_results_json = self.connector.load_and_delete_json(
                ec2_temp_output_json
            )
        except Exception as e:
            logger.exception(
                f"An exception occured while trying to load and delete json {ec2_temp_output_json} from local: {e}"
            )
            raise Exception(
                f"An exception occured while trying to load and delete json {ec2_temp_output_json} from local: {e}"
            )
        return train_results_json
