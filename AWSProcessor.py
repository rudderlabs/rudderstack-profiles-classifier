import time
import json
import boto3
import constants
from Processor import Processor
from typing import List, Tuple, Dict
from S3Utils import S3Utils
from logger import logger

class AWSProcessor(Processor):
    def _execute(self, ssm_client, instance_id, commands, ssm_sleep_time):
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': commands},
        )
        command_id = response['Command']['CommandId']
        output1 = ""
        output2 = ""
        while True:
            result = ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id,
            )
            time.sleep(ssm_sleep_time)
            if result['Status'] in ['Success', 'Failed', 'Cancelled']:
                output1 += result.get('StandardOutputContent', '')
                output2 += result.get('StandardErrorContent', '')
                break

        print("Error logs : ", output2)

    def train(self, train_procedure, materials: List[constants.TrainTablesInfo], merged_config: dict, prediction_task: str, wh_creds: dict, site_config: dict):
        s3_config = site_config["py_models"]["credentials_presets"]["s3"]
        remote_dir = constants.REMOTE_DIR
        instance_id = constants.INSTANCE_ID
        ec2_temp_output_json = constants.EC2_TEMP_OUTPUT_JSON
        s3_bucket = s3_config["bucket"]
        aws_region_name = s3_config["region"]
        s3_path = s3_config["path"]
        ssm_sleep_time = constants.SSM_SLEEP_TIME

        ssm_client = boto3.client(service_name='ssm', region_name=aws_region_name)
        commands = [
        f"cd {remote_dir}/rudderstack-profiles-classifier",
        f"pip install -r requirements.txt",
        f"python3 preprocess_and_train.py --s3_bucket {s3_bucket} --aws_region_name {aws_region_name} --s3_path {s3_path} --ec2_temp_output_json {ec2_temp_output_json} --material_names '{json.dumps(materials)}' --merged_config '{json.dumps(merged_config)}' --prediction_task {prediction_task} --wh_creds '{json.dumps(wh_creds)}'"
        ]
        self._execute(ssm_client, instance_id, commands, ssm_sleep_time)

        S3Utils._download_directory_from_s3(s3_bucket, aws_region_name, s3_path, self.connector.get_local_dir())
        S3Utils._delete_directory_from_s3(s3_bucket, aws_region_name, s3_path)

        try:
            train_results_json = self.connector.load_and_delete_json(ec2_temp_output_json)
        except Exception as e:
            logger.exception(f"An exception occured while trying to load and delete json {ec2_temp_output_json} from ec2: {e}")
            raise Exception(f"An exception occured while trying to load and delete json {ec2_temp_output_json} from ec2: {e}")
        return train_results_json