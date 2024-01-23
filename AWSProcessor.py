import os
import yaml
import time
import json
import boto3
import constants
from logger import logger
from Processor import Processor
from typing import Any, List, Tuple, Union
from S3Utils import S3Utils

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

    def train(self, train_procedure, material_names: List[Tuple[str]], merged_config: dict, prediction_task: str, wh_creds: dict):
        remote_dir = constants.REMOTE_DIR
        instance_id = constants.INSTANCE_ID
        ec2_temp_output_json = constants.EC2_TEMP_OUTPUT_JSON
        s3_bucket = constants.S3_BUCKET
        aws_region_name = constants.AWS_REGION_NAME
        s3_path = constants.S3_PATH
        ssm_sleep_time = constants.SSM_SLEEP_TIME

        ssm_client = boto3.client(service_name='ssm', region_name=aws_region_name)
        commands = [
        f"cd {remote_dir}/rudderstack-profiles-classifier",
        f"pip install -r requirements.txt",
        f"python3 preprocess_and_train.py --s3_bucket {s3_bucket} --aws_region_name {aws_region_name} --s3_path {s3_path} --ec2_temp_output_json {ec2_temp_output_json} --material_names '{json.dumps(material_names)}' --merged_config '{json.dumps(merged_config)}' --prediction_task {prediction_task} --wh_creds '{json.dumps(wh_creds)}'"
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
    
    def predict(
        self, 
        creds, 
        aws_config, 
        model_path, 
        inputs, 
        output_tablename, 
        merged_config, 
        prediction_task, 
        udf_name,
    ):
        remote_dir = constants.REMOTE_DIR
        instance_id = constants.INSTANCE_ID
        ssm_sleep_time = constants.SSM_SLEEP_TIME

        local_folder = self.connector.get_local_dir()
        json_output_filename = model_path.split("/")[-1]

        PREDICT_UPLOAD_WHITELIST = [file for file in os.listdir(local_folder) 
                                        if os.path.isfile(os.path.join(local_folder, file)) and 
                                        not file.endswith(".gzip")]+[json_output_filename]
        S3Utils.upload_directory(aws_config["bucket"], aws_config["region"], aws_config["path"], os.path.dirname(local_folder), PREDICT_UPLOAD_WHITELIST)

        ssm_client = boto3.client(service_name='ssm', region_name=aws_config["region"])
        commands = [
        f"cd {remote_dir}/rudderstack-profiles-classifier",
        f"pip install -r requirements.txt",
        f"""python3 preprocess_and_predict.py --wh_creds '{json.dumps(creds)}' 
                                            --aws_config '{json.dumps(aws_config)}' 
                                            --json_output_filename {json_output_filename} 
                                            --inputs '{json.dumps(inputs)}' 
                                            --output_tablename {output_tablename} 
                                            --merged_config '{json.dumps(merged_config)}' 
                                            --prediction_task {prediction_task} 
                                            --udf_name {udf_name}""",
        ]
        self._execute(ssm_client, instance_id, commands, ssm_sleep_time)