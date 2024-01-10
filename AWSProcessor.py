import os
import yaml
import time
import json
import boto3
import constants
from logger import logger
from Processor import Processor
from typing import Any, List, Tuple, Union
from botocore.exceptions import NoCredentialsError

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

        logger.error("Error logs : ", output2)

    def _download_directory_from_s3(self, bucket_name, aws_region_name, s3_path, local_directory):
        s3 = boto3.client('s3', region_name=aws_region_name)
        try:
            objects = s3.list_objects(Bucket=bucket_name, Prefix=s3_path)['Contents']
            for obj in objects:
                key = obj['Key']
                local_file_path = os.path.join(local_directory, os.path.relpath(key, s3_path))
                if not os.path.exists(os.path.dirname(local_file_path)):
                    os.makedirs(os.path.dirname(local_file_path))
                s3.download_file(bucket_name, key, local_file_path)
                print(f"File {key} downloaded to {local_file_path}")
            print(f"All files from {bucket_name}/{s3_path} downloaded to {local_directory}")
        except NoCredentialsError:
            raise Exception(f"Couldn't find aws credentials in ec2 for uploading artefacts to s3")
        
    def _delete_directory_from_s3(self, bucket_name, aws_region_name, folder_name):
        s3 = boto3.client('s3', region_name=aws_region_name)
        try:
            objects = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']
            for obj in objects:
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Deleted object: {obj['Key']}")
            s3.delete_object(Bucket=bucket_name, Key=folder_name)
            print(f"Deleted folder: {folder_name}")
        except NoCredentialsError:
            logger.error("Couldn't find aws credentials in ec2 for uploading artefacts to s3")
            raise Exception(f"Couldn't find aws credentials in ec2 for uploading artefacts to s3")
        except Exception as e:
            logger.error(f"An error occured while trying to delete directory {bucket_name}/{folder_name} from s3: {e}")
            raise Exception(f"An error occured while trying to delete directory {bucket_name}/{folder_name} from s3: {e}")

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
        f"python3 preprocess_and_train.py --remote_dir {remote_dir} --s3_bucket {s3_bucket} --aws_region_name {aws_region_name} --s3_path {s3_path} --ec2_temp_output_json {ec2_temp_output_json} --material_names '{json.dumps(material_names)}' --merged_config '{json.dumps(merged_config)}' --prediction_task {prediction_task} --wh_creds '{json.dumps(wh_creds)}'"
        ]
        self._execute(ssm_client, instance_id, commands, ssm_sleep_time)

        self._download_directory_from_s3(s3_bucket, aws_region_name, s3_path, self.connector.get_local_dir())
        self._delete_directory_from_s3(s3_bucket, aws_region_name, s3_path)
        with open(os.path.join(self.connector.get_local_dir(), ec2_temp_output_json), 'r') as file:
            train_results_json = json.load(file)
        return train_results_json