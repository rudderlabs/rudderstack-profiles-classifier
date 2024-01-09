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
    def _execute(self, ssm_client, instance_id, commands, sleepTime):
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
            time.sleep(sleepTime)
            if result['Status'] in ['Success', 'Failed', 'Cancelled']:
                output1 += result.get('StandardOutputContent', '')
                output2 += result.get('StandardErrorContent', '')
                break

        logger.error("Error logs : ", output2)

    def _download_directory_from_s3(self, bucket_name, region_name, s3_path, local_directory):
        s3 = boto3.client('s3', region_name=region_name)
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
            raise Exception(f"Credentials not available")
        
    def _delete_directory_from_s3(self, bucket_name, region_name, folder_name):
        s3 = boto3.client('s3', region_name=region_name)
        try:
            objects = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']
            for obj in objects:
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Deleted object: {obj['Key']}")
            s3.delete_object(Bucket=bucket_name, Key=folder_name)
            print(f"Deleted folder: {folder_name}")
        except NoCredentialsError:
            print("Credentials not available")
        except Exception as e:
            print(f"An error occurred: {e}")

    def train(self, train_procedure, material_names: List[Tuple[str]], merged_config: dict, prediction_task: str, wh_creds: dict, run_id: str):
        remote_dir = constants.REMOTE_DIR
        instance_id = constants.INSTANCE_ID
        sleepTime = constants.SLEEPTIME
        ec2_temp_output_json = constants.EC2_TEMP_OUTPUT_JSON
        s3_bucket = constants.S3_BUCKET
        region_name = constants.REGION_NAME
        s3_path = f"test_export_{run_id}/"
        folder_name = run_id
        venv_name = f"pysnowpark_{folder_name}"

        ssm_client = boto3.client(service_name='ssm', region_name=region_name)
        commands = [
        f"cd {remote_dir}/rudderstack-profiles-classifier",
        f"git pull",
        f"cd ..",
        f"cp -r rudderstack-profiles-classifier/ {folder_name}",
        f"cd {folder_name}",
        f"python3 -m venv {venv_name}",
        f"source {venv_name}/bin/activate",
        f"pip install -r requirements.txt",
        f"python3 preprocess_and_train.py --remote_dir {remote_dir} --s3_bucket {s3_bucket} --region_name {region_name} --s3_path {s3_path} --ec2_temp_output_json {ec2_temp_output_json} --material_names '{json.dumps(material_names)}' --merged_config '{json.dumps(merged_config)}' --prediction_task {prediction_task} --wh_creds '{json.dumps(wh_creds)}'",
        f"deactivate",
        f"rm -rf {venv_name}",
        f"cd ..",
        f"rm -rf {folder_name}"
        ]
        self._execute(ssm_client, instance_id, commands, sleepTime)

        self._download_directory_from_s3(s3_bucket, region_name, s3_path, self.connector.get_local_dir())
        self._delete_directory_from_s3(s3_bucket, region_name, s3_path)
        with open(os.path.join(self.connector.get_local_dir(), ec2_temp_output_json), 'r') as file:
            train_results_json = json.load(file)
        return train_results_json