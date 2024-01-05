import os
import yaml
import time
import json
import boto3
from logger import logger
from Processor import Processor
from typing import Any, List, Tuple, Union
from botocore.exceptions import NoCredentialsError

class AWSProcessor(Processor):
    def _execute(self, ssm_client, instance_id, commands):
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': commands},
        )
        command_id = response['Command']['CommandId']
        print(f"Command ID: {command_id}; command - ", commands)
        output1 = ""
        output2 = ""
        while True:
            result = ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id,
            )
            # sleep for few seconds
            if result['Status'] in ['Success', 'Failed', 'Cancelled']:
                output1 += result.get('StandardOutputContent', '')
                output2 += result.get('StandardErrorContent', '')
                break

        print("Error logs:")
        print(output2)

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
            raise Exception(f"Not able to list or download objects from folder {bucket_name}/{s3_path}")

    def train(self, train_procedure, material_names: List[Tuple[str]], merged_config: dict, prediction_task: str, wh_creds: dict, run_id: str):
        remote_dir = '/home/ec2-user'
        instance_id = 'i-001c6544decab0fa3'
        output_json = "train_results.json"
        s3_bucket = "ml-usecases-poc-srinivas"
        region_name='us-east-1'
        s3_path = "test_export/"
        folder_name = run_id
        venv_name = f"pysnowpark_{folder_name}"

        ssm_client = boto3.client(service_name='ssm', region_name='us-east-1')
        commands = [
        f"cd {remote_dir}/rudderstack-profiles-classifier",
        f"git pull",
        f"cd ..",
        f"cp -r rudderstack-profiles-classifier/ {folder_name}",
        f"cd {folder_name}",
        f"python3.9 -m venv {venv_name}",
        f"source {venv_name}/bin/activate",
        f"pip install -r requirements.txt",
        f"python3 preprocess_and_train.py --remote_dir {remote_dir} --s3_bucket {s3_bucket} --region_name {region_name} --s3_path {s3_path} --output_json {output_json} --material_names '{json.dumps(material_names)}' --merged_config '{json.dumps(merged_config)}' --prediction_task {prediction_task} --wh_creds '{json.dumps(wh_creds)}'",
        f"deactivate",
        f"rm -rf {venv_name}",
        f"cd ..",
        f"rm -rf {folder_name}"
        ]
        self._execute(ssm_client, instance_id, commands)

        self._download_directory_from_s3(s3_bucket, region_name, s3_path, self.connector.get_local_dir())
        self._delete_directory_from_s3(s3_bucket, region_name, s3_path)
        with open(os.path.join(self.connector.get_local_dir(), output_json), 'r') as file:
            train_results_json = json.load(file)
        return train_results_json