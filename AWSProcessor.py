import os
import yaml
import time
import json
import boto3
from logger import logger
from Processor import Processor
from typing import Any, List, Tuple, Union

class AWSProcessor(Processor):
    def execute(self, ssm_client, instance_id, commands):
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

    def train(self, train_procedure, material_names: List[Tuple[str]], merged_config: dict, prediction_task: str, wh_creds: dict):
        remote_dir = '/home/ec2-user'
        instance_id = 'i-001c6544decab0fa3'
        output_path = "output"
        s3_bucket = "ml-usecases-poc"
        s3_path = "test_export"

        ssm_client = boto3.client(service_name='ssm', region_name='us-east-1')
        commands = [
        f"cd {remote_dir}/rudderstack-profiles-classifier",
        f"pip install -r requirements.txt",
        f"python3 preprocess_and_train.py --output_path {output_path} --s3_bucket {s3_bucket} --s3_path {s3_path} --material_names '{json.dumps(material_names)}' --merged_config '{json.dumps(merged_config)}' --prediction_task {prediction_task} --wh_creds '{json.dumps(wh_creds)}'"
        ]
        self.execute(ssm_client, instance_id, commands)

        return None