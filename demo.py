import boto3
import json
from typing import Any, List, Tuple, Union
from botocore.exceptions import NoCredentialsError

def execute(ssm_client, instance_id, commands):
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

    print("Error logs:===============")
    print(output2)
    print("Output logs:===============")
    print(output1)

def download_from_s3(bucket_name, folder_name, file_name):
    s3 = boto3.client('s3', region_name='us-east-1')
    s3_path = f"{folder_name}/{file_name}"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_path)
        json_data = response['Body'].read().decode('utf-8')
        data = json.loads(json_data)
        return data
    except NoCredentialsError:
        raise Exception(f"Not able to load object from file {bucket_name}/{s3_path}/{file_name}")

if __name__ == "__main__":
    remote_dir = '/home/ec2-user'
    instance_id = 'i-001c6544decab0fa3'
    output_json = "sample_dict_can_delete.json"
    s3_bucket = "ml-usecases-poc-srinivas"
    s3_path = "test_export_can_delete"

    ssm_client = boto3.client(service_name='ssm', region_name='us-east-1')
    commands = [
        f"cd {remote_dir}/experimental",
    f"python3 aws_script.py --s3_bucket {s3_bucket} --s3_path {s3_path} --output_json {output_json}"
    ]
    execute(ssm_client, instance_id, commands)

    train_results_json = download_from_s3(s3_bucket, s3_path, output_json)
    print(train_results_json)