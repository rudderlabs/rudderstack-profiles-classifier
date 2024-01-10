import os
import boto3
from logger import logger
import constants
import configparser

class S3Utils():
    def download_directory(bucket_name, aws_region_name, s3_path, local_directory):
        s3 = boto3.client('s3', region_name=aws_region_name)
        objects = s3.list_objects(Bucket=bucket_name, Prefix=s3_path)['Contents']
        for obj in objects:
            key = obj['Key']
            local_file_path = os.path.join(local_directory, os.path.relpath(key, s3_path))
            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))
            s3.download_file(bucket_name, key, local_file_path)
            print(f"File {key} downloaded to {local_file_path}")
        print(f"All files from {bucket_name}/{s3_path} downloaded to {local_directory}")

    def delete_directory(bucket_name, aws_region_name, folder_name):
        s3 = boto3.client('s3', region_name=aws_region_name)
        objects = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Deleted object: {obj['Key']}")
        s3.delete_object(Bucket=bucket_name, Key=folder_name)
        print(f"Deleted folder: {folder_name}")

    def upload_directory(bucket, aws_region_name, destination, path):
        s3 = boto3.client('s3', region_name=aws_region_name)
        S3Utils._upload(bucket, destination, path, s3)

    def _upload(bucket, destination, path, client):
        for subdir, _, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    s3_key = os.path.join(destination, subdir[len(path) + 1:], file)
                    try:
                        client.upload_fileobj(data, bucket, s3_key)
                        logger.debug(f"File {full_path} uploaded to {bucket}/{s3_key}")
                    except FileNotFoundError:
                        raise Exception(f"The file {full_path} was not found in ec2 while uploading trained files to s3.")

    def upload_directory_using_keys(bucket, aws_region_name, destination, path):
        credentials_file_path = os.path.join(constants.REMOTE_DIR, ".aws/credentials")
        if os.path.exists(credentials_file_path):
            config = configparser.ConfigParser()
            config.read(credentials_file_path)
            aws_access_key_id = config.get("default", "aws_access_key_id")
            aws_secret_access_key = config.get("default", "aws_secret_access_key")
            aws_session_token = config.get("default", "aws_session_token")
        else:
            raise Exception(f"Credentials file not found at {credentials_file_path}.")
        s3 = boto3.client('s3', region_name=aws_region_name, aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
        S3Utils._upload(bucket, destination, path, s3)