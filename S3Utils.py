import os
import boto3
from logger import logger
import constants
import configparser


class S3Utils:
    def download_directory(bucket_name, aws_region_name, s3_path, local_directory):
        s3 = boto3.client("s3", region_name=aws_region_name)
        S3Utils._download(bucket_name, s3_path, s3, local_directory)

    def _download(bucket_name, s3_path, client, local_directory):
        objects = client.list_objects(Bucket=bucket_name, Prefix=s3_path)["Contents"]
        for obj in objects:
            key = obj["Key"]
            if key == s3_path:
                logger.debug(f"Skipping object: key: {key}, s3_path: {s3_path}")
                continue
            local_file_path = os.path.join(
                local_directory, os.path.relpath(key, s3_path)
            )
            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))
            client.download_file(bucket_name, key, local_file_path)
            logger.debug(f"File {key} downloaded to {local_file_path}")
        logger.debug(
            f"All files from {bucket_name}/{s3_path} downloaded to {local_directory}"
        )

    def download_directory_using_keys(s3_config, local_directory):
        s3 = boto3.client(
            "s3",
            region_name=s3_config["region"],
            aws_access_key_id=s3_config["access_key_id"],
            aws_secret_access_key=s3_config["access_key_secret"],
            aws_session_token=s3_config["aws_session_token"],
        )
        S3Utils._download(s3_config["bucket"], s3_config["path"], s3, local_directory)

    def delete_directory(bucket_name, aws_region_name, folder_name):
        s3 = boto3.client("s3", region_name=aws_region_name)
        objects = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)["Contents"]
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            logger.debug(f"Deleted object: {obj['Key']}")
        s3.delete_object(Bucket=bucket_name, Key=folder_name)
        logger.debug(f"Deleted folder: {folder_name}")

    def upload_directory(bucket, aws_region_name, destination, path, allowedFiles):
        s3 = boto3.client("s3", region_name=aws_region_name)
        S3Utils._upload(bucket, destination, path, s3, allowedFiles)

    def _upload(bucket, destination, path, client, allowedFiles):
        for subdir, _, files in os.walk(path):
            for file in files:
                if file not in allowedFiles:
                    continue
                full_path = os.path.join(subdir, file)
                with open(full_path, "rb") as data:
                    s3_key = os.path.join(destination, subdir[len(path) + 1 :], file)
                    try:
                        client.upload_fileobj(data, bucket, s3_key)
                        logger.debug(f"File {full_path} uploaded to {bucket}/{s3_key}")
                    except FileNotFoundError:
                        raise Exception(
                            f"The file {full_path} was not found in ec2 while uploading trained files to s3."
                        )

    def upload_directory_using_keys(bucket, aws_region_name, destination, path, allowedFiles):
        credentials_file_path = os.path.join(constants.REMOTE_DIR, ".aws/credentials")
        if os.path.exists(credentials_file_path):
            config = configparser.ConfigParser()
            config.read(credentials_file_path)
            aws_access_key_id = config.get("default", "aws_access_key_id")
            aws_secret_access_key = config.get("default", "aws_secret_access_key")
            aws_session_token = config.get("default", "aws_session_token")
        else:
            raise Exception(f"Credentials file not found at {credentials_file_path}.")
        s3 = boto3.client(
            "s3",
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
        S3Utils._upload(bucket, destination, path, s3, allowedFiles)

    def get_temporary_credentials(role_arn: str):
        sts_client = boto3.client("sts")
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="ml_redshift_s3_access",
            DurationSeconds=900,  # min vale
        )
        credentials = response["Credentials"]
        return {
            "access_key_id": credentials["AccessKeyId"],
            "access_key_secret": credentials["SecretAccessKey"],
            "aws_session_token": credentials["SessionToken"],
        }
