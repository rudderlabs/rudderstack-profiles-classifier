import os
import boto3
from ..utils.logger import logger
import re


class S3Utils:
    def download_directory(s3_config, local_directory):
        client = boto3.client("s3", region_name=s3_config["region"])
        bucket_name = s3_config["bucket"]
        s3_path = s3_config["path"]
        objects = client.list_objects(Bucket=bucket_name, Prefix=s3_path)["Contents"]
        for obj in objects:
            key = obj["Key"]
            if key == s3_path:
                logger.get().debug(f"Skipping object: key: {key}, s3_path: {s3_path}")
                continue
            local_file_path = os.path.join(
                local_directory, os.path.relpath(key, s3_path)
            )
            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))
            client.download_file(bucket_name, key, local_file_path)
            logger.get().debug(f"File {key} downloaded to {local_file_path}")
        logger.get().debug(
            f"All files from {bucket_name}/{s3_path} downloaded to {local_directory}"
        )

    def delete_directory(bucket_name, aws_region_name, folder_name):
        s3 = boto3.client("s3", region_name=aws_region_name)
        objects = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)["Contents"]
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            logger.get().debug(f"Deleted object: {obj['Key']}")
        s3.delete_object(Bucket=bucket_name, Key=folder_name)
        logger.get().debug(f"Deleted folder: {folder_name}")

    def download_training_artifacts(
        bucket_name,
        region,
        prefix,
        download_directory,
        folder_substring,
        min_creation_time,
    ):
        s3 = boto3.client("s3", region_name=region)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        for obj in response.get("Contents", []):
            creation_time = obj["LastModified"]
            is_recipe_file = folder_substring in os.path.basename(
                obj["Key"]
            )  # To avoid downloading top level training recipe which is created by the describe function
            if (
                creation_time > min_creation_time
                and folder_substring in obj["Key"]
                and not is_recipe_file
            ):
                file_key = obj["Key"]
                match = re.search(f"(.*?{re.escape(folder_substring)}[^/]*)", file_key)
                if match:
                    path_to_download = (
                        match.group(1) + "/"
                    )  # Adding trailing slash so that the download function ignores any top level file matching the prefix
                    config = {
                        "region": region,
                        "bucket": bucket_name,
                        "path": path_to_download,
                    }
                    logger.get().info(
                        f"Downloading training artifacts from {path_to_download} to {download_directory}"
                    )
                    S3Utils.download_directory(config, download_directory)
                    return True
        return False

    def upload_directory(bucket, aws_region_name, destination, path, allowedFiles):
        client = boto3.client("s3", region_name=aws_region_name)
        for subdir, _, files in os.walk(path):
            for file in files:
                if file not in allowedFiles:
                    continue
                full_path = os.path.join(subdir, file)
                with open(full_path, "rb") as data:
                    s3_key = os.path.join(destination, subdir[len(path) + 1 :], file)
                    try:
                        client.upload_fileobj(data, bucket, s3_key)
                        logger.get().debug(
                            f"File {full_path} uploaded to {bucket}/{s3_key}"
                        )
                    except FileNotFoundError:
                        raise Exception(
                            f"The file {full_path} was not found in ec2 while uploading trained files to s3."
                        )

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
