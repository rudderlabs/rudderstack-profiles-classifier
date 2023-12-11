import os 
import boto3
from botocore.exceptions import NoCredentialsError
import pathlib

def upload_directory(path, bucket, destination):
    s3 = boto3.client('s3')
    for subdir, _, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                s3_key = os.path.join(destination, subdir[len(path) + 1:], file)
                try:
                    s3.upload_fileobj(data, bucket, s3_key)
                    print(f"File {full_path} uploaded to {bucket}/{s3_key}")
                except FileNotFoundError:
                    print(f"The file {full_path} was not found")
                except NoCredentialsError:
                    print("Credentials not available")
                    
                    
if __name__ == "__main__":
    import sys 
    import argparse 
    import json
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--output_path', type=str)
    parser.add_argument("--s3_bucket", type=str)
    parser.add_argument("--s3_path", type=str)
    args = parser.parse_args()
     
    with open("credentials.json", "r") as f:
        creds = json.load(f)
    #creds = json.loads(args.credentials)
    
    pathlib.Path(args.output_path).mkdir(parents=True, exist_ok=True)
    from SnowflakeConnector import SnowflakeConnector
    connector = SnowflakeConnector()
    sess = connector.build_session(creds)
    t = sess.table("material_registry_4").toPandas()
    t.to_csv(os.path.join(args.output_path, 'data.csv'), index=False)
    with open(os.path.join(args.output_path,"test.txt"), "w") as f:
        f.write("Hello world")
        
    upload_directory(args.output_path, args.s3_bucket, args.s3_path)
