import boto3
import json
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import WaiterError

def upload_to_s3(bucket_name, folder_name, file_name, data):

    print("creating client")
    s3 = boto3.client('s3', region_name='us-east-1')

    print("converting to json")
    json_data = json.dumps(data)

    print("creating s3 path")
    s3_path = f"{folder_name}/{file_name}"

    print("json data ==============",json_data)
    print("bucket name ================",bucket_name)
    print("s3 path================",s3_path)
    
    try:
        print("putting the object")
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=s3_path)
        print("object uploaded successfully.")
    except:
        raise Exception(f"Not working.")
    print("success")
    # try:
    #     obj = s3.Object(bucket_name, s3_path)
    #     obj.wait_until_exists()
    #     print(f"{file_name} exists in {bucket_name}/{folder_name}")
    # except WaiterError as e:
    #     print(f"Error waiting for {file_name} in {bucket_name}/{folder_name}: {e}")
    
if __name__ == "__main__":
    import sys 
    import argparse
    
    parser = argparse.ArgumentParser()

    parser.add_argument("--s3_bucket", type=str)
    parser.add_argument("--s3_path", type=str)
    parser.add_argument("--output_json", type=str)
    args = parser.parse_args()

    data = {"config": {"training_dates": [["2023-12-21 00:00:00", "2023-12-28 00:00:00"]], "material_names": [["material_shopify_user_features_d10140c3_188", "material_shopify_user_features_d10140c3_189"]], "material_hash": "d10140c3"}, "model_info": {"file_location": {"stage": None, "file_name": "shopify_churn_classifier.joblib"}, "model_id": "1704195698", "threshold": 0.62}, "input_model_name": "shopify_user_features"}
    upload_to_s3(args.s3_bucket, args.s3_path, args.output_json, data)