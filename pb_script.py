import json
import sys
import os
import yaml 
os.chdir("../")

train_file_extension = ".json"
sys.path.append("../")
sys.path.append("./")
sys.path.append("./rudderstack_profiles_classifier")
from rudderstack_profiles_classifier import train as T
from rudderstack_profiles_classifier import predict as P
#import predict as P

if __name__ == "__main__":
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["dev_wh"]["outputs"]["dev"]

    credentials_presets = None
    p_output_tablename = 'test_run_can_delete_90'
    t_output_filename = 'rudderstack_profiles_classifier/output/dev/seq_no/8/train_output' + train_file_extension
    should_train = True
    site_config_path = os.path.join(homedir, ".pb/siteconfig.yaml")
    project_folder = '/Users/admin/Desktop/Profiles/rudderstack-profiles-shopify-churn'
    
    train_config = json.loads('{"data":{"eligible_users":"1=1","features_profiles_model":"rudder_user_base_features","inputs":["packages/feature_table/models/rudder_user_base_features"],"label_column":"days_since_last_seen","label_value":1,"output_profiles_ml_model":"shopify_churn","package_name":"feature_table","prediction_horizon_days":7},"preprocessing":{"ignore_features":["user_email","first_name","last_name"]}}')
    predict_config = json.loads('{"data":{"eligible_users":"1=1","features_profiles_model":"rudder_user_base_features","inputs":["packages/feature_table/models/rudder_user_base_features"],"label_column":"days_since_last_seen","label_value":1,"output_profiles_ml_model":"shopify_churn","package_name":"feature_table","prediction_horizon_days":7},"outputs":{"column_names":{"percentile":"percentile_churn_score_7_days","score":"churn_score_7_days"},"feature_meta_data":{"features":[{"description":"Percentile of churn score. Higher the percentile, higher the probability of churn","name":"percentile_churn_score_7_days"}]}},"preprocessing":{"ignore_features":["user_email","first_name","last_name"]}}')

    if should_train:
        T.train(creds, None, t_output_filename, train_config, site_config_path, project_folder)

    if credentials_presets is None:
        credentials_presets = {}

    s3_config = credentials_presets.get("s3", {})
    P.predict(creds, s3_config, t_output_filename, None, p_output_tablename, predict_config)
