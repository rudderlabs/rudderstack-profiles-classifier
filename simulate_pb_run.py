import sys
import os
import yaml
import pathlib

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import train as T
import predict as P

if __name__ == "__main__":
    train_file_extension = ".json"
    schema = "rs360"
    project_folder = '../rudderstack-profiles-shopify-churn'
    feature_table_name = 'shopify_user_features'
    eligible_users = '1=1'
    package_name = 'feature_table'
    label_column = 'is_churned_7_days'
    label_value = 1
    pred_horizon_days = 7
    output_model_name = 'shopify_churn'
    inputs = f'packages/{package_name}/models/{feature_table_name}'
    p_output_tablename = 'test_run_can_delete_2'
    t_output_filename = 'output/dev/seq_no/1/train_output' + train_file_extension
    should_train = True
    material_seq = 2508 # seq no of most recent material from pb run
    model_hash = '43619d31' # hash of the feature_table_name from current pb run
    json_argument = {'is_rudder_backend': False}
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"][schema]["outputs"]["dev"]
        
    # End of user inputs.   
    
    from logger import logger
    logger.setLevel("DEBUG")
    
    if creds['type'] == 'snowflake':
        print(f"Using {creds['schema']} schema in snowflake account: {creds['account']}")
    elif creds['type'] == 'redshift':
        print(f"Using {creds['schema']} schema in Redshift account: {creds['host']}")
    else:
        raise Exception(f"Unknown database type: {creds['type']}")

    credentials_presets = None
    if should_train:
        print("Training step is enabled.")
    else:
        print("Skipping training as the shoud_train param is set to False")
        
    print(f"Training output file: {t_output_filename}")
    pathlib.Path(os.path.dirname(t_output_filename)).mkdir(parents=True, exist_ok=True)
    site_config_path = os.path.join(homedir, ".pb/siteconfig.yaml")
    project_folder = os.path.abspath(project_folder)
    
    data = {"label_column": label_column, 
            "label_value": label_value,
            "prediction_horizon_days": pred_horizon_days, 
            "eligible_users": eligible_users,
            "features_profiles_model": feature_table_name, 
            "inputs": [inputs], 
            "output_profiles_ml_model": output_model_name, 
            "package_name": package_name}
    preprocessing = {"ignore_features":["user_email","first_name","last_name"]}
    train_config = {"data": data,
                    "preprocessing":preprocessing}
    predict_config = {"data": data,
                      "preprocessing":preprocessing,
                      "outputs":{"column_names":{"percentile":f"percentile_{output_model_name}_{pred_horizon_days}_days",
                                                 "score": f"{output_model_name}_{pred_horizon_days}_days"},
                                 "feature_meta_data":
                                     {"features":[{"description":"Percentile of churn score. Higher the percentile, higher the probability of churn","name":f"percentile_{output_model_name}_{pred_horizon_days}_days"}]}
                                     }
                      }
    
    runtime_info = {'is_rudder_backend': False}
    if should_train:
        T.train(
            creds,
            None,
            t_output_filename,
            train_config,
            site_config_path,
            project_folder,
            runtime_info
        )

    if credentials_presets is None:
        credentials_presets = {}

    s3_config = credentials_presets.get("s3", {})
    predict_inputs = [f"SELECT * FROM {schema}.Material_{feature_table_name}_{model_hash}_{material_seq}",]
    print(f"Using table Material_{feature_table_name}_{model_hash}_{material_seq} for predictions")
    P.predict(creds, s3_config, t_output_filename, predict_inputs, p_output_tablename, predict_config)
