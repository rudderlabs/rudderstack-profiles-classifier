from train import *
import shutil
import time
from predict import * 

# homedir = os.path.expanduser("~") 
# with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
#     creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]

creds = json.loads(os.environ["SNOWFLAKE_SITE_CONFIG"])
creds["schema"] = "PROFILES_INTEGRATION_TEST"

current_dir = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.join(current_dir, "sample_project")
siteconfig_path = os.path.join(project_path, "siteconfig.yaml")
output_filename = os.path.join(current_dir, "output/output.json")
output_folder = os.path.join(current_dir, "output")

package_name = "feature_table"
feature_table_name = "shopify_user_features"
eligible_users = "1=1"
package_name = "feature_table"
label_column = "is_churned_7_days"
inputs = [f"packages/{package_name}/models/{feature_table_name}"]
output_model_name = "ltv_classification_integration_test"
pred_horizon_days = 7

s3_config = {}
p_output_tablename = 'test_run_can_delete_2'


data = {
        "prediction_horizon_days": pred_horizon_days,
        "features_profiles_model": feature_table_name,
        "inputs": inputs,
        "eligible_users": "1=1",
        "label_column" : label_column,
        "task" : "classification",
        "output_profiles_ml_model": output_model_name
    }

train_config = {
    "data" : data
}

preprocessing = {"ignore_features": ["user_email", "first_name", "last_name"]}
predict_config = {
    "data": data,
    "preprocessing": preprocessing,
    "outputs": {
        "column_names": {
            "percentile": f"percentile_{output_model_name}_{pred_horizon_days}_days",
            "score": f"{output_model_name}_{pred_horizon_days}_days",
        },
        "feature_meta_data": {
            "features": [
                {
                    "description": "Percentile of churn score. Higher the percentile, higher the probability of churn",
                    "name": f"percentile_{output_model_name}_{pred_horizon_days}_days",
                }
            ]
        },
    },
}


os.makedirs(output_folder, exist_ok=True)

def cleanup_pb_project(project_path, siteconfig_path):
    directories = ['migrations', 'output']
    for directory in directories:
        dir_path = os.path.join(project_path, directory)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
    os.remove(siteconfig_path)

def cleanup_reports(reports_folders):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for folder_name in reports_folders:
        folder_path = os.path.join(current_dir, folder_name)
        shutil.rmtree(folder_path)

def validate_training_summary():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "output/train_reports", "training_summary.json")
    with open(file_path, 'r') as file:
        json_data = json.load(file)
        timestamp = json_data['timestamp']
        assert isinstance(timestamp, str), f"Invalid timestamp - {timestamp}"
        assert timestamp, "Timestamp is empty"
        metrics = json_data['data']['metrics']
        prob_th = metrics['prob_th']
        assert 0<=prob_th<=1, f"Invalid prob_th - {prob_th}"
        assert prob_th, "prob_th is empty"
        threshold = json_data['data']['threshold']
        assert 0<=threshold<=1, f"Invalid threshold - {threshold}"
        assert threshold, "threshold is empty"
        keys = ['test', 'train', 'val']
        for key in keys:
            innerKeys = ["f1_score", "pr_auc", "precision", "recall", "roc_auc", "users"]
            for innerKey in innerKeys:
                assert metrics[key][innerKey], f"Invalid {innerKey} of {key} - ${metrics[key][innerKey]}"

def validate_reports():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    reports_directory = os.path.join(current_dir, "output/train_reports")
    expected_files = ["01-feature-importance-chart", "02-test-lift-chart", "03-test-pr-auc", "04-test-roc-auc"]
    files = os.listdir(reports_directory)
    missing_files = []
    for expected_file in expected_files:
        found = False
        for file_name in files:
            if expected_file in file_name:
                found = True
        if not found:
            missing_files.append(expected_file)
    if len(missing_files) > 0:
        raise Exception(f"{missing_files} not found in reports directory")

def create_site_config_file(creds, siteconfig_path):
    json_data = {
        "connections": {
            "test": {
                "target": "test",
                "outputs": {
                    "test": creds
                }
            }
        }
    }
    yaml_data = yaml.dump(json_data, default_flow_style=False)
    with open(siteconfig_path, 'w') as file:
        file.write(yaml_data)


def test_classification():
    st = time.time()

    create_site_config_file(creds, siteconfig_path)

    # Use os.path.join to get the full path for the output folder
    folders = [os.path.join(output_folder, folder) for folder in os.listdir(output_folder) if os.path.isdir(os.path.join(output_folder, folder))]
    reports_folders = [folder for folder in folders if folder.endswith('_reports')]

    try:
        train(creds, None, output_filename, train_config, siteconfig_path, project_path)
        validate_training_summary()
        validate_reports()
        
        with open(output_filename, "r") as f:
            results = json.load(f)

        print(results)
        model_hash = results["config"]["material_hash"]
        feature_table_name_from_train = results["input_model_name"]

        material_seq = 272
        predict_inputs = [f"SELECT * FROM PROFILES_INTEGRATION_TEST.Material_{feature_table_name_from_train}_{model_hash}_{material_seq}",]

        predict(creds, s3_config, output_filename, predict_inputs, p_output_tablename, predict_config)

    except Exception as e:
            raise e
    finally:
        cleanup_pb_project(project_path, siteconfig_path)
        cleanup_reports(reports_folders)

    et = time.time()
    # get the execution time
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')

test_classification()