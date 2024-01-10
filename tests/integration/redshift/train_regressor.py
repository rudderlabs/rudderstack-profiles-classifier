from train import *
import shutil

homedir = os.path.expanduser("~") 
with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
    creds = yaml.safe_load(f)["connections"]["shopify_wh_rs"]["outputs"]["dev"]

# creds = json.loads(os.environ["REDSHIFT_SITE_CONFIG"])
# creds["schema"] = "rs_profiles_3"


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

def validate_training_summary_regression():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "output/train_reports", "training_summary.json")
    with open(file_path, 'r') as file:
        json_data = json.load(file)
        timestamp = json_data['timestamp']
        assert isinstance(timestamp, str), f"Invalid timestamp - {timestamp}"
        assert timestamp, "Timestamp is empty"
        metrics = json_data['data']['metrics']
        keys = ['test', 'train', 'val']
        for key in keys:
            innerKeys = ["mean_absolute_error", "mean_squared_error", "r2_score"]
            for innerKey in innerKeys:
                assert metrics[key][innerKey], f"Invalid {innerKey} of {key} - ${metrics[key][innerKey]}"

def validate_reports_regression():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    reports_directory = os.path.join(current_dir, "output/train_reports")
    expected_files = ["01-feature-importance-chart", "02-residuals-chart-ltv_test", "03-deciles-plot-ltv_test"]
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


def test_regressor_training():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.join(current_dir, "sample_project")
    siteconfig_path = os.path.join(project_path, "siteconfig.yaml")
    output_filename = os.path.join(current_dir, "output/output.json")
    output_folder = os.path.join(current_dir, "output")

    os.makedirs(output_folder, exist_ok=True)

    config = {
      "data": {
        "features_profiles_model": "shopify_user_features",
        "inputs": ["packages/feature_table/models/shopify_user_features"],
        "eligible_users": "1=1",
        "label_column" : "days_since_last_seen",
        "task" : "regression"
      }
    }
    create_site_config_file(creds, siteconfig_path)

    folders = [os.path.join(output_folder, folder) for folder in os.listdir(output_folder) if os.path.isdir(os.path.join(output_folder, folder))]
    reports_folders = [folder for folder in folders if folder.endswith('_reports')]
    
    try:
        train(creds, None, output_filename, config, siteconfig_path, project_path)
        validate_training_summary_regression()
        validate_reports_regression()
    except Exception as e:
        raise e
    finally:
        cleanup_pb_project(project_path, siteconfig_path)
        cleanup_reports(reports_folders)

test_regressor_training()