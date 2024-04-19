from train import *
import shutil
from predict import *
from src.predictions.rudderstack_predictions.wht.rudderPB import RudderPB
from src.predictions.rudderstack_predictions.connectors.ConnectorFactory import (
    ConnectorFactory,
)
import json
from tests.integration.utils import create_site_config_file
import os

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
label_column = "days_since_last_seen"
inputs = [f"packages/{package_name}/models/{feature_table_name}"]
train_input_model_name = "shopify_user_features"
output_model_name = "ltv_regression_integration_test"
pred_horizon_days = 7
pred_column = f"{output_model_name}_{pred_horizon_days}_days".upper()
s3_config = {}
p_output_tablename = "test_run_can_delete_2"
entity_key = "user"
material_registry_table_name = "MATERIAL_REGISTRY_4"


data = {
    "prediction_horizon_days": pred_horizon_days,
    "features_profiles_model": feature_table_name,
    "inputs": inputs,
    "eligible_users": "1=1",
    "label_column": label_column,
    "task": "regression",
    "output_profiles_ml_model": output_model_name,
    "train_start_dt": "2024-02-08",
    "train_end_dt": "2024-02-09",
}

train_config = {"data": data}

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


def cleanup_pb_project(project_path, siteconfig_path):
    directories = ["migrations", "output"]
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
    file_path = os.path.join(
        current_dir, "output/train_reports", "training_summary.json"
    )
    with open(file_path, "r") as file:
        json_data = json.load(file)
        timestamp = json_data["timestamp"]
        assert isinstance(timestamp, str), f"Invalid timestamp - {timestamp}"
        assert timestamp, "Timestamp is empty"
        metrics = json_data["data"]["metrics"]
        keys = ["test", "train", "val"]
        for key in keys:
            innerKeys = ["mean_absolute_error", "mean_squared_error", "r2_score"]
            for innerKey in innerKeys:
                assert metrics[key][
                    innerKey
                ], f"Invalid {innerKey} of {key} - ${metrics[key][innerKey]}"


def validate_column_names_in_output_json():
    with open(output_filename, "r") as file:
        results = json.load(file)

    expected_keys = {
        "input_column_types": {
            "numeric": [],
            "categorical": [],
            "arraytype": [],
            "timestamp": [],
        },
        "ignore_features": [],
        "feature_table_column_types": {"numeric": [], "categorical": []},
    }

    for key, subkeys in expected_keys.items():
        assert (
            key in results["column_names"]
        ), f"Missing key: {key} in output json file."

        if subkeys:
            for subkey in subkeys:
                assert (
                    subkey in results["column_names"][key]
                ), f"Missing subkey {subkey} under key: {key} in output json file."


def validate_reports_regression():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    reports_directory = os.path.join(current_dir, "output/train_reports")
    expected_files = [
        "01-feature-importance-chart",
        "02-residuals-chart",
        "03-deciles-plot",
    ]
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


def validate_predictions_df():
    connector = ConnectorFactory.create("snowflake")
    session = connector.build_session(creds)
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        pred_column,
        "MODEL_ID",
        f"PERCENTILE_{pred_column}",
    ]

    try:
        df = connector.get_table_as_dataframe(session, p_output_tablename)
        columns_in_file = df.columns.tolist()
    except Exception as e:
        raise e

    # Check if the required columns are present
    if not set(required_columns).issubset(columns_in_file):
        missing_columns = set(required_columns) - set(columns_in_file)
        raise Exception(f"Miissing columns: {missing_columns} in predictions csv file.")

    session.close()
    return True


def test_regressor():
    connector = ConnectorFactory.create("snowflake")
    session = connector.build_session(creds)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.join(current_dir, "sample_project")
    siteconfig_path = os.path.join(project_path, "siteconfig.yaml")
    output_filename = os.path.join(current_dir, "output/output.json")
    output_folder = os.path.join(current_dir, "output")

    os.makedirs(output_folder, exist_ok=True)

    create_site_config_file(creds, siteconfig_path)

    folders = [
        os.path.join(output_folder, folder)
        for folder in os.listdir(output_folder)
        if os.path.isdir(os.path.join(output_folder, folder))
    ]
    reports_folders = [folder for folder in folders if folder.endswith("_reports")]

    latest_model_hash, entity_var_model_name = RudderPB().get_latest_material_hash(
        entity_key,
        siteconfig_path,
        project_path,
    )

    latest_seq_no = connector.get_latest_seq_no_from_registry(
        session, material_registry_table_name, latest_model_hash, entity_var_model_name
    )

    input_model_hash = connector.get_model_hash_from_registry(
        session, material_registry_table_name, train_input_model_name, latest_seq_no
    )

    # Closing the session immediately after use to avoid multiple open sessions conflict
    session.close()

    train_inputs = [
        f"""SELECT * FROM {creds['schema']}.material_{train_input_model_name}_{input_model_hash}_{latest_seq_no}""",
    ]
    runtime_info = {"site_config_path": siteconfig_path}

    try:
        train(
            creds,
            train_inputs,
            output_filename,
            train_config,
            siteconfig_path,
            project_path,
        )
        validate_training_summary_regression()
        validate_reports_regression()
        validate_column_names_in_output_json()

        with open(output_filename, "r") as f:
            results = json.load(f)

        material_table_name = results["config"]["material_names"][0][-1]
        predict_inputs = [
            f"SELECT * FROM {creds['schema']}.{material_table_name}",
        ]

        predict(
            creds,
            s3_config,
            output_filename,
            predict_inputs,
            p_output_tablename,
            predict_config,
            runtime_info,
        )
        validate_predictions_df()

    except Exception as e:
        raise e
    finally:
        cleanup_pb_project(project_path, siteconfig_path)
        cleanup_reports(reports_folders)


test_regressor()
