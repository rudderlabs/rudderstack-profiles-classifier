import os
import re
import json
import yaml
from src.predictions.profiles_mlcorelib.py_native.warehouse import standardize_ref_name
from src.predictions.profiles_mlcorelib.connectors.ConnectorFactory import (
    ConnectorFactory,
)
import shutil
from src.predictions.profiles_mlcorelib.wht.rudderPB import RudderPB


def get_pynative_output_folder():
    seq_no_dir = os.path.join(
        current_dir,
        "..",
        "..",
        "samples",
        "py_native",
        "output",
        connection_name,
        "seq_no",
    )
    if not os.path.exists(seq_no_dir):
        return None
    items = os.listdir(seq_no_dir)
    directories = [
        # This logic will fail if there are multiple sequence numbers
        item
        for item in items
        if os.path.isdir(os.path.join(seq_no_dir, item)) and item != "latest"
    ]
    return os.path.join(seq_no_dir, directories[0], "run")


current_dir = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.join(current_dir, "sample_project")
pynative_project = os.path.join(current_dir, "..", "..", "samples", "py_native")
connection_name = "test"
siteconfig_path = os.path.join(project_path, "siteconfig.yaml")
output_filename = os.path.join(current_dir, "output/output.json")
output_folder = os.path.join(current_dir, "output")
folder_path_output_file = os.path.dirname(output_filename)

package_name = "feature_table"
feature_table_name = "shopify_user_features"
eligible_users = "1=1"
package_name = "feature_table"
classifier_label_column = "is_churned_7_days"
regressor_label_column = "days_since_last_seen"
inputs = [f"packages/{package_name}/models/{feature_table_name}"]
s3_config = {}
pred_horizon_days = 7
output_model_name = "ltv_classification_integration_test"
pred_column = f"{output_model_name}_{pred_horizon_days}_days".upper()
output_label = "OUTPUT_LABEL"
p_output_tablename = "test_run_can_delete_2"
entity_key = "user"
material_registry_table_name = "MATERIAL_REGISTRY_4"


def create_site_config_file(creds, siteconfig_path):
    json_data = {
        "connections": {
            connection_name: {"target": "test", "outputs": {"test": creds}}
        },
        "py_models": {"credentials_presets": None},
    }
    yaml_data = yaml.dump(json_data, default_flow_style=False)
    with open(siteconfig_path, "w") as file:
        file.write(yaml_data)


def get_material_name(wh_type: str, regex: str):
    output_folder = get_pynative_output_folder()
    files = os.listdir(output_folder)
    regex = re.compile(regex)
    file_name = None
    for file in files:
        if regex.match(file):
            file_name = os.path.splitext(file)[0]
    return standardize_ref_name(wh_type, file_name)


def cleanup_pb_project(project_path, siteconfig_path):
    directories = ["migrations", "output"]
    for directory in directories:
        dir_path = os.path.join(project_path, directory)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
    os.remove(siteconfig_path)


def assert_training_artefacts():
    validate_reports()
    output_folder = get_pynative_output_folder()
    files = os.listdir(output_folder)
    model1Regex = re.compile("Material_traininG_model_.+_training_file")
    model2Regex = re.compile("Material_training_regression_model_.+_training_file")
    count = 0
    for file in files:
        if model1Regex.match(file):
            count = count + 1
        if model2Regex.match(file):
            count = count + 1
    if count != 2:
        raise Exception(f"{count} training files found in output folder. Expected 2.")


def validate_training_summary():
    output_folder = get_pynative_output_folder()
    file_path = os.path.join(output_folder, "train_reports", "training_summary.json")
    with open(file_path, "r") as file:
        json_data = json.load(file)
        timestamp = json_data["timestamp"]
        assert isinstance(timestamp, str), f"Invalid timestamp - {timestamp}"
        assert timestamp, "Timestamp is empty"
        metrics = json_data["data"]["metrics"]
        prob_th = metrics["prob_th"]
        assert 0 <= prob_th <= 1, f"Invalid prob_th - {prob_th}"
        assert prob_th, "prob_th is empty"
        threshold = json_data["data"]["threshold"]
        assert 0 <= threshold <= 1, f"Invalid threshold - {threshold}"
        assert threshold, "threshold is empty"
        keys = ["test", "train", "val"]
        for key in keys:
            innerKeys = [
                "f1_score",
                "pr_auc",
                "precision",
                "recall",
                "roc_auc",
                "users",
            ]
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
            "booleantype": [],
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


def validate_reports():
    output_folder = get_pynative_output_folder()
    reports_directory = os.path.join(output_folder, "train_reports")
    expected_files = [
        "01-feature-importance-chart-ltv_classification",
        "02-test-lift-chart-ltv_classification",
        "03-test-pr-auc-ltv_classification",
        "04-test-roc-auc-ltv_classification",
        "01-feature-importance-chart-ltv_regression",
        "02-residuals-chart-ltv_regression",
        "03-deciles-plot-ltv_regression",
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


def get_latest_entity_var(
    creds: dict, siteconfig_path: str, project_path: str, train_input_model_name: str
):
    connector = ConnectorFactory.create(creds, current_dir)

    latest_model_hash, entity_var_model_name = RudderPB().get_latest_material_hash(
        entity_key,
        siteconfig_path,
        project_path,
    )

    latest_seq_no = connector.get_latest_seq_no_from_registry(
        material_registry_table_name,
        latest_model_hash,
        entity_var_model_name,
    )
    input_model_hash = connector.get_model_hash_from_registry(
        material_registry_table_name, train_input_model_name, latest_seq_no
    )
    connector.post_job_cleanup()
    return input_model_hash, latest_seq_no


def validate_predictions_df_regressor(creds: dict):
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        pred_column,
        "MODEL_ID",
        f"PERCENTILE_{pred_column}",
    ]
    _validate_predictions_df(creds, required_columns, p_output_tablename)


def validate_py_native_df_regressor(creds: dict):
    table_name = get_material_name(
        creds["type"], "Material_prediction_regression_model_.+"
    )
    column_name = standardize_ref_name(creds["type"], "regression_days_since_last_seen")
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        column_name,
        "MODEL_ID",
        standardize_ref_name(creds["type"], f"PERCENTILE_{column_name}"),
    ]
    _validate_predictions_df(creds, required_columns, table_name)


def validate_predictions_df_classification(creds: dict):
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        pred_column,
        "MODEL_ID",
        output_label,
        f"PERCENTILE_{pred_column}",
    ]
    _validate_predictions_df(creds, required_columns, p_output_tablename)


def validate_py_native_df_classification(creds: dict):
    table_name = get_material_name(creds["type"], "Material_prediction_model_.+")
    column_name = standardize_ref_name(creds["type"], "classification_churn_7_days")
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        column_name,
        "MODEL_ID",
        output_label,
        standardize_ref_name(creds["type"], f"PERCENTILE_{column_name}"),
    ]
    _validate_predictions_df(creds, required_columns, table_name)


def _validate_predictions_df(creds: dict, required_columns, table_name: str):
    connector = ConnectorFactory.create(creds, current_dir)

    try:
        df = connector.get_table_as_dataframe(connector.session, table_name)
        columns_in_file = df.columns.tolist()
    except Exception as e:
        raise e
    required_columns_lower = [column.lower() for column in required_columns]
    columns_in_file_lower = [column.lower() for column in columns_in_file]

    # Check if the required columns are present
    if not set(required_columns_lower).issubset(columns_in_file_lower):
        missing_columns = set(required_columns_lower) - set(columns_in_file_lower)
        raise Exception(f"Miissing columns: {missing_columns} in predictions csv file.")

    connector.post_job_cleanup()
