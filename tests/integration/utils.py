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
    base_dir = os.path.join(
        current_dir,
        "..",
        "..",
        "samples",
        "py_native",
    )
    seq_no_dir = None
    for root, dirs, _ in os.walk(base_dir):
        if "seq_no" in dirs:
            seq_no_dir = os.path.join(root, "seq_no")
            break
    if not seq_no_dir:
        raise Exception("seq_no directory not found")
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
eligible_users = "*"
package_name = "feature_table"
classifier_label_column = "is_churned_7_days"
regressor_label_column = "days_since_last_seen"
inputs = [f"packages/{package_name}/models/{feature_table_name}"]
s3_config = {}
pred_horizon_days = 7
output_model_name_classification = "prediction_model"
output_model_name_regression = "prediction_regression_model"
pred_column_classification = (
    f"{output_model_name_classification}_{pred_horizon_days}_days".upper()
)
pred_column_regression = (
    f"{output_model_name_regression}_{pred_horizon_days}_days".upper()
)
output_label = "OUTPUT_LABEL"
p_output_tablename_classification = "classification_test_run_can_delete_1"
p_output_tablename_regression = "regression_test_run_can_delete_1"
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


def get_directory_name(regex: str):
    output_folder = get_pynative_output_folder()
    entries = os.listdir(output_folder)
    directories = [
        entry for entry in entries if os.path.isdir(os.path.join(output_folder, entry))
    ]
    compiledRegex = re.compile(regex)
    for file in directories:
        if compiledRegex.match(file):
            return file
    print(output_folder)
    print(directories)
    raise Exception(f"Material for {regex} not found")


def get_file_name(regex: str):
    output_folder = get_pynative_output_folder()
    entries = os.listdir(output_folder)
    compiledRegex = re.compile(regex)
    for file in entries:
        if compiledRegex.match(file):
            return os.path.splitext(file)[0]
    raise Exception(f"Material for {regex} not found")


def cleanup_pb_project(project_path, siteconfig_path):
    directories = ["migrations", "output"]
    for directory in directories:
        dir_path = os.path.join(project_path, directory)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
    os.remove(siteconfig_path)


def pb_cleanup_warehouse_tables(project_path, siteconfig_path):
    cleanup_command = " ".join(
        [
            "pb",
            "cleanup",
            "materials",
            "-p",
            project_path,
            "-c",
            siteconfig_path,
            "--migrate_on_load=True",
            "--retention_time_in_days",
            "4",
        ]
    )
    return cleanup_command


def assert_training_artefacts():
    output_folder = get_pynative_output_folder()
    models = [
        {
            "regex": "Material_propensity_model_training_.+",
            "reports": [
                "01-feature-importance-chart",
                "02-test-lift-chart",
                "03-test-pr-auc",
                "04-test-roc-auc",
            ],
            "classification": True,
        },
        {
            "regex": "Material_training_Regression_model_.+",
            "reports": [
                "01-feature-importance-chart",
                "02-residuals-chart",
                "03-deciles-plot",
            ],
            "classification": False,
        },
    ]
    for model in models:
        material_directory = get_directory_name(model["regex"])
        training_file_path = os.path.join(
            output_folder, material_directory, "training_file"
        )
        training_reports_path = os.path.join(
            output_folder,
            material_directory,
            "training_reports",
        )
        if model["classification"]:
            validate_training_summary(
                os.path.join(training_reports_path, "training_summary.json")
            )
        else:
            validate_training_summary_regression(
                os.path.join(training_reports_path, "training_summary.json")
            )
        validate_reports(training_reports_path, model["reports"])


def validate_training_summary(file_path: str):
    with open(file_path, "r") as file:
        json_data = json.load(file)
        timestamp = json_data["timestamp"]
        assert isinstance(timestamp, str), f"Invalid timestamp - {timestamp}"
        assert timestamp, "Timestamp is empty"
        metrics = json_data["data"]["metrics"]
        prob_th = metrics["prob_th"]
        assert 0 <= prob_th <= 1, f"Invalid prob_th - {prob_th}"
        threshold = json_data["data"]["threshold"]
        assert 0 <= threshold <= 1, f"Invalid threshold - {threshold}"
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
                assert (
                    metrics[key][innerKey] is not None
                ), f"Invalid {innerKey} of {key} - ${metrics[key][innerKey]}"


def validate_training_summary_regression(file_path: str):
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
                assert (
                    metrics[key][innerKey] is not None
                ), f"Invalid {innerKey} of {key} - ${metrics[key][innerKey]}"


def validate_reports(directory: str, expected_files: list[str]):
    files = os.listdir(directory)
    missing_files = []
    for expected_file in expected_files:
        found = False
        for file_name in files:
            if expected_file in file_name:
                found = True
        if not found:
            missing_files.append(expected_file)
    if len(missing_files) > 0:
        raise Exception(f"{missing_files} not found in {directory}")


def get_latest_model_hash(
    model_name: str,
    site_config_path: str = None,
    project_folder: str = None,
):
    args = {
        "project_folder": project_folder,
        "site_config_path": site_config_path,
    }
    pb_compile_output = RudderPB()._compile(args)
    try:
        model_hash = pb_compile_output[
            pb_compile_output.index(model_name) + len(model_name) :
        ].split("_")[1]
    except ValueError:
        raise Exception(
            f"Could not find entity-var-model '{model_name}' in the output of pb compile command: {pb_compile_output}"
        )
    return model_hash


def get_latest_entity_var(
    creds: dict, siteconfig_path: str, project_path: str, train_input_model_name: str
):
    connector = ConnectorFactory.create(creds, current_dir)

    latest_model_hash = get_latest_model_hash(
        train_input_model_name,
        siteconfig_path,
        project_path,
    )

    latest_seq_no = connector.get_latest_seq_no_from_registry(
        material_registry_table_name,
        latest_model_hash,
        train_input_model_name,
    )

    connector.post_job_cleanup()
    return latest_model_hash, latest_seq_no


def validate_predictions_df_regressor(creds: dict):
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        pred_column_regression,
        "MODEL_ID",
        f"PERCENTILE_{pred_column_regression}",
    ]
    _validate_predictions_df(creds, required_columns, p_output_tablename_regression)


def validate_py_native_df_regressor(creds: dict):
    material_name = get_file_name("Material_prediction_regression_model_.+")
    table_name = standardize_ref_name(creds["type"], material_name)
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
        pred_column_classification,
        "MODEL_ID",
        output_label,
        f"PERCENTILE_{pred_column_classification}",
    ]
    _validate_predictions_df(creds, required_columns, p_output_tablename_classification)


def validate_py_native_df_classification(creds: dict):
    material_name = get_file_name("Material_propensity_model_prediction_.+")
    table_name = standardize_ref_name(creds["type"], material_name)
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
