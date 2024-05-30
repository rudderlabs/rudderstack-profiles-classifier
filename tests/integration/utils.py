import os
import re
import yaml
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
        item for item in items if os.path.isdir(os.path.join(seq_no_dir, item)) and item != "latest"
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
    _validate_predictions_df(creds, required_columns)


def validate_predictions_df_classification(creds: dict):
    required_columns = [
        "USER_MAIN_ID",
        "VALID_AT",
        pred_column,
        "MODEL_ID",
        output_label,
        f"PERCENTILE_{pred_column}",
    ]
    _validate_predictions_df(creds, required_columns)


def _validate_predictions_df(creds: dict, required_columns):
    connector = ConnectorFactory.create(creds, current_dir)

    try:
        df = connector.get_table_as_dataframe(connector.session, p_output_tablename)
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
