import yaml
from src.predictions.rudderstack_predictions.connectors.ConnectorFactory import (
    ConnectorFactory,
)
from src.predictions.rudderstack_predictions.wht.rudderPB import RudderPB

package_name = "feature_table"
feature_table_name = "shopify_user_features"
eligible_users = "1=1"
package_name = "feature_table"
label_column = "is_churned_7_days"
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
        "connections": {"test": {"target": "test", "outputs": {"test": creds}}},
        "py_models": {"credentials_presets": None},
    }
    yaml_data = yaml.dump(json_data, default_flow_style=False)
    with open(siteconfig_path, "w") as file:
        file.write(yaml_data)


def get_latest_entity_var(creds: dict, siteconfig_path: str, project_path: str):
    connector = ConnectorFactory.create(creds)

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
    connector.post_job_cleanup()
    return entity_var_model_name, latest_model_hash, latest_seq_no


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
    connector = ConnectorFactory.create(creds)

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
