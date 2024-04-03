import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yaml
import pathlib
import json
from dotenv import load_dotenv  # pip3 install python-dotenv

load_dotenv()

import train as T
import predict as P

from src.predictions.rudderstack_predictions.utils.logger import logger


if __name__ == "__main__":
    connection_name = os.getenv("SITE_CONN_NAME", None)
    training_task = os.getenv("TRAINING_TASK", None)
    env_name = os.getenv("SITE_ENV_NAME", None)
    label_column = os.getenv("LABEL_COLUMN", None)
    train_inputs = os.getenv("TRAIN_INPUTS", None)
    project_folder = os.getenv("PROJECT_FOLDER", "samples/application_project")
    feature_table_name = os.getenv("FEATURE_TABLE_NAME", "rudder_user_base_features")

    if (
        connection_name is None
        or training_task is None
        or env_name is None
        or label_column is None
        or train_inputs is None
    ):
        logger.error("One or more required environment variable(s) are not set")
        sys.exit(1)

    train_file_extension = ".json"
    eligible_users = "1=1"
    package_name = "feature_table"
    label_value = 1
    pred_horizon_days = 7
    p_output_tablename = "test_run_can_delete_2"
    t_output_filename = "output/dev/seq_no/1/train_output" + train_file_extension
    should_train = True
    entity_key = "user"
    output_model_name = "shopify_churn"
    inputs = [
        f"packages/{package_name}/models/{feature_table_name}"
    ]  # TODO: packages/{package_name} will be removed from input while running with 'samples/predictions_dev_project'

    homedir = os.path.expanduser("~")

    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"][connection_name]["outputs"][env_name]

    # End of user inputs.
    logger.setLevel("DEBUG")

    if creds["type"] == "snowflake":
        print(
            f"Using {creds['schema']} schema in snowflake account: {creds['account']}"
        )
    elif creds["type"] == "redshift":
        print(f"Using {creds['schema']} schema in Redshift account: {creds['host']}")
    elif creds["type"] == "bigquery":
        print(
            f"Using {creds['schema']} schema in BigQuery project: {creds['project_id']}"
        )
    else:
        raise Exception(f"Unknown database type: {creds['type']}")

    credentials_presets = None
    if should_train:
        print("Training step is enabled.")
    else:
        print("Skipping training as the should_train param is set to False")

    print(f"Training output file: {t_output_filename}")
    pathlib.Path(os.path.dirname(t_output_filename)).mkdir(parents=True, exist_ok=True)
    site_config_path = os.path.join(homedir, ".pb/siteconfig.yaml")

    data = {
        "task": training_task,
        "label_column": label_column,
        "label_value": label_value,
        "prediction_horizon_days": pred_horizon_days,
        "eligible_users": eligible_users,
        "features_profiles_model": feature_table_name,
        "inputs": inputs,
        "entity_key": entity_key,
        "output_profiles_ml_model": output_model_name,
        "package_name": package_name,
        "new_materialisations_config": {
            "feature_data_min_date_diff": 14,
            "strategy": "auto",
            "max_no_of_dates": 3,
        },
    }

    preprocessing = {"ignore_features": ["user_email", "first_name", "last_name"]}
    train_config = {
        "data": data,
        "preprocessing": preprocessing,
    }

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

    runtime_info = {
        "is_rudder_backend": False,
        "site_config_path": site_config_path,
    }
    try:
        train_inputs = train_inputs.split(",")
    except Exception as e:
        logger.error(f"Error parsing train inputs: {str(e)}")
        raise Exception(f"Error parsing train inputs: {str(e)}")

    logger.info(f"Training inputs: {train_inputs}")
    if should_train:
        T.train(
            creds,
            train_inputs,
            t_output_filename,
            train_config,
            site_config_path,
            project_folder,
            runtime_info,
        )

    if credentials_presets is None:
        credentials_presets = {}

    s3_config = credentials_presets.get("s3", {})

    # Read train results
    with open(t_output_filename, "r") as f:
        results = json.load(f)

    material_table_name = results["config"]["material_names"][0][-1]
    predict_inputs = [
        f"SELECT * FROM {creds['schema']}.{material_table_name}",
    ]
    print(f"Using table {material_table_name} for predictions")

    P.predict(
        creds,
        s3_config,
        t_output_filename,
        predict_inputs,
        p_output_tablename,
        predict_config,
        runtime_info,
    )
