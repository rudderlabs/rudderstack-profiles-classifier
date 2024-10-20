from tests.integration.utils import *
from train import *
import shutil
from predict import *
import time
import json
import os
import subprocess

creds = json.loads(os.environ["REDSHIFT_SITE_CONFIG"])
creds["schema"] = "classifier_integration_test"

os.makedirs(output_folder, exist_ok=True)

train_input_model_name = "shopify_user_features"

data = {
    "prediction_horizon_days": pred_horizon_days,
    "features_profiles_model": feature_table_name,
    "inputs": inputs,
    "eligible_users": eligible_users,
    "label_column": classifier_label_column,
    "task": "classification",
    "output_profiles_ml_model": output_model_name_classification,
}

train_config = {"data": data}

preprocessing = {"ignore_features": ["user_email", "first_name", "last_name"]}
predict_config = {
    "data": data,
    "preprocessing": preprocessing,
    "outputs": {
        "column_names": {
            "percentile": f"percentile_{output_model_name_classification}_{pred_horizon_days}_days",
            "score": f"{output_model_name_classification}_{pred_horizon_days}_days",
        },
        "feature_meta_data": {
            "features": [
                {
                    "description": "Percentile of churn score. Higher the percentile, higher the probability of churn",
                    "name": f"percentile_{output_model_name_classification}_{pred_horizon_days}_days",
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


def cleanup_reports(reports_folders):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for folder_name in reports_folders:
        folder_path = os.path.join(current_dir, folder_name)
        shutil.rmtree(folder_path)


def validate_training_summary():
    file_path = os.path.join(output_folder, "train_reports", "training_summary.json")
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
                assert metrics[key][
                    innerKey
                ], f"Invalid {innerKey} of {key} - ${metrics[key][innerKey]}"


def validate_reports():
    reports_directory = os.path.join(output_folder, "train_reports")
    expected_files = [
        "01-feature-importance-chart",
        "02-test-lift-chart",
        "03-test-pr-auc",
        "04-test-roc-auc",
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


def test_classification():
    st = time.time()

    create_site_config_file(creds, siteconfig_path)

    # Use os.path.join to get the full path for the output folder
    folders = [
        os.path.join(output_folder, folder)
        for folder in os.listdir(output_folder)
        if os.path.isdir(os.path.join(output_folder, folder))
    ]
    reports_folders = [folder for folder in folders if folder.endswith("_reports")]

    input_model_hash, latest_seq_no = get_latest_entity_var(
        creds, siteconfig_path, project_path, train_input_model_name
    )

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
        validate_training_summary()
        validate_reports()

        predict_inputs = train_inputs
        predict(
            creds,
            s3_config,
            output_filename,
            predict_inputs,
            p_output_tablename_classification,
            predict_config,
            runtime_info,
        )
        validate_predictions_df_classification(creds)

    except Exception as e:
        raise e
    finally:
        cleanup_cmd = pb_cleanup_warehouse_tables(project_path, siteconfig_path)
        subprocess.run(f"yes | {cleanup_cmd}", shell=True, text=True)
        cleanup_pb_project(project_path, siteconfig_path)
        cleanup_reports(reports_folders)

    et = time.time()
    # get the execution time
    elapsed_time = et - st
    print("Execution time:", elapsed_time, "seconds")


test_classification()
