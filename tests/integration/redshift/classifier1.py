import shutil
import time
import json
from tests.integration.utils import *
import os
import subprocess

creds = json.loads(os.environ["REDSHIFT_SITE_CONFIG"])
creds["schema"] = "PROFILES_INTEGRATION_TEST"


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


def validate_training_summary():
    file_path = os.path.join(
        pynative_output_folder, "train_reports", "training_summary.json"
    )
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
    reports_directory = os.path.join(pynative_output_folder, "train_reports")
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

    try:
        pb_args = [
            "pb",
            "run",
            "-p",
            pynative_project,
            "--migrate_on_load=True",
            "-c",
            siteconfig_path,
            "--seq_no",
            "68",
            "--concurrency",
            "20",
        ]
        subprocess.run(pb_args)
        validate_training_summary()
        validate_reports()
        validate_column_names_in_output_json()

    except Exception as e:
        raise e
    finally:
        cleanup_pb_project(project_path, siteconfig_path)
        folders = [
            os.path.join(pynative_output_folder, folder)
            for folder in os.listdir(pynative_output_folder)
            if os.path.isdir(os.path.join(pynative_output_folder, folder))
        ]
        reports_folders = [folder for folder in folders if folder.endswith("_reports")]
        cleanup_reports(reports_folders)

    et = time.time()
    # get the execution time
    elapsed_time = et - st
    print("Execution time:", elapsed_time, "seconds")


test_classification()
