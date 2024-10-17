from tests.integration.utils import *
import os
import subprocess

creds = json.loads(os.environ["SITE_CONFIG"])
creds["schema"] = "CLASSIFIER_INTEGRATION_TEST"


def run_project():
    create_site_config_file(creds, siteconfig_path)
    try:
        pb_args = [
            "pb",
            "run",
            "-p",
            pynative_project,
            "-c",
            siteconfig_path,
            "--migrate_on_load",
        ]
        subprocess.run(pb_args)
        assert_training_artefacts()
        validate_py_native_df_classification(creds)
        validate_py_native_df_regressor(creds)
    except Exception as e:
        raise e
    finally:
        cleanup_args = pb_cleanup_warehouse_tables(pynative_project, siteconfig_path)
        subprocess.run(cleanup_args)
        cleanup_pb_project(pynative_project, siteconfig_path)


run_project()
