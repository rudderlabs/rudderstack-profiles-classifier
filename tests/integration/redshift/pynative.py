import re
import shutil
import json
from tests.integration.utils import *
import os
import subprocess

creds = json.loads(os.environ["REDSHIFT_SITE_CONFIG"])
creds["schema"] = "profiles_integration_test"


def cleanup_pb_project(project_path, siteconfig_path):
    directories = ["migrations", "output"]
    for directory in directories:
        dir_path = os.path.join(project_path, directory)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
    os.remove(siteconfig_path)


def assert_training_artefacts():
    output_folder = get_pynative_output_folder()
    files = os.listdir(output_folder)
    regex = re.compile("Material_training_model_.+_training_file")
    for file in files:
        if regex.match(file):
            return True
    raise Exception("Training file in output folder not found")


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
            "--concurrency",
            "20",
        ]
        subprocess.run(pb_args)
        assert_training_artefacts()
    except Exception as e:
        raise e
    finally:
        cleanup_pb_project(project_path, siteconfig_path)


run_project()
