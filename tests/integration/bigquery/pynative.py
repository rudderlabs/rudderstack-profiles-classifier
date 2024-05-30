import json
from tests.integration.utils import *
import os
import subprocess

creds = json.loads(os.environ["SITE_CONFIG"])
creds["schema"] = "PROFILES_INTEGRATION_TEST"


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
            "2",
        ]
        subprocess.run(pb_args)
        assert_training_artefacts()
    except Exception as e:
        raise e
    finally:
        cleanup_pb_project(project_path, siteconfig_path)


run_project()