from tests.integration.utils import *
import os
import subprocess

creds = json.loads(os.environ["SITE_CONFIG"])
creds["schema"] = "classifier_integration_test"


def run_project():
    project_directory = os.path.join("samples", "attribution_project")
    siteconfig_path = os.path.join(project_directory, "siteconfig.yaml")
    create_site_config_file(creds, siteconfig_path)
    try:
        pb_args = [
            "pb",
            "run",
            "-p",
            project_directory,
            "-c",
            siteconfig_path,
            "--concurrency",
            "20",
            "--migrate_on_load",
        ]
        response = subprocess.run(pb_args)
        if response.returncode != 0:
            raise Exception(f"Subprocess Error")
    except Exception as e:
        raise e
    finally:
        cleanup_pb_project(project_directory, siteconfig_path)
        cleanup_args = pb_cleanup_warehouse_tables(project_directory, siteconfig_path)
        subprocess.run(cleanup_args)


run_project()
