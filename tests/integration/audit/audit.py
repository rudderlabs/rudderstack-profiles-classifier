from tests.integration.utils import *
import os
import subprocess

creds = json.loads(os.environ["SITE_CONFIG"])
creds["schema"] = "classifier_integration_test"

project_directory = os.path.join("samples", "integration_test_project")
siteconfig_path = os.path.join(project_directory, "siteconfig.yaml")
create_site_config_file(creds, siteconfig_path)


def run_audit():
    pb_args = [
        "pb",
        "audit",
        "id_stitcher",
        "-p",
        project_directory,
        "-c",
        siteconfig_path,
        "--migrate_on_load",
    ]
    response = subprocess.run(
        pb_args, input="skip\n how many uniques id_types are present?\n quit\n".encode()
    )
    if response.returncode != 0:
        raise Exception(f"Subprocess Error")


def run_generate_material():
    pb_args = [
        "pb",
        "run",
        "-p",
        project_directory,
        "-c",
        siteconfig_path,
        "--migrate_on_load",
    ]
    response = subprocess.run(pb_args)
    if response.returncode != 0:
        raise Exception(f"Subprocess Error")


def run_project():
    try:
        run_audit()
    except:
        try:
            run_generate_material()
            run_audit()
        except Exception as e:
            raise e
    finally:
        cleanup_cmd = pb_cleanup_warehouse_tables(project_directory, siteconfig_path)
        subprocess.run(f"yes | {cleanup_cmd}", shell=True, text=True)
        cleanup_pb_project(project_directory, siteconfig_path)


run_project()
