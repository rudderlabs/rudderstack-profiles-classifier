from tests.integration.utils import *
import os
import subprocess

input_schema = "classifier_integration_test"
creds = json.loads(os.environ["SITE_CONFIG"])

if creds["type"] in ("snowflake", "redshift"):
    creds["schema"] = input_schema
elif creds["type"] in ("bigquery"):
    creds["schema"] = input_schema.upper()
else:
    raise Exception("Unsupported warehouse for audit test.")

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
        pb_args,
        input="skip\n yes\n how many uniques id_types are present?\n quit\n",
        text=True,
        capture_output=True,
    )
    if response.returncode != 0:
        raise Exception(f"Subprocess Error")
    return response


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
        response = run_audit()
        if (
            "An error occurred while running the audit: no valid run found for the id stitcher model"
            in response.stdout
        ):
            run_generate_material()
            response = run_audit()
        else:
            print(response.stdout)
    except Exception as e:
        raise e
    finally:
        cleanup_cmd = pb_cleanup_warehouse_tables(project_directory, siteconfig_path)
        subprocess.run(f"yes | {cleanup_cmd}", shell=True, text=True)
        cleanup_pb_project(project_directory, siteconfig_path)


run_project()
