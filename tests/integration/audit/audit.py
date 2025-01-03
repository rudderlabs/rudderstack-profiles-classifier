from tests.integration.utils import *
import os
import sys
import pexpect
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
    pb_audit_args = [
        "pb",
        "audit",
        "id_stitcher",
        "-p",
        project_directory,
        "-c",
        siteconfig_path,
        "--migrate_on_load",
    ]
    pb_audit_cmd = " ".join(pb_audit_args)
    TIMEOUT = 60

    child = pexpect.spawn(pb_audit_cmd, encoding="utf-8")
    child.logfile_read = sys.stdout

    try:
        # Wait for visualization prompt
        child.expect("Enter an ID to visualize.*skip.*", timeout=TIMEOUT)
        print("Sending skip...")
        child.sendline("skip")

        # give consent for LLM usage
        child.expect("Do you consent to LLM usage?*yes*", timeout=TIMEOUT)
        print("Sending yes...")
        child.sendline("yes")

        # Wait for LLM interactive mode
        child.expect("Enter your question.*", timeout=TIMEOUT)
        print("Sending LLM query...")
        child.sendline("how many uniques id_types are present?")

        # quit from LLM interactive mode
        child.expect("Enter your question.*quit*", timeout=TIMEOUT)
        print("Sending quit...")
        child.sendline("quit")

        # Wait for completion
        child.expect("Audit Completed Successfully.", timeout=TIMEOUT)
        print("Process completed successfully!")

    except pexpect.TIMEOUT as e:
        print("\nTimeout occurred!")
        print("Last received output:")
        print(child.before)
    except pexpect.EOF as e:
        print("\nEOF occurred!")
        print("Last received output:")
        print(child.before)
    except Exception as e:
        print(f"\nUnexpected error: {type(e).__name__}: {str(e)}")
        print("Last received output:")
        print(child.before)
    finally:
        child.close()


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
