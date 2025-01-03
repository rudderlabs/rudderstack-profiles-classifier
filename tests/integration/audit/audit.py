from tests.integration.utils import *
import os
import pexpect
import subprocess
from io import StringIO

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
    TIMEOUT = 100

    output_buffer = StringIO()
    child = pexpect.spawn(pb_audit_cmd, encoding="utf-8")
    child.logfile_read = output_buffer

    try:
        # Wait for visualization prompt
        child.expect("Enter an ID to visualize.*skip.*", timeout=TIMEOUT)
        child.sendline("skip")

        # TODO: Denying LLM consent for now. Uncomment the below code to test LLM once it's available.
        child.expect("Do you consent to LLM usage?.*no.*", timeout=TIMEOUT)
        child.sendline("no")

        # # give consent for LLM usage
        # child.expect("Do you consent to LLM usage?.*yes.*", timeout=TIMEOUT)
        # child.sendline("yes")

        # # Wait for LLM interactive mode
        # child.expect("Enter your question.*", timeout=TIMEOUT)
        # child.sendline("how many uniques id_types are present?")

        # # quit from LLM interactive mode
        # child.expect("Enter your question.*quit.*", timeout=TIMEOUT)
        # child.sendline("quit")

        # Wait for completion
        child.expect("Audit Completed Successfully.", timeout=TIMEOUT)

    except pexpect.TIMEOUT as e:
        print("\nTimeout occurred!")
        print("Complete output:")
        print(output_buffer.getvalue())
    except pexpect.EOF as e:
        print("\nEOF occurred!")
        print("Complete output:")
        print(output_buffer.getvalue())
    except Exception as e:
        print(f"\nUnexpected error: {type(e).__name__}: {str(e)}")
        print("Complete output:")
        print(output_buffer.getvalue())
    finally:
        process_output = output_buffer.getvalue()
        child.close()
        output_buffer.close()
        return process_output


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
            in response
        ):
            run_generate_material()
            response = run_audit()

        if "error" in response.lower():
            raise Exception(f"Audit failed with error: {response}")
    except Exception as e:
        raise e
    finally:
        cleanup_cmd = pb_cleanup_warehouse_tables(project_directory, siteconfig_path)
        subprocess.run(f"yes | {cleanup_cmd}", shell=True, text=True)
        cleanup_pb_project(project_directory, siteconfig_path)


run_project()
