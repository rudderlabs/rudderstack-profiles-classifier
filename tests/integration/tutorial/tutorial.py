from tests.integration.utils import *
from tempfile import TemporaryDirectory
import pexpect
import sys

# This is one approach that we can take to run integrations test for tutorial
# This is a hacky approach that is hard to maintain and can easily break
# Explore if there is a better approach to do this

# TODO: Uncomment for CI
# creds = json.loads(os.environ["SITE_CONFIG"])
# creds["schema"] = "classifier_integration_test"

tutorial_inputs = (
    "yes",
    # Entity Creation
    "user",
    # ID Types creations
    "anon_id",
    "email",
    "user_id",
    "device_id",
    "shopify_store_id",
    "shopify_customer_id",
    # Associating inputs with ID types
    #  Identifies
    "anon_id,email,shopify_store_id",
    "anonymous_id",
    "email",
    "shopify_store_id",
    #  Pages
    "anon_id,email,shopify_store_id",
    "anonymous_id",
    "email",
    "shopify_store_id",
    #  Tracks
    "anon_id,email,user_id,device_id,shopify_customer_id",
    "anonymous_id",
    "email",
    "user_id",
    "device_id",
    "shopify_customer_id",
    # Id Stitcher Creation
    "user_id_graph",
    "inputs/rsIdentifies_pb_tutorial",
    "inputs/rsPages_pb_tutorial",
    "inputs/rsTracks_pb_tutorial",
    "pb compile",
    # First Run
    "pb run",
    "",  # skip the > in first run explain
    # Second Run
    "pb run",
    "",
    "",  # skip the > in sql
    # Third Run
    "",  # use default here since we don't know the seq_no
    # Feature Creation
    "account_creation_date",
    "min(event_timestamp)",
    "inputs/rsIdentifies_pb_tutorial",
    "last_seen_timestamp",
    "last_value(event_timestamp)",
    "event_timestamp asc",
    "inputs/rsPages_pb_tutorial",
    "total_revenue",
    "sum(INVOICE_COST)",
    "inputs/rsTracks_pb_tutorial",
    "customers_by_email",
    "email",
    # Fourth Run
    "pb run",
)


def run_project():
    temp_dir = TemporaryDirectory(prefix="tutorial_integration_test")
    # TODO: Uncomment for CI
    # siteconfig_path = os.path.join(".pb", "siteconfig.yaml")
    # create_site_config_file(creds, siteconfig_path)
    process = None
    try:
        pb_args = ["pb", "tutorial", "-c", "test", "-f"]
        process = pexpect.spawn(
            " ".join(pb_args),
            cwd=temp_dir.name,
            encoding="utf-8",
            timeout=25 * 60,  # 25 minutes
        )
        process.logfile = sys.stdout
        for ti in tutorial_inputs:
            process.expect_exact("> ")
            process.sendline(ti)

        process.expect(pexpect.EOF)
        # TODO: assertion logic
        # verify the generated project files
        # Verify warehouse tables, if required
    except Exception as e:
        raise e
    finally:
        print("Cleaning up...")
        temp_dir.cleanup()
        # os.remove(siteconfig_path)
        if process:
            process.close()


run_project()
