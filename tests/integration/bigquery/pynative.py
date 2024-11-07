from tests.integration.utils import *
import os
import subprocess

# creds = json.loads(os.environ["SITE_CONFIG"])
creds = {
    "credentials": {
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "client_email": "rudderstack-service-test@rudderstacktestbq.iam.gserviceaccount.com",
        "client_id": "111481231866230448510",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/rudderstack-service-test%40rudderstacktestbq.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDdRI94ziCUCv98\nHoFpwrTa5TRQyk8l7NQ2dbxBBa+OQETCeZ77cxALkm1kQdCKKV14ygrtb5Ce+9d8\ng7hU//Bk/DcL9/SWBeg1psNjbUPYboUQkoIv7EuY6wHe66oXo5JXahr1leGoI8vs\n6xlL30/9y83WtZ5coVBDCLS020Fiu3/OfCdN2U8lG4IGQmCq0qYGeY0MwN0M2dl2\nIdGYBDUa4MkcKi4iJ8viqYnIaGVgkHB9bNKDbBqnzKH1NEIYlK5uVPcqLkBgCnQy\nZrnDuyiRwIorOV2DTm9z2dFLDiAcsKR1UbzPqD79KCQ+8+/BV9rKa9plKKzEf8PS\nxrq3v/qdAgMBAAECggEAMrzp+VWq+sea1idYnZvcROWSHYSGqbeo4eQViwN2wjPS\nkJW68wXrg/vLwdWlsEjT+kK6Rr/ydcJiI1dBCZy2zzFWnhP5rf0kxki7PS1tBVAy\nix5NZBfXLfWVtDxuAIWtpQXbsLzxxdCPjoBKiK9odFYrmTSEX9FNylFQvrgYQj83\nHE133M7apZfbDaW5S/nKRauDvr/ilamhXzKNFzxlgCkGJ22z76/DilXn4OeNBIIT\nGKAAXpN24pdRwNieqbeDb2o0Xer1Z/NKVxEi4pxqhZpok7V7mj1dmJGyDJA0FxI4\nlhDzxUB71P7N4oYdGvwy0XrTREX59bRigOitPJC0AQKBgQD/v9F22eadW9XV1HKy\n/so6pwtVngaX4LPi914HTbsw3hPvWw/CL9pHy1YBlDFx5fCLvYhLpuICad2qU5gt\nH4TOmvsjclJfSBuAKPYTh/oRkm0ZNQHBf30QUca0yHEgsAfZhnPvOUJ8hS2FMh5a\nzem7YS1r4oQpym2V3Pf8VplBwQKBgQDdfBbBcvKiuENG2HeXkNxc77gxIAhnSLHw\nE3JKXKhUxNTrdn4wTQBlMCwth9TdmvArKOk2I/PSgA8xAHzQc/LTyH0ggmGDJdJu\nlGKlwsAd2FWVpyPtyXlWlSyee1okTN3yhGgG2vEErM9wn+Eajb5trL353vNPmuiK\n8tLLpUH33QKBgFZABI5GkmXDUOxaR4xiLSV2rHXlY2fZGhGTRxzDdDvYyXRRi5D7\nvmu0AX9q0PoOh/84njyVPWd++Ii2xH2DQbDDx6p+pZUIpm2kYsjXdNh0P2Le44a0\nTiMw0QirCKvzcdJEa9jjwK5p454l9uK8yvSsozrdG6FUaXy/Fsr9EZaBAoGAUHJN\n4Z3au3eqGAVwCsE3CRqZEF7OLpWc69JkZNYa3g9QNuYnF2wghKBmq3L2wjcQnNyT\nOHL+kKqq2eWPcDdtL7dWm4Q/3t7R/BAxdHu0RCLbkyvORQQ06lnshvPO3fh9dSTa\nlALaIBSUlBe+L2Lkk/l2V1e/kF53sGBaaFVyDJECgYEAg1u3nWoT/rPxTfYwM8He\nBgkCeWARM+TmKpyZnlv/DAPK0g8ucoZUbU2YSZtGCvGlwMKRKnKduTO1ZNzflvsy\nljHoEpYM54eJpEXS1bG0zdjcsC9efNsGyRh7pol1ek9UUAjeAyAxOrgpvtUK+w0y\nNHOKDQkQ7iVXIuhEt2EgIZA=\n-----END PRIVATE KEY-----\n",
        "private_key_id": "5f4888127bfbf5aa848b29f245dcf371a8512b6e",
        "project_id": "rudderstacktestbq",
        "token_uri": "https://oauth2.googleapis.com/token",
        "type": "service_account",
    },
    "project_id": "rudderstacktestbq",
    "schema": "CLASSIFIER_INTEGRATION_TEST",
    "type": "bigquery",
    "user": "rudderstack-service-test@rudderstacktestbq.iam.gserviceaccount.com",
}
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
    # finally:
    #     cleanup_cmd = pb_cleanup_warehouse_tables(pynative_project, siteconfig_path)
    #     subprocess.run(f"yes | {cleanup_cmd}", shell=True, text=True)
    #     cleanup_pb_project(pynative_project, siteconfig_path)


run_project()
