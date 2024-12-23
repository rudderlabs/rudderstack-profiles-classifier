import os
import json
import uuid
import time
from typing import List
from dataclasses import asdict
from kubernetes import client, config, watch
import base64
import sys

from ..processors.Processor import Processor
from ..utils import constants
from ..utils.logger import logger
from ..utils.S3Utils import S3Utils
from ..utils.constants import TrainTablesInfo
from ..utils import utils


class K8sProcessor(Processor):
    def _create_wh_creds_secret(
        self, job_name: str, namespace: str, wh_creds: dict, core_v1_api
    ):
        encoded_creds = base64.b64encode(json.dumps(wh_creds).encode("utf-8")).decode(
            "utf-8"
        )
        secret_name = job_name + "-wh-secret"
        secret_key = "creds"
        payload = client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name),
            data={secret_key: encoded_creds},
            type="Opaque",
        )
        core_v1_api.create_namespaced_secret(namespace, payload)
        logger.get().info("Created secret %s", secret_name)
        return {"name": secret_name, "key": secret_key}

    def _create_job(
        self,
        job_name: str,
        secret: dict,
        k8s_config: dict,
        batch_v1_api,
        command: List[str],
    ):
        resources = {
            "cpu": k8s_config["resources"]["limits_cpu"],
            "memory": k8s_config["resources"]["limits_memory"],
        }
        namespace = k8s_config["namespace"]
        payload = client.V1Job(
            metadata=client.V1ObjectMeta(
                name=job_name,
                labels={
                    "job-name": job_name,
                },
            ),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=300,
                backoff_limit=0,
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        service_account_name=k8s_config.get("service_account", ""),
                        containers=[
                            client.V1Container(
                                name="container",
                                image="rudderstack/profiles-classifier:v0.3.2",
                                image_pull_policy="Always",
                                env=[
                                    client.V1EnvVar(
                                        name=constants.K8S_WH_CREDS_KEY,
                                        value_from=client.V1EnvVarSource(
                                            secret_key_ref=client.V1SecretKeySelector(
                                                key=secret["key"], name=secret["name"]
                                            )
                                        ),
                                    ),
                                ],
                                command=command,
                                resources=client.V1ResourceRequirements(
                                    requests={
                                        "cpu": resources["cpu"],
                                        "memory": resources["memory"],
                                    },
                                    limits={"memory": resources["memory"]},
                                ),
                            )
                        ],
                        restart_policy="Never",
                    ),
                ),
            ),
        )
        batch_v1_api.create_namespaced_job(namespace=namespace, body=payload)
        logger.get().info("Created job %s", job_name)

    def _wait_for_pod(self, job_name: str, namespace: str, core_v1_api):
        counter = 0
        while True:
            if counter >= constants.K8S_TIMEOUT_IN_SEC:
                raise Exception(f"Timed out while waiting for pod to start running")
            job_pods = core_v1_api.list_namespaced_pod(
                namespace=namespace, label_selector=f"job-name={job_name}"
            )
            if len(job_pods.items) == 0:
                logger.get().info("Waiting for pod to be created")
                time.sleep(1)
                counter = counter + 1
                continue
            pod = job_pods.items[0]
            phase = pod.status.phase.lower()
            if phase == "pending":
                logger.get().info("Pod currently in pending state")
                time.sleep(1)
                counter = counter + 1
            else:
                logger.get().info(
                    "Pod is now in running state. Status - %s, Name = %s",
                    phase,
                    pod.metadata.name,
                )
                return pod.metadata.name

    def _stream_logs(self, job_name: str, namespace: str, core_v1_api, pod_name: str):
        error_message = self._get_logs(
            pod_name=pod_name, namespace=namespace, core_v1_api=core_v1_api
        )
        if error_message != "":
            raise Exception(error_message)
        job_pods = core_v1_api.list_namespaced_pod(
            namespace=namespace, label_selector=f"job-name={job_name}"
        )
        pod = job_pods.items[0]
        phase = pod.status.phase.lower()
        if phase == "running":
            logger.get().info("Pod currently in running state")
            # Re streaming the logs to verify that the job actually completed and not because watch stream got disconnected
            # A cleaner approach probably is to use resource_version in the watcher
            time.sleep(2)
            self._get_logs(
                pod_name=pod_name, namespace=namespace, core_v1_api=core_v1_api
            )
            self._stream_logs(
                job_name=job_name,
                namespace=namespace,
                core_v1_api=core_v1_api,
                pod_name=pod_name,
            )
        elif phase == "succeeded":
            return
        else:
            raise Exception(f"Internal server error: pod in {phase} state")

    def _get_logs(self, pod_name: str, namespace: str, core_v1_api):
        w = watch.Watch()
        stream = w.stream(
            core_v1_api.read_namespaced_pod_log, name=pod_name, namespace=namespace
        )
        error_message = ""
        logger.get().debug("Streaming logs")
        while True:
            try:
                log = next(stream)
                logger.get().info(log)
                if error_message != "":
                    error_message = error_message + "\n" + log
                    continue
                index = max(
                    log.lower().find("traceback"),
                    log.lower().find("exception"),
                    log.lower().find("typeerror"),
                )
                if index != -1:
                    error_message = log[index:]
            except StopIteration:
                break
        return error_message

    def train(
        self,
        train_procedure,
        materials: List[TrainTablesInfo],
        merged_config: dict,
        input_column_types: dict,
        input_columns: List[str],
        metrics_table: str,
        wh_creds: dict,
        site_config: dict,
    ):
        credentials_presets = site_config["py_models"]["credentials_presets"]
        k8s_config = credentials_presets["kubernetes"]
        s3_config = credentials_presets["s3"]
        command = [
            sys.executable,
            "-u",
            "-m",
            f"{self.ml_core_path}.preprocess_and_train",
            "--s3_bucket",
            s3_config["bucket"],
            "--mode",
            constants.RUDDERSTACK_MODE,
            "--aws_region_name",
            s3_config["region"],
            "--s3_path",
            s3_config["path"],
            "--material_names",
            json.dumps(materials),
            "--merged_config",
            json.dumps(merged_config),
            "--input_column_types",
            json.dumps(input_column_types),
            "--input_columns",
            json.dumps(input_columns),
            "--connector_feature_table_name",
            self.connector.feature_table_name,
            "--metrics_table",
            metrics_table,
        ]
        job_name = "ml-training-" + str(uuid.uuid4())
        self._execute(
            job_name=job_name, wh_creds=wh_creds, k8s_config=k8s_config, command=command
        )
        S3Utils.download_directory(
            s3_config,
            self.connector.get_local_dir(),
        )
        S3Utils.delete_directory(
            s3_config["bucket"], s3_config["region"], s3_config["path"]
        )
        with open(
            os.path.join(
                self.connector.get_local_dir(), constants.TRAIN_JSON_RESULT_FILE
            ),
            "r",
        ) as file:
            train_results_json = json.load(file)
        return train_results_json

    def _execute(self, job_name, wh_creds, k8s_config, command):
        namespace = k8s_config["namespace"]
        config.load_incluster_config()
        core_v1_api = client.CoreV1Api()
        batch_v1_api = client.BatchV1Api()
        secret = self._create_wh_creds_secret(
            job_name=job_name,
            namespace=namespace,
            wh_creds=wh_creds,
            core_v1_api=core_v1_api,
        )
        try:
            self._create_job(
                job_name=job_name,
                secret=secret,
                k8s_config=k8s_config,
                batch_v1_api=batch_v1_api,
                command=command,
            )
            pod_name = self._wait_for_pod(
                job_name=job_name, namespace=namespace, core_v1_api=core_v1_api
            )
            self._stream_logs(
                job_name=job_name,
                namespace=namespace,
                core_v1_api=core_v1_api,
                pod_name=pod_name,
            )
        finally:
            core_v1_api.delete_namespaced_secret(
                name=secret["name"], namespace=namespace
            )

    def predict(
        self,
        wh_creds: dict,
        s3_config: dict,
        model_path: str,
        inputs: List[utils.InputsConfig],
        end_ts: str,
        output_tablename: str,
        merged_config: dict,
        site_config: dict,
        pkl_model_file_name: str,
        model_hash: str,
    ):
        credentials_presets = site_config["py_models"]["credentials_presets"]
        k8s_config = credentials_presets["kubernetes"]
        s3_config = credentials_presets["s3"]
        json_output_filename = model_path.split("/")[-1]
        predict_upload_whitelist = [
            pkl_model_file_name,
            json_output_filename,
        ]
        logger.get().debug("Uploading files required for prediction to S3")
        local_folder = self.connector.get_local_dir()
        S3Utils.upload_directory(
            s3_config["bucket"],
            s3_config["region"],
            s3_config["path"],
            os.path.dirname(local_folder),
            predict_upload_whitelist,
        )
        command = [
            sys.executable,
            "-u",
            "-m",
            f"{self.ml_core_path}.preprocess_and_predict",
            "--s3_config",
            json.dumps(s3_config),
            "--mode",
            constants.RUDDERSTACK_MODE,
            "--json_output_filename",
            json_output_filename,
            "--inputs",
            json.dumps(inputs, default=asdict),
            "--end_ts",
            end_ts,
            "--output_tablename",
            output_tablename,
            "--merged_config",
            json.dumps(merged_config),
            "--pkl_model_file_name",
            pkl_model_file_name,
            "--model_hash",
            model_hash,
        ]
        job_name = "ml-prediction-" + str(uuid.uuid4())
        self._execute(
            job_name=job_name, wh_creds=wh_creds, k8s_config=k8s_config, command=command
        )
        S3Utils.delete_directory(
            s3_config["bucket"], s3_config["region"], s3_config["path"]
        )
