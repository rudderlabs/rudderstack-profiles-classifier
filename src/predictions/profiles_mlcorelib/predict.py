#!/usr/bin/env python
# coding: utf-8
import os
from typing import List

from .trainers.TrainerFactory import TrainerFactory

from .utils.S3Utils import S3Utils
from .wht.pythonWHT import PythonWHT

from .processors.ProcessorFactory import ProcessorFactory

from .utils.logger import logger

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

from .utils import utils

from .connectors.ConnectorFactory import ConnectorFactory


warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)


def _predict(
    creds: dict,
    model_path: str,
    inputs: List[dict],
    output_tablename: str,
    config: dict,
    runtime_info: dict,
    ml_core_path: str,
    whtService: PythonWHT,
) -> None:
    logger.get().debug("Starting Predict job")

    is_rudder_backend = utils.fetch_key_from_dict(
        runtime_info, "is_rudder_backend", False
    )
    site_config_path = utils.fetch_key_from_dict(runtime_info, "site_config_path", "")

    folder_path = os.path.dirname(model_path)

    connector = ConnectorFactory.create(creds, folder_path)
    whtService.set_connector(connector)

    default_config = utils.load_yaml(utils.get_model_configs_file_path())
    _ = config["data"].pop(
        "package_name", None
    )  # For backward compatibility. Not using it anywhere else, hence deleting.
    merged_config = utils.combine_config(default_config, config)
    merged_config = whtService.update_config_info(merged_config)
    user_preference_order_infra = merged_config["data"].pop(
        "user_preference_order_infra", None
    )
    trainer = TrainerFactory.create(merged_config)

    logger.get().debug(
        f"Started Predicting for {trainer.output_profiles_ml_model} to predict {trainer.label_column}"
    )

    connector.compute_udf_name(model_path)
    connector.pre_job_cleanup()

    mode = connector.fetch_processor_mode(
        user_preference_order_infra, is_rudder_backend
    )
    processor = ProcessorFactory.create(mode, trainer, connector, ml_core_path)
    logger.get().debug(f"Using {mode} processor for predictions")

    site_config = utils.load_yaml(site_config_path)
    end_ts = whtService.get_end_ts()
    presets = site_config["py_models"].get("credentials_presets")
    if presets is None or presets.get("s3") is None:
        s3_config = {}
    else:
        s3_config = presets["s3"]

    if is_rudder_backend and creds["type"] == "redshift":
        s3_creds = S3Utils.get_temporary_credentials(s3_config["role_arn"])
        s3_config["access_key_id"] = s3_creds["access_key_id"]
        s3_config["access_key_secret"] = s3_creds["access_key_secret"]
        s3_config["aws_session_token"] = s3_creds["aws_session_token"]

    _ = processor.predict(
        creds,
        s3_config,
        model_path,
        inputs,
        end_ts,
        output_tablename,
        merged_config,
        site_config,
    )
