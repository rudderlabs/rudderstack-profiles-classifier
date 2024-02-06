import os
import sys
import warnings
import numpy as np
import pandas as pd
from logger import logger
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
from S3Utils import S3Utils

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import utils
import constants
import ProcessorMap
from SnowflakeConnector import SnowflakeConnector
from MLTrainer import ClassificationTrainer, RegressionTrainer

warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

try:
    from RedshiftConnector import RedshiftConnector
except Exception as e:
    logger.warning(f"Could not import RedshiftConnector")


def predict(
    creds: dict,
    _: dict, # s3_config is not being populated for some reason. Using site_config to get its value
    model_path: str,
    inputs: str,
    output_tablename: str,
    config: dict,
    runtime_info: dict = None,
) -> None:
    """Generates the prediction probabilities and save results for given model_path

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        s3_config (dict): aws credentials - not required for snowflake. only used for redshift
        model_path (str): path to the file where the model details including model id etc are present. Created in training step
        inputs: (List[str]), containing sql queries such as "select * from <feature_table_name>" from which the script infers input tables        output_tablename (str): name of output table where prediction results are written
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file
        runtime_info (dict): Whether the code is running on rudder infra or local. Useful to decide if redshift processor should run locally or in k8s

    Returns:
        None: save the prediction results but returns nothing
    """
    logger.debug("Starting Predict job")

    is_rudder_backend = utils.fetch_key_from_dict(
        runtime_info, "is_rudder_backend", False
    )
    site_config_path = utils.fetch_key_from_dict(runtime_info, "site_config_path", "")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.dirname(model_path)

    default_config = utils.load_yaml(
        os.path.join(current_dir, "config", "model_configs.yaml")
    )
    _ = config["data"].pop(
        "package_name", None
    )  # For backward compatibility. Not using it anywhere else, hence deleting.
    merged_config = utils.combine_config(default_config, config)

    user_preference_order_infra = merged_config["data"].pop(
        "user_preference_order_infra", None
    )
    prediction_task = merged_config["data"].pop(
        "task", "classification"
    )  # Assuming default as classification

    if prediction_task == "classification":
        trainer = ClassificationTrainer(**merged_config)
    elif prediction_task == "regression":
        trainer = RegressionTrainer(**merged_config)

    logger.debug(
        f"Started Predicting for {trainer.output_profiles_ml_model} to predict {trainer.label_column}"
    )

    if creds["type"] == "snowflake":
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
    elif creds["type"] == "redshift":
        connector = RedshiftConnector(folder_path)
        session = connector.build_session(creds)

    udf_name = connector.get_udf_name(model_path)
    connector.cleanup(session, udf_name=udf_name)

    mode = connector.fetch_processor_mode(
        user_preference_order_infra, is_rudder_backend
    )
    processor = ProcessorMap.processor_mode_map[mode](trainer, connector, session)
    logger.debug(f"Using {mode} processor for predictions")
    if site_config_path == "":
        # TODO - Remove it post pb release
        s3_config = {
            "bucket": "ml-usecases-poc-srinivas",
            "region": "us-east-1",
            "path": "2b0AM2EcotEiuoa0GwkpCtzuQaa/cmu7ff9gtmi99j1fb330/cmu7ff9gtmi99j1fb33g",
            "role_arn": "arn:aws:iam::454531037350:role/profiles-ml-s3",
        }
        site_config = {
            "py_models": {
                "credentials_presets": {
                    "s3": s3_config,
                    "kubernetes": {
                        "namespace": "profiles-qa",
                        "resources": {"limits_cpu": "2000m", "limits_memory": "2Gi"},
                    },
                }
            }
        }
    else:
        site_config = utils.load_yaml(site_config_path)
        s3_config = site_config["py_models"]["credentials_presets"]["s3"]
    if mode == ProcessorMap.RUDDERSTACK_MODE:
        s3_creds = S3Utils.get_temporary_credentials(s3_config["role_arn"])
        s3_config["access_key_id"] = s3_creds["access_key_id"]
        s3_config["access_key_secret"] = s3_creds["access_key_secret"]
        s3_config["aws_session_token"] = s3_creds["aws_session_token"]
    _ = processor.predict(
        creds,
        s3_config,
        model_path,
        inputs,
        output_tablename,
        merged_config,
        prediction_task,
        site_config,
     )
