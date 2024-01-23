import os
import sys
import json
import joblib
import warnings
import cachetools
import numpy as np
import pandas as pd

from typing import Any , List
from logger import logger
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import utils
import constants
from AWSProcessor import AWSProcessor
from LocalProcessor import LocalProcessor
from SnowflakeProcessor import SnowflakeProcessor
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
    aws_config: dict,
    model_path: str,
    inputs: str,
    output_tablename: str,
    config: dict,
    runtime_info: dict = None,
) -> None:
    """Generates the prediction probabilities and save results for given model_path

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        aws_config (dict): aws credentials - not required for snowflake. only used for redshift
        model_path (str): path to the file where the model details including model id etc are present. Created in training step
        inputs: (List[str]), containing sql queries such as "select * from <feature_table_name>" from which the script infers input tables        output_tablename (str): name of output table where prediction results are written
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

    Returns:
        None: save the prediction results but returns nothing
    """
    logger.debug("Starting Predict job")

    if aws_config is None:
        import configparser
        homedir = os.path.expanduser("~")
        credentials_file_path = os.path.join(homedir, ".aws/credentials")
        if os.path.exists(credentials_file_path):
            config = configparser.ConfigParser()
            config.read(credentials_file_path)
        else:
            raise Exception(f"Credentials file not found at {credentials_file_path}.")
        aws_config = {
            "access_key_id": config.get("default", "aws_access_key_id"),
            "access_key_secret": config.get("default", "aws_secret_access_key"),
            "aws_session_token": config.get("default", "aws_session_token"),
            "bucket": constants.S3_BUCKET,
            "path": constants.S3_PATH,
            "region": constants.AWS_REGION_NAME,
        }

    is_rudder_backend = utils.fetch_key_from_dict(
        runtime_info, "is_rudder_backend", False
    )

    current_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.dirname(model_path)
    processor_mode_map = {
        "local": LocalProcessor,
        "native-warehouse": SnowflakeProcessor,
        "rudderstack-infra": AWSProcessor,
    }

    default_config = utils.load_yaml(os.path.join(current_dir, "config", "model_configs.yaml"))
    _ = config["data"].pop("package_name", None) # For backward compatibility. Not using it anywhere else, hence deleting.
    merged_config = utils.combine_config(default_config, config)

    user_preference_order_infra = merged_config["data"].pop(
        "user_preference_order_infra", None
    )
    prediction_task = merged_config["data"].pop(
        "task", "classification"
    )  # Assuming default as classification

    prep_config = utils.PreprocessorConfig(**merged_config["preprocessing"])
    outputs_config = utils.OutputsConfig(**merged_config["outputs"])
    if prediction_task == "classification":
        trainer = ClassificationTrainer(**merged_config["data"], prep=prep_config, outputs=outputs_config)
    elif prediction_task == "regression":
        trainer = RegressionTrainer(**merged_config["data"], prep=prep_config, outputs=outputs_config)

    logger.debug(
        f"Started Predicting for {trainer.output_profiles_ml_model} to predict {trainer.label_column}"
    )

    with open(model_path, "r") as f:
        results = json.load(f)
    stage_name = results["model_info"]["file_location"]["stage"]

    udf_name = None
    if creds["type"] == "snowflake":
        udf_name = f"prediction_score_{stage_name.replace('@','')}"
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
        connector.cleanup(session, udf_name=udf_name)
    elif creds["type"] == "redshift":
        connector = RedshiftConnector(folder_path)
        session = connector.build_session(creds)

    mode = connector.fetch_processor_mode(
        user_preference_order_infra, is_rudder_backend
    )
    processor = processor_mode_map[mode](trainer, connector, session)
    logger.debug(f"Using {mode} processor for predictions")
    _ = processor.predict(creds, aws_config, model_path, inputs, output_tablename, merged_config, prediction_task, udf_name)