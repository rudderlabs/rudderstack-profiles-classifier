from abc import ABC
from typing import Any, List, Tuple, Union, Dict

from src.trainers.MLTrainer import MLTrainer
from src.connectors.Connector import Connector
from src.ml_core.preprocess_and_predict import preprocess_and_predict

import snowflake.snowpark
import redshift_connector
import redshift_connector.cursor


class Processor(ABC):
    def __init__(
        self,
        trainer: MLTrainer,
        connector: Connector,
        session: Union[snowflake.snowpark.Session, redshift_connector.cursor.Cursor],
    ):
        self.trainer = trainer
        self.connector = connector
        self.session = session

    def predict(
        self,
        creds,
        s3_config,
        model_path,
        inputs,
        output_tablename,
        merged_config,
        prediction_task,
        site_config: dict,
    ):
        return preprocess_and_predict(
            creds,
            s3_config,
            model_path,
            inputs,
            output_tablename,
            prediction_task,
            session=self.session,
            connector=self.connector,
            trainer=self.trainer,
        )
