from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union, Dict

import src.utils.constants as constants
from src.trainers.MLTrainer import MLTrainer
from src.connectors.Connector import Connector

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

    @abstractmethod
    def train(
        self,
        train_procedure,
        materials: List[constants.TrainTablesInfo],
        model_config: dict,
        prediction_task: str,
        wh_creds: dict,
        site_config: dict,
    ):
        pass

    @abstractmethod
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
        pass
