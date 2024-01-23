from logger import logger
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union

from MLTrainer import MLTrainer
from Connector import Connector
from preprocess_and_train import preprocess_and_train
from preprocess_and_predict import preprocess_and_predict

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

    def train(
        self,
        train_procedure,
        material_names: List[Tuple[str]],
        merged_config: dict,
        prediction_task: str,
        wh_creds: dict,
    ):
        return preprocess_and_train(
            train_procedure,
            material_names,
            merged_config,
            session=self.session,
            connector=self.connector,
            trainer=self.trainer,
        )

    def predict(
        self, 
        creds, 
        aws_config, 
        model_path, 
        inputs, 
        output_tablename, 
        merged_config, 
        prediction_task, 
        udf_name,
    ):
        return preprocess_and_predict(
            creds, 
            aws_config, 
            model_path, 
            inputs, 
            output_tablename,
            prediction_task, 
            udf_name,
            session=self.session,
            connector=self.connector,
            trainer=self.trainer,
        )