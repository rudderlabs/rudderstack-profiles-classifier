from logger import logger
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union

from MLTrainer import MLTrainer
from Connector import Connector
from preprocess_and_train import preprocess_and_train

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
