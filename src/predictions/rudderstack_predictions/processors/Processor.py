from abc import ABC, abstractmethod
from typing import List, Union

from ..utils.constants import *
from ..trainers.MLTrainer import MLTrainer
from ..connectors.Connector import Connector

import snowflake.snowpark
import redshift_connector
import redshift_connector.cursor


class Processor(ABC):
    def __init__(
        self,
        trainer: MLTrainer,
        connector: Connector,
        session: Union[snowflake.snowpark.Session, redshift_connector.cursor.Cursor],
        ml_core_path: str,
    ):
        self.trainer = trainer
        self.connector = connector
        self.session = session
        self.ml_core_path = ml_core_path

    @abstractmethod
    def train(
        self,
        train_procedure,
        materials: List[TrainTablesInfo],
        model_config: dict,
        input_column_types: dict,
        metrics_table: str,
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
        site_config: dict,
    ):
        pass
