from abc import ABC, abstractmethod
from typing import List

from ..utils.constants import *
from ..trainers.MLTrainer import MLTrainer
from ..connectors.Connector import Connector


class Processor(ABC):
    def __init__(
        self,
        trainer: MLTrainer,
        connector: Connector,
        ml_core_path: str,
    ):
        self.trainer = trainer
        self.connector = connector
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
