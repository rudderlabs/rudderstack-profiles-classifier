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
        input_columns: List[str],
        metrics_table: str,
        wh_creds: dict,
        site_config: dict,
        pkl_model_file_name: str,
    ):
        pass

    @abstractmethod
    def predict(
        self,
        creds,
        s3_config,
        model_path,
        inputs,
        end_ts,
        output_tablename,
        merged_config,
        site_config: dict,
        model_hash: str,
    ):
        pass
