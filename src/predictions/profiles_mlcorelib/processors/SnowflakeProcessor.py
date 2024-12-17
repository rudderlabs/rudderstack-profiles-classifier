from typing import List

from predictions.profiles_mlcorelib.connectors.Connector import Connector
from predictions.profiles_mlcorelib.trainers.MLTrainer import MLTrainer

from ..processors.Processor import Processor
from ..utils.constants import TrainTablesInfo
from ..ml_core.preprocess_and_train import preprocess_and_train
from ..ml_core.preprocess_and_predict import preprocess_and_predict


class SnowflakeProcessor(Processor):
    def __init__(self, trainer: MLTrainer, connector: Connector, ml_core_path: str):
        super().__init__(trainer, connector, ml_core_path)

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
        return preprocess_and_train(
            train_procedure,
            materials,
            model_config,
            input_column_types,
            input_columns,
            metrics_table,
            pkl_model_file_name,
            connector=self.connector,
            trainer=self.trainer,
        )

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
        return preprocess_and_predict(
            creds,
            s3_config,
            model_path,
            inputs,
            end_ts,
            output_tablename,
            connector=self.connector,
            trainer=self.trainer,
            model_hash=model_hash,
        )
