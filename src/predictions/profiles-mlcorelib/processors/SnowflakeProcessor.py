from typing import List

from ..processors.Processor import Processor
from ..utils.constants import TrainTablesInfo
from ..ml_core.preprocess_and_train import preprocess_and_train
from ..ml_core.preprocess_and_predict import preprocess_and_predict


class SnowflakeProcessor(Processor):
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
        return preprocess_and_train(
            train_procedure,
            materials,
            model_config,
            input_column_types,
            metrics_table,
            connector=self.connector,
            trainer=self.trainer,
        )

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
        return preprocess_and_predict(
            creds,
            s3_config,
            model_path,
            inputs,
            output_tablename,
            connector=self.connector,
            trainer=self.trainer,
        )
