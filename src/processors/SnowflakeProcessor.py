from typing import List

from src.processors.Processor import Processor
from src.utils.constants import TrainTablesInfo
from src.ml_core.preprocess_and_train import preprocess_and_train


class SnowflakeProcessor(Processor):
    def train(
        self,
        train_procedure,
        materials: List[TrainTablesInfo],
        model_config: dict,
        prediction_task: str,
        wh_creds: dict,
        site_config: dict,
    ):
        return preprocess_and_train(
            train_procedure,
            materials,
            model_config,
            session=self.session,
            connector=self.connector,
            trainer=self.trainer,
        )
