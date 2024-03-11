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
        prediction_task: str,
        wh_creds: dict,
        site_config: dict,
        run_id: str,
    ):
        return preprocess_and_train(
            train_procedure,
            materials,
            model_config,
            run_id,
            session=self.session,
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
