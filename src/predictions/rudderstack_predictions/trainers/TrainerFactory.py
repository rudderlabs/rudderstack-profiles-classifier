from .MLTrainer import ClassificationTrainer, RegressionTrainer, MLTrainer
from ..connectors.Connector import Connector


class TrainerFactory:
    def create(
        model_config,
        connector: Connector = None,
        session=None,
        entity_var_model_name: str = None,
    ) -> MLTrainer:
        task = model_config["data"].get("task", "classification")
        if task == "classification":
            return ClassificationTrainer(
                connector, session, entity_var_model_name, **model_config
            )
        elif task == "regression":
            return RegressionTrainer(**model_config)
        else:
            raise Exception(f"Invalid task type {task}")
