from .MLTrainer import MLTrainer
from .ClassificationTrainer import ClassificationTrainer
from .RegressionTrainer import RegressionTrainer
from ..connectors.Connector import Connector


class TrainerFactory:
    def create(
        model_config,
        connector: Connector = None,
        entity_var_model_name: str = None,
    ) -> MLTrainer:
        task = model_config["data"].get("task", "classification")
        if task == "classification":
            return ClassificationTrainer(
                connector, entity_var_model_name, **model_config
            )
        elif task == "regression":
            return RegressionTrainer(**model_config)
        else:
            raise Exception(f"Invalid task type {task}")
