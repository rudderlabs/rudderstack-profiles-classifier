from profiles_rudderstack.project import WhtProject
from .py_native.training import ClassifierTrainingModel
from .py_native.prediction import ClassifierPredictionModel


def register_extensions(project: WhtProject):
    project.register_model_type(ClassifierTrainingModel)
    project.register_model_type(ClassifierPredictionModel)
