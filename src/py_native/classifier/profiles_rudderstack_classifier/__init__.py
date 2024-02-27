from profiles_rudderstack.project import WhtProject
from .training import ClassifierTrainingModel
from .prediction import ClassifierPredictionModel


def register_extensions(project: WhtProject):
    project.register_model_type(ClassifierTrainingModel)
    project.register_model_type(ClassifierPredictionModel)
