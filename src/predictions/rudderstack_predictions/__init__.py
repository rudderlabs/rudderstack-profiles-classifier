# Enabling the following comment throws an error when running the project using python model
# from profiles_rudderstack.project import WhtProject
def register_extensions(project):
    from .py_native.training import ClassifierTrainingModel

    project.register_model_type(ClassifierTrainingModel)
    from .py_native.prediction import ClassifierPredictionModel

    project.register_model_type(ClassifierPredictionModel)
