# Enabling the following comment throws an error when running the project using python model
# from profiles_rudderstack.project import WhtProject
def register_extensions(project):
    from .py_native.training import TrainingModel

    project.register_model_type(TrainingModel)
    from .py_native.prediction import PredictionModel

    project.register_model_type(PredictionModel)
    from .py_native.llm import LLMModel
    project.register_model_type(LLMModel)
