# Enabling the following comment throws an error when running the project using python model
# from profiles_rudderstack.project import WhtProject
def register_extensions(project):
    from .py_native.training import TrainingModel

    project.register_model_type(TrainingModel)

    from .py_native.prediction import PredictionModel

    project.register_model_type(PredictionModel)

    from .py_native.llm import LLMModel

    project.register_model_type(LLMModel)

    from .py_native.attribution_report import AttributionModel

    project.register_model_type(AttributionModel)

    from .py_native.propensity import PropensityModel

    project.register_model_type(PropensityModel)

    from .py_native.id_stitcher.audit import AuditIdStitcherModel

    project.register_model_type(AuditIdStitcherModel)

    from .py_native.profiles_tutorial.model import TutorialModel

    project.register_model_type(TutorialModel)
