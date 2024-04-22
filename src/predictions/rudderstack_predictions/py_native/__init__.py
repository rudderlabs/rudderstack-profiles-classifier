from profiles_rudderstack.project import WhtProject
from .sf_cortex_model import SnowflakeCortexModel


def register_extensions(project: WhtProject):
    project.register_model_type(SnowflakeCortexModel)
