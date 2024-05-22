import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "src", "predictions"))
)

# If path starts with the "src" directory, Snowflake throws an error: No module named 'src'
from rudderstack_predictions.train import _train
from rudderstack_predictions.wht.pythonWHT import PythonWHT
from rudderstack_predictions.utils import constants



def train(
    creds: dict,
    inputs: str,
    output_filename: str,
    config: dict,
    site_config_path: str = None,
    project_folder: str = None,
    runtime_info: dict = None,
) -> None:
    input_models = None
    _train(
        creds,
        inputs,
        output_filename,
        config,
        site_config_path,
        project_folder,
        runtime_info,
        input_models,
        PythonWHT(),
        constants.ML_CORE_PYTHON_PATH,
        "TRAINING_METRICS_v4",
    )
