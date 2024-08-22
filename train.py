import sys
import os
from typing import List

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "src", "predictions"))
)

# If path starts with the "src" directory, Snowflake throws an error: No module named 'src'
from profiles_mlcorelib.train import _train
from profiles_mlcorelib.wht.pythonWHT import PythonWHT
from profiles_mlcorelib.utils import constants


def train(
    creds: dict,
    input_selector_sqls: List[str],
    output_filename: str,
    config: dict,
    site_config_path: str = None,
    project_folder: str = None,
    runtime_info: dict = None,
) -> None:
    wht = PythonWHT(site_config_path, project_folder)
    _train(
        creds,
        wht.get_inputs(input_selector_sqls, False),
        output_filename,
        config,
        site_config_path,
        runtime_info,
        wht,
        constants.ML_CORE_PYTHON_PATH,
        "TRAINING_METRICS_v4",
    )
