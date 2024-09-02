import sys
import os
from typing import List

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "src", "predictions"))
)

# If path starts with the "src" directory, Snowflake throws an error: No module named 'src'

from profiles_mlcorelib.predict import _predict
from profiles_mlcorelib.utils import constants
from src.predictions.profiles_mlcorelib.wht.pythonWHT import PythonWHT


def predict(
    creds: dict,
    _: dict,  # s3_config is not being populated for some reason. Using site_config to get its value
    model_path: str,
    input_selector_sqls: List[str],
    output_tablename: str,
    config: dict,
    runtime_info: dict = None,
) -> None:
    """Generates the prediction probabilities and save results for given model_path

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        s3_config (dict): aws credentials - not required for snowflake. only used for redshift
        model_path (str): path to the file where the model details including model id etc are present. Created in training step
        input_selector_sqls: (List[str]), containing sql queries such as "select * from <feature_table_name>"
        output_tablename (str): name of output table where prediction results are written
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file
        runtime_info (dict): Whether the code is running on rudder infra or local. Useful to decide if redshift processor should run locally or in k8s

    Returns:
        None: save the prediction results but returns nothing
    """
    wht = PythonWHT(None, None)
    _predict(
        creds,
        model_path,
        # Skipping compile when computing inputs since the information derived from it is not used in the predict step
        wht.get_inputs(input_selector_sqls, False),
        output_tablename,
        config,
        runtime_info,
        constants.ML_CORE_PYTHON_PATH,
        wht,
    )
