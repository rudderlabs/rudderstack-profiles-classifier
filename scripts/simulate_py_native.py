import sys
import os

import yaml

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.predictions.rudderstack_predictions.wht.pyNativeWHT import PyNativeWHT
from src.predictions.rudderstack_predictions.utils import constants
from src.predictions.rudderstack_predictions.train import _train
from src.predictions.rudderstack_predictions.predict import _predict


class EntityVarTable:
    def name(self):
        # To be configured
        return "Material_user_var_table_bcf242bb_1093"


class Training:
    def de_ref(self, model_ref):
        return EntityVarTable()


if __name__ == "__main__":
    build_spec = {
        "inputs": [
            "entity/user/is_churned_7_days",
        ],
        "ml_config": {
            "data": {"label_column": "is_churned_7_days", "prediction_horizon_days": 7}
        },
    }
    homedir = os.path.expanduser("~")
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["test"]["outputs"]["redshift"]
    input_model_refs = build_spec.get("inputs", [])
    output_filename = os.path.join(os.getcwd(), "training_file")
    site_config_path = os.getenv("SITE_CONFIG_PATH")
    project_folder = os.getenv("PROJECT_FOLDER")
    runtime_info = {"site_config_path": site_config_path}
    config = build_spec.get("ml_config", {})
    input_materials = ["Material_is_churned_7_days_d5df048c_1093"]  # To be configured
    training_material = Training()
    _train(
        creds,
        input_materials,
        output_filename,
        config,
        site_config_path,
        project_folder,
        runtime_info,
        input_model_refs,
        PyNativeWHT(training_material),
        constants.ML_CORE_PYTHON_PATH,
    )
    _predict(
        creds,
        output_filename,
        input_materials,
        "Material_is_churned_7_days_d5df048c_1093",
        config,
        runtime_info,
        constants.ML_CORE_PYTHON_PATH,
    )
