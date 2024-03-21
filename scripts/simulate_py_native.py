import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.predictions.rudderstack_predictions.wht.pyNativeWHT import PyNativeWHT
from src.predictions.rudderstack_predictions.utils import constants
from src.predictions.rudderstack_predictions.train import _train


class EntityVarTable:
    def name(self):
        # To be configured
        return "Material_user_var_table_d1cb1479_2876"


class Training:
    def de_ref(self, model_ref):
        return EntityVarTable()


if __name__ == "__main__":
    build_spec = {
        "inputs": [
            "entity/user/is_churned_7_days",
        ],
        "ml_config": {"data": {}},
    }
    creds = {
        "dbname": os.getenv("REDSHIFT_DB_NAME"),
        "host": os.getenv("REDSHIFT_HOST"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "port": os.getenv("REDSHIFT_PORT"),
        "schema": os.getenv("REDSHIFT_SCHEMA"),
        "type": "redshift",
        "user": os.getenv("REDSHIFT_USER"),
    }
    input_model_refs = build_spec.get("inputs", [])
    output_filename = os.path.join(os.getcwd(), "training_file")
    site_config_path = os.getenv("SITE_CONFIG_PATH")
    project_folder = os.getenv("PROJECT_FOLDER")
    runtime_info = {}
    config = build_spec.get("ml_config", {})
    input_materials = ["Material_is_churned_7_days_73580aff_2875"]  # To be configured
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
        constants.ML_CORE_PYNATIVE_PATH,
    )
