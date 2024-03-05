from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from profiles_rudderstack.schema import EntityKeyBuildSpecSchema
from ..train import _train
import os

from dotenv import load_dotenv

load_dotenv()

class ClassifierTrainingModel(BaseModelType):
    TypeName = "classifier_training"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "occurred_at_col": {"type": "string"},
            **EntityKeyBuildSpecSchema["properties"],
            "validity_time": {"type": "string"},
            "inputs": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "ml_config": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "data": {
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "label_column": {"type": "string"},
                            "prediction_horizon_days": {"type": "integer"},
                        },
                        "required": ["label_column", "prediction_horizon_days"],
                    },
                    "preprocessing": {
                        "type": "object",
                    },
                },
                "required": ["data"],
            },
        },
        "required": [
            "inputs",
            "ml_config",
        ]
        + EntityKeyBuildSpecSchema["required"],
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return ClassifierTrainingRecipe(self.build_spec)

    def validate(self) -> tuple[bool, str]:
        min_version = 49
        if self.schema_version < min_version:
            return False, f"schema version should >= {min_version}"
        return super().validate()


class ClassifierTrainingRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.build_spec = build_spec
        self.logger = Logger("ClassifierTrainingRecipe")

    def describe(self, this: WhtMaterial):
        return (
            f"""
        Material - {this.name()}
        """,
            ".txt",
        )

    def prepare(self, this: WhtMaterial):
        for input in self.build_spec["inputs"]:
            this.de_ref(input)

    def execute(self, this: WhtMaterial):
        creds = {
            "dbname": os.getenv("REDSHIFT_DB_NAME"),
            "host": os.getenv("REDSHIFT_HOST"),
            "password": os.getenv("REDSHIFT_PASSWORD"),
            "port": os.getenv("REDSHIFT_PORT"),
            "schema": os.getenv("REDSHIFT_SCHEMA"),
            "type": "redshift",
            "user": os.getenv("REDSHIFT_USER"),
        }
        print(creds)
        inputs = ",".join(self.build_spec.get("inputs", []))
        output_filename = os.path.abspath(__file__)
        config = {
            "data": {}
        }
        site_config_path = "/Users/shashankshekhar/.pb/siteconfig.yaml"
        project_folder = "/Users/shashankshekhar/Documents/RudderStack/rudderstack-profiles-classifier/samples/py_native"
        runtime_info = {}
        _train(creds, inputs, output_filename, config, site_config_path, project_folder, runtime_info)

if __name__ == "__main__":
    build_spec = {
        "inputs": [],
    }
    a = ClassifierTrainingRecipe(build_spec)
    a.execute(None)
