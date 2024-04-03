from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from profiles_rudderstack.schema import EntityKeyBuildSpecSchema

import pandas as pd

from ..utils import constants

from ..wht.pyNativeWHT import PyNativeWHT
from ..train import _train


class TrainingModel(BaseModelType):
    TypeName = "training_model"
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
        return TrainingRecipe(self.build_spec)

    def validate(self) -> tuple[bool, str]:
        min_version = constants.MIN_PB_VERSION
        if self.schema_version < min_version:
            return False, f"schema version should >= {min_version}"
        return super().validate()


class TrainingRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.build_spec = build_spec
        self.logger = Logger("TrainingRecipe")

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

    def get_output_filepath(this: WhtMaterial):
        parts = this.name().rsplit("_", 1)
        file_name = parts[0] + "_" + "training_file"
        folder = this.get_output_folder()
        return f"{folder}/{file_name}"

    def execute(self, this: WhtMaterial):
        import os
        import yaml

        homedir = os.path.expanduser("~")
        with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
            creds = yaml.safe_load(f)["connections"]["test"]["outputs"]["redshift"]
        input_model_refs = self.build_spec.get("inputs", [])
        output_filename = TrainingRecipe.get_output_filepath(this)
        site_config_path = os.getenv("SITE_CONFIG_PATH")
        project_folder = os.getenv("PROJECT_FOLDER")
        runtime_info = {}
        config = self.build_spec.get("ml_config", {})
        input_materials = []
        # TODO
        # 1. Get project_folder from pywht
        # 2. Get site_config_path from pywht
        # 3. Compute creds from site_config_path
        # 4. Get runtime_info from pywht
        for input in self.build_spec["inputs"]:
            material = this.de_ref(input)
            input_materials.append(material.name())
        _train(
            creds,
            input_materials,
            output_filename,
            config,
            site_config_path,
            project_folder,
            runtime_info,
            input_model_refs,
            PyNativeWHT(this),
            constants.ML_CORE_PYNATIVE_PATH,
            this.name(),
        )
