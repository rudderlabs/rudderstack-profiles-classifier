from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from typing import Tuple
from profiles_rudderstack.schema import EntityKeyBuildSpecSchema

from ..wht.pyNativeWHT import PyNativeWHT

from .training import TrainingRecipe

from ..predict import _predict
from ..utils import constants
from ..utils import utils


class PredictionModel(BaseModelType):
    TypeName = "prediction_model"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "occurred_at_col": {"type": "string"},
            **EntityKeyBuildSpecSchema["properties"],
            "validity_time": {"type": "string"},
            "inputs": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "training_model": {"type": "string"},
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
            "training_model",
            "ml_config",
        ]
        + EntityKeyBuildSpecSchema["required"],
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return PredictionRecipe(self.build_spec)

    def validate(self) -> Tuple[bool, str]:
        min_version = constants.MIN_PB_VERSION
        if self.schema_version < min_version:
            return False, f"schema version should >= {min_version}"
        return super().validate()


class PredictionRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.build_spec = build_spec
        self.logger = Logger("PredictionRecipe")

    def describe(self, this: WhtMaterial):
        return (
            f"""
        Material - {this.name()}
        """,
            ".txt",
        )

    def register_dependencies(self, this: WhtMaterial):
        this.de_ref(self.build_spec["training_model"])

    def _get_train_output_filepath(self, this: WhtMaterial):
        # If training is skipped, this function will return incorrect path
        # Option 1: Implement the logic for testing file validity in this package
        # Option 2: Always run training before prediction till file as output type is released
        train_material = this.de_ref(self.build_spec["training_model"])
        return TrainingRecipe.get_output_filepath(train_material)

    def execute(self, this: WhtMaterial):
        site_config_path = this.wht_ctx.site_config().get("FilePath")
        whtService = PyNativeWHT(this)
        # TODO: Get creds from pywht
        creds = whtService.get_credentials(
            this.base_wht_project.project_path(), site_config_path
        )
        runtime_info = {"site_config_path": site_config_path}
        config = self.build_spec.get("ml_config", {})
        input_materials = []
        output_tablename = this.name()
        train_output = self._get_train_output_filepath(this)
        for input in self.build_spec["inputs"]:
            material = this.de_ref(input)
            input_materials.append(material.name())
        _predict(
            creds,
            train_output,
            input_materials,
            output_tablename,
            config,
            runtime_info,
            constants.ML_CORE_PYNATIVE_PATH,
        )
