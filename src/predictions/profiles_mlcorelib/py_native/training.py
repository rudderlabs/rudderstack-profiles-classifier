import os
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
    MaterializationBuildSpecSchema,
)

from ..utils.logger import logger

from .warehouse import standardize_ref_name

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
            **MaterializationBuildSpecSchema["properties"],
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
        if (
            "materialization" not in build_spec
            or "enable_status" not in build_spec["materialization"]
        ):
            # This will ensure that the training model won't run if there is no prediction model in the project
            build_spec["materialization"] = {"enable_status": "only_if_necessary"}
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

    def register_dependencies(self, this: WhtMaterial):
        for input in self.build_spec["inputs"]:
            this.de_ref(input)

    def get_training_file_path(this: WhtMaterial):
        folder = this.get_output_folder()
        return f"{folder}/{this.name()}/training_file"

    def execute(self, this: WhtMaterial):
        logger.set_logger(self.logger)
        whtService = PyNativeWHT(this)
        site_config_path = this.wht_ctx.site_config().get("FilePath")
        # TODO: Get creds from pywht
        creds = whtService.get_credentials(
            this.base_wht_project.project_path(), site_config_path
        )
        input_model_refs = self.build_spec.get("inputs", [])
        output_filename = TrainingRecipe.get_training_file_path(this)
        project_folder = this.base_wht_project.project_path()
        runtime_info = {"is_rudder_backend": this.base_wht_project.is_rudder_backend()}
        config = self.build_spec.get("ml_config", {})
        input_materials = []
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
            whtService,
            constants.ML_CORE_PYNATIVE_PATH,
            standardize_ref_name(creds["type"], this.name()),
            os.path.join(this.get_output_folder(), this.name()),
        )
