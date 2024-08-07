import os
import shutil
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

from ..utils import constants, utils
from ..utils.S3Utils import S3Utils

from ..wht.pyNativeWHT import PyNativeWHT
from ..train import _train
from datetime import datetime, timedelta, timezone
import pandas as pd


class TrainingModel(BaseModelType):
    TypeName = "training_model"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "occurred_at_col": {"type": "string"},
            **EntityKeyBuildSpecSchema["properties"],
            **MaterializationBuildSpecSchema["properties"],
            "training_file_lookup_path": {"type": "string"},
            "validity_time": {"type": "string", "enum": ["day", "week", "month"]},
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
            or "requested_enable_status" not in build_spec["materialization"]
        ):
            # This will ensure that the training model won't run if there is no prediction model in the project
            build_spec["materialization"] = {
                "requested_enable_status": "only_if_necessary"
            }
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

    def _skip_training(self, this: WhtMaterial):
        if not self.build_spec.get("validity_time"):
            return False
        site_config_path = this.wht_ctx.site_config().get("FilePath")
        py_models = utils.load_yaml(site_config_path)["py_models"]
        mat_name_without_seq = this.name().rsplit("_", 1)[0]
        if "credentials_presets" in py_models:
            # This flow is for runs in the UI
            credentials_presets = py_models["credentials_presets"]
            if "run_artefacts_s3" in credentials_presets:
                run_artefacts_config = credentials_presets["run_artefacts_s3"]
                bucket = run_artefacts_config["bucket"]
                region = run_artefacts_config["region"]
                path = run_artefacts_config["path"]
                self.logger.info(f"Checking for training files in {bucket} at {path}")
                return S3Utils.download_training_artifacts(
                    bucket,
                    region,
                    path,
                    os.path.join(this.get_output_folder(), this.name()),
                    mat_name_without_seq,
                    self._map_validity_to_timestamp(datetime.now(timezone.utc)),
                )
        # This flow is for runs from CLI
        if self.build_spec.get("training_file_lookup_path"):
            lookup_path = self.build_spec.get("training_file_lookup_path")
            if not os.path.exists(lookup_path):
                raise Exception(f"Path {lookup_path} does not exist")
            for root, dirs, _ in os.walk(lookup_path):
                for subdir in dirs:
                    if mat_name_without_seq in subdir:
                        subdir_path = os.path.join(root, subdir)
                        creation_time = os.path.getctime(subdir_path)
                        creation_datetime = datetime.fromtimestamp(creation_time)
                        if creation_datetime >= self._map_validity_to_timestamp(
                            datetime.now()
                        ):
                            subdir_files = [
                                f
                                for f in os.listdir(subdir_path)
                                if os.path.isfile(os.path.join(subdir_path, f))
                            ]
                            if len(subdir_files) == 0:
                                # Run probably failed that is why no files are present
                                continue
                            dest_dir = os.path.join(
                                this.get_output_folder(), this.name()
                            )
                            os.makedirs(dest_dir, exist_ok=True)
                            for file in os.listdir(subdir_path):
                                src_path = os.path.join(subdir_path, file)
                                dst_path = os.path.join(dest_dir, file)
                                if os.path.isdir(src_path):
                                    shutil.copytree(src_path, dst_path)
                                else:
                                    shutil.copy2(src_path, dst_path)
                            self.logger.info(
                                f"Valid training file found for {this.name()} in {subdir_path}"
                            )
                            return True
        return False

    def _map_validity_to_timestamp(self, current_time):
        validity_time = self.build_spec.get("validity_time")
        time_delta = {
            "day": timedelta(days=1),
            "week": timedelta(weeks=1),
            "month": timedelta(days=30),
        }
        return current_time - time_delta[validity_time]

    def register_dependencies(self, this: WhtMaterial):
        for input in self.build_spec["inputs"]:
            this.de_ref(input)

    def get_training_file_path(this: WhtMaterial):
        folder = this.get_output_folder()
        return f"{folder}/{this.name()}/training_file"

    def execute(self, this: WhtMaterial):
        logger.set_logger(self.logger)
        if self._skip_training(this):
            self.logger.info(f"Skipping training for {this.name()}")
            # pb expects every model to create a material
            data = {"dummy_column": ["dummy_value"]}
            df = pd.DataFrame(data)
            this.write_output(df)
            return
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
        input_material_names = []
        for input in self.build_spec["inputs"]:
            material = this.de_ref(input)
            input_material_names.append(material.name())
        _train(
            creds,
            input_material_names,
            output_filename,
            config,
            site_config_path,
            project_folder,
            runtime_info,
            None,
            whtService,
            constants.ML_CORE_PYNATIVE_PATH,
            standardize_ref_name(creds["type"], this.name()),
            os.path.join(this.get_output_folder(), this.name()),
        )
