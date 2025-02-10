from typing import Tuple
import os
import uuid
import zipfile
import importlib.resources as resources

from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from .config import SAMPLE_DATA_DIR
from .tutorial import ProfileBuilder


class TutorialModel(BaseModelType):
    TypeName = "profiles_tutorial"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "fast_mode": {"type": "boolean"},
        },
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        build_spec["materialization"] = {
            "output_type": "shell",
            "run_type": "interactive",
        }
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return TutorialRecipe(self.build_spec)

    def validate(self) -> tuple[bool, str]:
        return super().validate()


class TutorialRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        super().__init__()
        self.build_spec = build_spec
        self.logger = Logger("TutorialRecipe")
        self.run_id = str(uuid.uuid4())

    def describe(self, this: WhtMaterial) -> Tuple[str, str]:
        return (
            "# Profiles Tutorial\nThis is a guided interactive tutorial on Rudderstack Profiles. This tutorial will walk through key concepts of profiles and how it works. As a part of this tutorial, we will also build a basic project with an ID Stitcher Model ultimately producing an ID Graph in your warehouse.",
            ".md",
        )

    def register_dependencies(self, this: WhtMaterial):
        pass

    def execute(self, this: WhtMaterial):
        self.logger.info("unzipping sample data...")
        unzip_sample_data(self.logger)

        profile_builder = ProfileBuilder(
            self.reader, self.build_spec.get("fast_mode", False), self.run_id
        )
        profile_builder.run(this)


def unzip_sample_data(logger: Logger):
    zip_file_path = get_sample_data_path()
    # Ensure the zip file exists
    if not zip_file_path:
        raise Exception(f"Error: {zip_file_path} not found.")

    # Unzip the file
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                if not file_info.filename.startswith("__MACOSX"):
                    zip_ref.extract(file_info, ".")
        logger.info(f"Successfully extracted {zip_file_path}")
    except Exception as e:
        raise Exception(f"An error occurred while extracting: {str(e)}")


def get_sample_data_path():
    """Returns the path to the data directory"""
    try:
        zip_file = resources.files("profiles_mlcorelib").joinpath(
            "py_native", "profiles_tutorial", f"{SAMPLE_DATA_DIR}.zip"
        )
        if not zip_file.is_file():
            return None

        return str(zip_file)
    except Exception as e:
        print(f"Error locating zip file: {e}")
        return None
