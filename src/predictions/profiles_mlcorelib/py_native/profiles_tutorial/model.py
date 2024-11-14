import pkg_resources
import os
import zipfile
from typing import Tuple

from .tutorial import ProfileBuilder

from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger


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
        self.fast_mode = build_spec.get("fast_mode", False)
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return TutorialRecipe(self.fast_mode)

    def validate(self) -> tuple[bool, str]:
        return super().validate()


class TutorialRecipe(PyNativeRecipe):
    def __init__(self, fast_mode: bool) -> None:
        super().__init__()
        self.fast_mode = fast_mode
        self.logger = Logger("TutorialRecipe")

    def describe(self, this: WhtMaterial) -> Tuple[str, str]:
        return (
            "# Profiles Tutorial\nThis is a guided interactive tutorial on Rudderstack Profiles. This tutorial will walk through key concepts of profiles and how it works. As a part of this tutorial, we will also build a basic project with an ID Stitcher Model ultimately producing an ID Graph in your warehouse.",
            ".md",
        )

    def register_dependencies(self, this: WhtMaterial):
        pass

    def execute(self, this: WhtMaterial):
        if not os.path.exists("sample_data"):
            self.logger.info("unzipping sample data...")
            unzip_sample_data(self.logger)

        profile_builder = ProfileBuilder(self.reader, self.fast_mode)
        wh_client = this.wht_ctx.client
        profile_builder.run(wh_client)


def get_sample_data_path():
    """Returns the path to the data directory"""
    return pkg_resources.resource_filename(
        "profiles_mlcorelib", "py_native/profiles_tutorial/sample_data.zip"
    )


def unzip_sample_data(logger: Logger):
    zip_file_path = get_sample_data_path()
    # Ensure the zip file exists
    if not os.path.exists(zip_file_path):
        raise Exception(f"Error: {zip_file_path} not found.")
    # Unzip the file
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                if not file_info.filename.startswith("__MACOSX"):
                    zip_ref.extract(file_info, ".")
        logger.info(f"Successfully extracted {zip_file_path} to the current directory")
    except Exception as e:
        raise Exception(f"An error occurred while extracting: {str(e)}")
