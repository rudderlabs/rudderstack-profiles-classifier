from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import EntityKeyBuildSpecSchema
from profiles_rudderstack.logger import Logger
from typing import List
import re


class SnowflakeCortexModel(BaseModelType):
    TypeName = "sf_cortex"  # the name of the model type

    # json schema for the build spec
    BuildSpecSchema = {
        "type": "object",
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            "prompt": {"type": "string"},
            "vars": {"type": "array", "items": {"type": "string"}},
            "llm_model_name": {"type": "string"},
        },
        "required": ["prompt","vars"] + EntityKeyBuildSpecSchema["required"],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return SnowflakeCortexRecipe(
            self.build_spec["prompt"],
            self.build_spec["vars"],
            self.build_spec["llm_model_name"],
        )

    def validate(self):
        return super().validate()


class SnowflakeCortexRecipe(PyNativeRecipe):
    def __init__(self, prompt: str, vars: List[str], llm_model_name: str) -> None:
        self.prompt = prompt
        self.vars = vars
        self.logger = Logger("sf_cortex_recipe")
        self.llm_model_name = llm_model_name
        self.sql = ""

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def prepare(self, this: WhtMaterial):
        self.sql = ""
        return
    def execute(self, this: WhtMaterial):
        result = this.wht_ctx.client.query_sql_with_result(self.sql)
        return result