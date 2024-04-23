from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import EntityKeyBuildSpecSchema
from profiles_rudderstack.logger import Logger

class LLMModel(BaseModelType):
    TypeName = "llm_model"  # the name of the model type

    # json schema for the build spec
    BuildSpecSchema = {
        "type": "object",
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            "prompt": {"type": "string"},
            "prompt_inputs": {"type": "array", "items": {"type": "string"}},
            "llm_model_name": {"type": "string"},
        },
        "required": ["prompt","prompt_inputs"] + EntityKeyBuildSpecSchema["required"],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return LLMModelRecipe(self.build_spec)

    def validate(self):
        return super().validate()


class LLMModelRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.prompt = build_spec["prompt"]
        self.prompt_inputs = build_spec["prompt_inputs"]
        self.logger = Logger("llm_model_recipe")
        self.llm_model_name = build_spec["llm_model_name"]
        self.sql = ""

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def prepare(self, this: WhtMaterial):
        self.sql = ""
        return
    def execute(self, this: WhtMaterial):
        result = this.wht_ctx.client.query_sql_with_result(self.sql)
        return result