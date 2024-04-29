from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import EntityKeyBuildSpecSchema
from profiles_rudderstack.logger import Logger
import re

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
        "required": ["prompt", "prompt_inputs"] + EntityKeyBuildSpecSchema["required"],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        DEFAULT_LLM_MODEL = "llama2-70b-chat"
        return LLMModelRecipe(self.build_spec)

    def validate(self):
        valid_models = ["mistral-large", "reka-flash", "mixtral-8x7b", "llama2-70b-chat", "mistral-7b", "gemma-7b"]
        if self.build_spec['llm_model_name'] not in valid_models:
            raise ValueError(
                f"Invalid model name: {self.build_spec['llm_model_name']}. Valid options are: {', '.join(valid_models)}")

        prompt = self.build_spec["prompt"]
        input_count = prompt.count('{')
        input_lst = self.build_spec["prompt_inputs"]
        prompt_input_count = len(input_lst)
        if input_count != prompt_input_count:
            raise ValueError(
                f"Invalid number number of inputs")
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
        entity = this.model.entity()
        name = this.model.name()
        if entity is None:
            raise Exception("no entity found for model")
        input_columns = [
            f"{{{{{var}.Model.Name()}}}}"  # Assuming var is already in the format "user.Var('profession')"
            for var in self.prompt_inputs
        ]

        prompt_replaced = self.prompt.replace("'", "''", -1)  # This is to escape single quotes
        input_indices = re.findall(r'{(\w+)\[(\d+)\]}', prompt_replaced)
        for word, index in input_indices:
            index = int(index)
            if 0 <= index < len(input_columns):
                placeholder = f'{{{word}[{index}]}}'
                col = "' ||" + input_columns[index] + "|| ' "
                prompt_replaced = prompt_replaced.replace(placeholder, col)
            else:
                self.logger.error(
                    f"Index {index} out of range for input_columns list.")

        # get the var table from a var
        # var_table_ref = f"this.DeRef(makePath({entity_key}.Var('{self.vars[0]}').Model.GetVarTableRef()))"
        var_table_ref = f"this.DeRef(makePath({self.prompt_inputs[0]}.Model.GetVarTableRef()))"

        # model_creator_sql
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% set entityVarTable = {var_table_ref} %}}

                    SELECT *, SNOWFLAKE.CORTEX.COMPLETE('{self.llm_model_name}','{prompt_replaced}') AS {name} FROM {{{{entityVarTable}}}}
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this.Name(), selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}

            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""

        self.sql = this.execute_text_template(query_template)
        return

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)

