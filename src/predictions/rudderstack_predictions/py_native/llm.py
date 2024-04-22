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
        "required": ["vars"] + EntityKeyBuildSpecSchema["required"],
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
        if self.llm_model_name == "":
            self.llm_model_name = "llama2-70b-chat"
        entity = this.model.entity()
        entity_key = entity["Name"]  # get entity key

        # input_columns = ', '.join([
        #     # example - {{user.Var('var1').Model.Name()}} -> returns the col name of the var
        #     f"{{{{{entity_key}.Var('{var}').Model.Name()}}}}"
        #     for var in self.vars
        # ])
        input_columns = [
            # example - {{user.Var('var1').Model.Name()}} -> returns the col name of the var
            f"{{{{{entity_key}.Var('{var}').Model.Name()}}}}"
            for var in self.vars
        ]
        prompt_replaced = self.prompt
        input_indices = re.findall(r"{(\w+)\[(\d+)\]}", prompt_replaced)
        for word, index in input_indices:
            index = int(index)
            if 0 <= index < len(input_columns):
                placeholder = f"{{{word}[{index}]}}"
                col = "' ||" + input_columns[index] + "|| ' "
                prompt_replaced = prompt_replaced.replace(placeholder, col)
            else:
                self.logger.error(f"Index {index} out of range for input_columns list.")

        # get the var table from a var
        var_table_ref = f"this.DeRef(makePath({entity_key}.Var('{self.vars[0]}').Model.GetVarTableRef()))"

        # model_creator_sql
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% set entityVarTable = {var_table_ref} %}}

                    SELECT *,SNOWFLAKE.CORTEX.COMPLETE('{self.llm_model_name}','{prompt_replaced}') AS Gender
                        FROM {{{{entityVarTable}}}}
                         LIMIT 10;
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this.Name(), selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}

            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""

        # Assigning the SQL query to the class attribute
        self.sql = this.execute_text_template(query_template)

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)
