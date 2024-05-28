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
            "llm_model_name": {"type": ["string", "null"]},
            "run_for_top_k_distinct": {"type": "integer"},
        },
        "required": ["prompt", "prompt_inputs", "run_for_top_k_distinct"]
        + EntityKeyBuildSpecSchema["required"],
        "additionalProperties": False,
    }
    DEFAULT_LLM_MODEL = "llama2-70b-chat"

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)
        if self.build_spec["llm_model_name"] == None:
            self.build_spec["llm_model_name"] = self.DEFAULT_LLM_MODEL

    def get_material_recipe(self) -> PyNativeRecipe:
        return LLMModelRecipe(self.build_spec)

    def validate(self):
        model_limits = {
            "snowflake-arctic": 4096,
            "reka-core": 32000,
            "mistral-large": 32000,
            "reka-flash": 100000,
            "mixtral-8x7b": 32000,
            "llama3-8b": 8000,
            "llama3-70b": 8000,
            "llama2-70b-chat": 4096,
            "mistral-7b": 32000,
            "gemma-7b": 8000,
        }

        prompt = self.build_spec["prompt"]
        input_lst = self.build_spec["prompt_inputs"]
        model_name = self.build_spec["llm_model_name"]

        prompt_tokens = len(prompt.split())
        if model_name in model_limits:
            if prompt_tokens > model_limits[model_name]:
                raise ValueError(
                    f"The prompt exceeds the token limit for model '{model_name}'. Maximum allowed tokens: {model_limits[model_name]}"
                )
        prompt_replaced = prompt.replace("'", "''", -1)
        input_indices = re.findall(r"{(\w+)\[(\d+)\]}", prompt_replaced)
        if len(input_indices) == 0:
            max_index = 0
        else:
            max_index = max(int(index) for _, index in input_indices)

        if max_index >= len(input_lst):
            raise ValueError(
                f"Maximum index {max_index} is out of range for input_columns list."
            )
        return super().validate()


class LLMModelRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.prompt = build_spec["prompt"]
        self.prompt_inputs = build_spec["prompt_inputs"]
        self.logger = Logger("llm_model_recipe")
        self.llm_model_name = build_spec["llm_model_name"]
        self.k_distinct_values = build_spec["run_for_top_k_distinct"]
        self.sql = ""

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def prepare(self, this: WhtMaterial):
        entity = this.model.entity()
        column_name = this.model.name()
        if entity is None:
            raise Exception("no entity found for model")
        input_columns = [
            f"{{{{{var}.Model.DbObjectNamePrefix()}}}}"  # Assuming var is already in the format "user.Var('profession')"
            for var in self.prompt_inputs
        ]
        entity_id_column_name = this.model.entity().get("IdColumnName")
        prompt_replaced = self.prompt.replace(
            "'", "''", -1
        )  # This is to escape single quotes
        input_indices = re.findall(r"{(\w+)\[(\d+)\]}", prompt_replaced)
        for word, index in input_indices:
            index = int(index)
            placeholder = f"{{{word}[{index}]}}"
            col = "' ||" + input_columns[index] + "|| ' "
            prompt_replaced = prompt_replaced.replace(placeholder, col)

        var_table_ref = (
            f"this.DeRef(makePath({self.prompt_inputs[0]}.Model.GetVarTableRef()))"
        )
        # Joined_columns used to create a comma seperated string in order to mention
        # all the columns that are used as input in the query.
        joined_columns = ", ".join(input_columns)
        # Join condition to join the predicted column to the original table in order to mention
        # all the column that are being used in the input columns list.
        join_condition = " AND ".join([f"a.{col} = b.{col}" for col in input_columns])

        # model_creator_sql
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% set entityVarTable = {var_table_ref} %}}
                    -- Common Table Expression (CTE) to get the top k distinct values of specified columns based on their frequency
                    WITH top_k_distinct_attribute AS (
                        SELECT {joined_columns}, COUNT(*) AS frequency
                        FROM {{{{entityVarTable}}}}
                        GROUP BY {joined_columns}
                        ORDER BY frequency DESC
                        LIMIT {self.k_distinct_values}
                    ), predicted_attribute AS (
                        -- CTE to get predicted attributes using the specified model for the top k distinct values
                        SELECT {joined_columns}, SNOWFLAKE.CORTEX.COMPLETE('{self.llm_model_name}','{prompt_replaced}') AS {column_name}
                        FROM top_k_distinct_attribute
                    )
                    SELECT a.{entity_id_column_name}, b.{column_name}
                    FROM {{{{entityVarTable}}}} a
                    -- Perform a LEFT JOIN between the original table and the predicted attributes to fill all the 
                    -- attribute values with their corresponding predicted value.
                    LEFT JOIN predicted_attribute b ON {join_condition}
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this.Name(), selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}
            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}
        """

        self.sql = this.execute_text_template(query_template)
        return

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)
