from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
    FeatureDetailsBuildSpecSchema,
    EntityIdsBuildSpecSchema,
)
from profiles_rudderstack.logger import Logger
import re


class LLMModel(BaseModelType):
    TypeName = "llm_model"  # the name of the model type

    # json schema for the build spec
    BuildSpecSchema = {
        "type": "object",
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            **FeatureDetailsBuildSpecSchema["properties"],
            **EntityIdsBuildSpecSchema["properties"],
            "prompt": {"type": "string"},
            "var_inputs": {"type": ["array", "null"], "items": {"type": "string"}},
            "table_inputs": {
                "type": ["array", "null"],
                "items": {
                    "type": "object",
                    "properties": {
                        "select": {"type": "string"},
                        "from": {"type": "string"},
                    },
                    "required": ["select", "from"],
                },
            },
            "llm_model_name": {"type": ["string", "null"]},
            "run_for_top_k_distinct": {"type": ["integer", "null"]},
        },
        "required": ["prompt", "prompt_inputs"] + EntityKeyBuildSpecSchema["required"],
        "additionalProperties": False,
    }
    DEFAULT_LLM_MODEL = "llama2-70b-chat"

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        if "ids" not in build_spec:
            build_spec["ids"] = []

            build_spec["ids"].append(
                {"select": "user_main_id", "type": "rudder_id", "entity": "user"}
            )
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
        var_inputs = self.build_spec["var_inputs"]
        table_inputs = self.build_spec["table_inputs"]
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

        if type(var_inputs) is not type(None) and max_index >= len(var_inputs):
            raise ValueError(
                f"Maximum index {max_index} is out of range for var_inputs list."
            )
        if type(table_inputs) is not type(None) and max_index >= len(table_inputs):
            raise ValueError(
                f"Maximum index {max_index} is out of range for table_inputs list."
            )
        return super().validate()


class LLMModelRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.prompt = build_spec["prompt"]
        self.var_inputs = build_spec["var_inputs"] if build_spec["var_inputs"] else None
        self.table_inputs = (
            build_spec["table_inputs"] if build_spec["table_inputs"] else None
        )
        self.logger = Logger("llm_model_recipe")
        self.llm_model_name = build_spec["llm_model_name"]
        self.k_distinct_values = (
            build_spec["run_for_top_k_distinct"]
            if build_spec["run_for_top_k_distinct"]
            else None
        )
        self.sql = ""

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def query_template_360(
        self,
        var_table_ref,
        limit_top_k,
        prompt_replaced,
        column_name,
        entity_id_column_name,
        join_condition,
        joined_columns,
    ):
        # model_creator_sql
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% set entityVarTable = {var_table_ref} %}}
                    -- Common Table Expression (CTE) to get the distinct values of specified columns based on their frequency
                    WITH top_k_distinct_attribute AS (
                        SELECT {joined_columns}, COUNT(*) AS frequency
                        FROM {{{{entityVarTable}}}}
                        GROUP BY {joined_columns}
                        {limit_top_k}
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
            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""
        return query_template

    def register_dependencies(self, this: WhtMaterial):
        entity_id_column_name = this.model.entity().get("IdColumnName")
        limit_top_k = (
            f"ORDER BY frequency DESC LIMIT {self.k_distinct_values}"
            if self.k_distinct_values
            else ""
        )
        if self.var_inputs:
            var_table_ref = (
                f"this.DeRef(makePath({self.var_inputs[0]}.Model.GetVarTableRef()))"
            )
            entity = this.model.entity()
            column_name = this.model.name()
            if entity is None:
                raise Exception("no entity found for model")
            input_columns_vars = [
                f"{{{{{var}.Model.DbObjectNamePrefix()}}}}"  # Assuming var is already in the format "user.Var('profession')"
                for var in self.var_inputs
            ]
            prompt_replaced = self.prompt.replace(
                "'", "''", -1
            )  # This is to escape single quotes
            input_indices = re.findall(r"{(\w+)\[(\d+)\]}", prompt_replaced)
            for word, index in input_indices:
                index = int(index)
                placeholder = f"{{{word}[{index}]}}"
                col = "' ||" + input_columns_vars[index] + "|| ' "
                prompt_replaced = prompt_replaced.replace(placeholder, col)

            # Joined_columns used to create a comma seperated string in order to mention
            # all the columns that are used as input in the query.
            joined_columns = ", ".join(input_columns_vars)
            # Join condition to join the predicted column to the original table in order to mention
            # all the column that are being used in the input columns list.
            join_condition = " AND ".join(
                [f"a.{col} = b.{col}" for col in input_columns_vars]
            )

            query_template = LLMModelRecipe.query_template_360(
                self,
                var_table_ref,
                limit_top_k,
                prompt_replaced,
                column_name,
                entity_id_column_name,
                join_condition,
                joined_columns,
            )
            self.sql = this.execute_text_template(query_template)

        elif self.table_inputs:
            table_column_name = this.model.name()
            table_columns = []
            for var in self.table_inputs:
                column_name = var.get("select")
                if column_name:
                    table_columns.append(column_name)
                else:
                    print(
                        "Invalid entry found in prompt_inputs: 'select' key is missing"
                    )
            table_name = self.table_inputs[0].get("from")
            prompt_replaced = self.prompt.replace(
                "'", "''", -1
            )  # This is to escape single quotes
            input_indices = re.findall(r"{(\w+)\[(\d+)\]}", prompt_replaced)
            for word, index in input_indices:
                index = int(index)
                if 0 <= index < len(table_columns):
                    placeholder = f"{{{word}[{index}]}}"
                    col = "' ||" + table_columns[index] + "|| ' "
                    prompt_replaced = prompt_replaced.replace(placeholder, col)
                else:
                    self.logger.error(
                        f"Index {index} out of range for input_columns list."
                    )
            dereferencing_string = f"{table_name}/var_Table/{entity_id_column_name}"
            column = this.de_ref(dereferencing_string)
            table_reference_string = f"{table_name}/var_Table"
            var_table_ref = f"this.DeRef('{table_reference_string}')"
            joined_columns = ", ".join(table_columns)
            join_condition = " AND ".join(
                [f"a.{col} = b.{col}" for col in table_columns]
            )
            query_template = LLMModelRecipe.query_template_360(
                self,
                var_table_ref,
                limit_top_k,
                prompt_replaced,
                table_column_name,
                entity_id_column_name,
                join_condition,
                joined_columns,
            )
            self.sql = this.execute_text_template(query_template)

        return

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)
