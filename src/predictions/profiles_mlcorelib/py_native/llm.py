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
            "eligible_users": {"type": ["string", "null"]},
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
            "sql_inputs": {"type": "array", "items": {"type": "string"}},
            "llm_model_name": {"type": ["string", "null"]},
            "run_for_top_k_distinct": {"type": ["integer", "null"]},
        },
        "required": ["prompt"] + EntityKeyBuildSpecSchema["required"],
        "oneOf": [{"required": ["var_inputs"]}, {"required": ["table_inputs"]}],
        "additionalProperties": False,
    }
    DEFAULT_VALUES = {
        "eligible_users": "",
        "sql_inputs": [],
        "llm_model_name": "llama2-70b-chat",
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        if "ids" not in build_spec:
            entity_key = build_spec["entity_key"]
            # TODO - select should be computed from the entity object
            build_spec["ids"] = [
                {
                    "select": entity_key + "_main_id",
                    "type": "rudder_id",
                    "entity": entity_key,
                }
            ]
        super().__init__(build_spec, schema_version, pb_version)
        for key in self.DEFAULT_VALUES.keys():
            if key not in self.build_spec or self.build_spec[key] is None:
                self.build_spec[key] = self.DEFAULT_VALUES[key]

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
        eligible_users = self.build_spec["eligible_users"]
        if "var_inputs" in self.build_spec:
            var_inputs = self.build_spec["var_inputs"]
            table_inputs = None
        else:
            var_inputs = None
            table_inputs = self.build_spec["table_inputs"]
        sql_inputs_lst = self.build_spec["sql_inputs"]
        model_name = self.build_spec["llm_model_name"]
        prompt_tokens = len(prompt.split())
        if model_name in model_limits:
            if prompt_tokens > model_limits[model_name]:
                raise ValueError(
                    f"The prompt exceeds the token limit for model '{model_name}'. Maximum allowed tokens: {model_limits[model_name]}"
                )

        input_indices = []
        for text_input in (prompt, eligible_users):
            text_input_replaced = text_input.replace("'", "''", -1)
            input_indices.extend(re.findall(r"{(\w+)\[(\d+)\]}", text_input_replaced))
        max_var_inputs_index, max_sql_inputs_index = -1, -1
        if len(input_indices) > 0:
            max_var_inputs_index = max(
                (int(index) for word, index in input_indices if word == "var_inputs"),
                default=-1,
            )
            max_sql_inputs_index = max(
                (int(index) for word, index in input_indices if word == "sql_inputs"),
                default=-1,
            )

        validation_inputs = var_inputs if var_inputs is not None else table_inputs

        if validation_inputs is not None and max_var_inputs_index >= len(
            validation_inputs
        ):
            raise ValueError(
                f"Maximum index {max_var_inputs_index} is out of range for the inputs list."
            )

        if max_sql_inputs_index >= len(sql_inputs_lst):
            raise ValueError(
                f"Maximum index {max_sql_inputs_index} is out of range for sql_inputs list."
            )
        if table_inputs:
            from_tables = set()
            for var in table_inputs:
                table_name = var.get("from")
                from_tables.add(table_name)
            if len(from_tables) > 1:
                raise ValueError(
                    "The model doesn't support specifying more than 1 input tables"
                )

        return super().validate()


class Utility:
    def __init__(self):
        pass

    def get_index_list(self, text_input):
        return re.findall(r"{(\w+)\[(\d+)\]}", text_input)

    def replace_placeholders(
        self,
        var_inputs_indices,
        eligible_users_indices,
        task_prompt,
        eligible_users_prompt,
        input_columns_vars,
        sql_inputs_df=None,
    ):
        def replace_inputs_references(word, index, original_prompt, replacement_prompt):
            placeholder = f"{{{word}[{index}]}}"
            replaced_prompt = original_prompt.replace(placeholder, replacement_prompt)
            return replaced_prompt

        var_inputs_prompt_replaced, eligible_users_prompt_replaced = (
            task_prompt,
            eligible_users_prompt,
        )
        for word, index in var_inputs_indices:
            replacement_prompt = ""
            index = int(index)
            if word == "var_inputs":
                replacement_prompt = "' ||" + input_columns_vars[index] + "|| '"
            elif word == "sql_inputs" and sql_inputs_df:
                replacement_prompt = sql_inputs_df[index]
            var_inputs_prompt_replaced = replace_inputs_references(
                word, index, var_inputs_prompt_replaced, replacement_prompt
            )

        for word, index in eligible_users_indices:
            replacement_prompt = ""
            index = int(index)
            if word == "var_inputs":
                replacement_prompt = input_columns_vars[index]
            eligible_users_prompt_replaced = replace_inputs_references(
                word, index, eligible_users_prompt_replaced, replacement_prompt
            )

        return var_inputs_prompt_replaced, eligible_users_prompt_replaced


class LLMModelRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        self.prompt = build_spec["prompt"]
        self.eligible_users = build_spec["eligible_users"]
        if "var_inputs" in build_spec:
            self.var_inputs = build_spec["var_inputs"]
            self.table_inputs = None
        else:
            self.var_inputs = None
            self.table_inputs = build_spec["table_inputs"]
        self.logger = Logger("llm_model_recipe")
        self.llm_model_name = build_spec["llm_model_name"]
        self.k_distinct_values = (
            build_spec["run_for_top_k_distinct"]
            if "run_for_top_k_distinct" in build_spec
            else None
        )
        self.sql_inputs = build_spec["sql_inputs"]
        self.sql = ""

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def query_template_creator(
        self,
        input_material_template,
        column_name,
        entity_id_column_name,
        input_columns_vars,
        sql_inputs_df,
    ):
        limit_top_k = (
            f"ORDER BY frequency DESC LIMIT {self.k_distinct_values}"
            if self.k_distinct_values
            else ""
        )

        utils = Utility()
        var_inputs_indices = utils.get_index_list(self.prompt)
        eligible_users_indices = utils.get_index_list(self.eligible_users)
        (
            var_inputs_prompt_replaced,
            eligible_users_prompt_replaced,
        ) = utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.prompt,
            self.eligible_users,
            input_columns_vars,
            sql_inputs_df,
        )

        # Joined_columns used to create a comma seperated string in order to mention
        # all the columns that are used as input in the query.
        joined_columns = ", ".join(input_columns_vars)
        # Join condition to join the predicted column to the original table in order to mention
        # all the column that are being used in the input columns list.
        join_condition = " AND ".join(
            [f"a.{col} = b.{col}" for col in input_columns_vars]
        )

        filtered_entity_var_str = (
            f"WHERE {eligible_users_prompt_replaced}"
            if eligible_users_prompt_replaced
            else ""
        )
        filtered_entity_var_table_cte = f"WITH filtered_entity_var_table AS (SELECT * FROM {{{{entityVarTable}}}} {filtered_entity_var_str})"

        # model_creator_sql
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% set entityVarTable = {input_material_template} %}}
                    -- Common Table Expression (CTE) to get the distinct values of specified columns based on their frequency
                    {filtered_entity_var_table_cte}, top_k_distinct_attribute AS (
                        SELECT {joined_columns}, COUNT(*) AS frequency
                        FROM filtered_entity_var_table
                        GROUP BY {joined_columns}
                        {limit_top_k}
                    ), predicted_attribute AS (
                        -- CTE to get predicted attributes using the specified model for the top k distinct values
                        SELECT {joined_columns}, SNOWFLAKE.CORTEX.COMPLETE('{self.llm_model_name}','{var_inputs_prompt_replaced}') AS {column_name}
                        FROM top_k_distinct_attribute
                    )
                    SELECT a.{entity_id_column_name}, b.{column_name}
                    FROM filtered_entity_var_table a
                    -- Perform a LEFT JOIN between the original table and the predicted attributes to fill all the
                    -- attribute values with their corresponding predicted value.
                    LEFT JOIN predicted_attribute b ON {join_condition}
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this, selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}
            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""
        return query_template

    def register_dependencies(self, this: WhtMaterial):
        entity_id_column_name = this.model.entity().get("IdColumnName")
        column_name = this.model.name()
        sql_inputs_df = None
        if not this.wht_ctx.is_null_ctx and len(self.sql_inputs) > 0:
            sql_inputs_df = [
                this.wht_ctx.client.query_sql_with_result(sql_query).to_json(
                    orient="records"
                )
                for sql_query in self.sql_inputs
            ]

        if self.var_inputs:
            var_table_material_template = (
                f"this.DeRef(makePath({self.var_inputs[0]}.Model.GetVarTableRef()))"
            )
            entity = this.model.entity()
            if entity is None:
                raise Exception("no entity found for model")
            input_columns_vars = [
                f"{{{{{var}.Model.DbObjectNamePrefix()}}}}"  # Assuming var is already in the format "user.Var('profession')"
                for var in self.var_inputs
            ]

            query_template = LLMModelRecipe.query_template_creator(
                self,
                var_table_material_template,
                column_name,
                entity_id_column_name,
                input_columns_vars,
                sql_inputs_df,
            )
            self.sql = this.execute_text_template(query_template)

        elif self.table_inputs:
            table_columns = []
            for var in self.table_inputs:
                input_column_name = var.get("select")
                table_columns.append(input_column_name)

            table_name = self.table_inputs[0].get("from")
            entity_id_column_material = this.de_ref(
                f"{table_name}/var_table/{entity_id_column_name}"
            )
            var_table_material_template = f"this.DeRef('{f'{table_name}/var_table'}')"

            query_template = LLMModelRecipe.query_template_creator(
                self,
                var_table_material_template,
                column_name,
                entity_id_column_name,
                table_columns,
                sql_inputs_df,
            )
            self.sql = this.execute_text_template(query_template)

        return

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)
