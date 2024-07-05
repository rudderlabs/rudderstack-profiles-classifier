from typing import List, Dict, Tuple, Union
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
)
from profiles_rudderstack.logger import Logger
import re


class AttributionModel(BaseModelType):
    TypeName = "attribution_report"  # the name of the model type

    # json schema for the build spec
    BuildSpecSchema = {
        "type": "object",
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            "id_column_name": {"type": "string"},
            "spend_inputs": {"type": "array", "items": {"type": "string"}},
            "user_journeys": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "from": {"type": "string"},
                        "timestamp": {"type": "string"},
                        "touch": {
                            "type": "object",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "utm_source": {"type": "string"},
                                    "utm_campaign": {"type": "string"},
                                },
                                "oneOf": [
                                    {"required": ["utm_source", "utm_campaign"]},
                                    {"required": ["utm_source"]},
                                    {"required": ["utm_campaign"]},
                                ],
                            },
                        },
                    },
                    "required": ["from", "timestamp", "touch"],
                },
            },
            "conversions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "timestamp": {"type": "string"},
                        "value": {"type": "string"},
                    },
                    "required": ["name", "timestamp"],
                },
            },
        },
        "required": EntityKeyBuildSpecSchema["required"]
        + ["spend_inputs", "user_journeys", "conversions"],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)
        if "id_column_name" not in self.build_spec:
            self.build_spec[
                "id_column_name"
            ] = f"{self.build_spec['entity_key']}_main_id"

    def get_material_recipe(self) -> PyNativeRecipe:
        return AttributionModelRecipe(self.build_spec)

    def validate(self) -> Tuple[bool, str]:
        return True, "Validated successfully"


class AttributionModelRecipe(PyNativeRecipe):
    def __init__(self, config: Dict) -> None:
        self.logger = Logger("attribution_model")
        self.config = config

        self.inputs = {
            "var_table": f'{self.config["entity_key"]}/all/var_table',
        }
        for spend_input in self.config["spend_inputs"]:
            self.inputs[spend_input] = f"{spend_input}"

        for obj in self.config["user_journeys"]:
            tbl = obj["from"]
            self.inputs[tbl] = tbl
            self.inputs[f"{tbl}/var_table"] = f"{tbl}/var_table"
            self.inputs[
                f"{tbl}/var_table/{self.config['id_column_name']}"
            ] = f"{tbl}/var_table/{self.config['id_column_name']}"
            # self.inputs[f"{tbl}/{obj['timestamp']}"] = f"{tbl}/{obj['timestamp']}"
            # for key in obj["touch"]:
            #     self.inputs[f"{tbl}/{obj['touch'][key]}"] = f"{tbl}/{obj['touch'][key]}"

        for obj in self.config["conversions"]:
            for key, value in obj.items():
                if key != "name":
                    self.inputs[value] = f'entity/{self.config["entity_key"]}/{value}'

    def describe(self, this: WhtMaterial):
        description = """You can see the output table in the warehouse where each touchpoint has an attribution score."""
        return description, ".txt"

    def register_dependencies(self, this: WhtMaterial):
        for key in self.inputs:
            this.de_ref(self.inputs[key])

        id_column_name = self.config["id_column_name"]
        user_journeys = self.config["user_journeys"]
        spend_inputs = self.config["spend_inputs"]
        conversions = self.config["conversions"]

        # creating user journeys
        journey_query, union_op, set_jouney_ref = "", "", ""
        prefix, counter = "Table", 1
        for journey_info in user_journeys:
            set_jouney_ref = (
                set_jouney_ref
                + f"{prefix}{counter} = this.DeRef('{journey_info['from']}/var_table') "
            )
            select_info = (
                f"SELECT {id_column_name}, {journey_info['timestamp']} AS timestamp, "
                + ", ".join(
                    [
                        f"{journey_info['touch'][key]} AS {key}"
                        if key in journey_info["touch"]
                        else f"null AS {key}"
                        for key in ("utm_source", "utm_campaign")
                    ]
                )
            )
            from_info = f"FROM {{{{{prefix}{counter}}}}}"
            where_info = f"WHERE " + " AND ".join(
                [f"{journey_info['touch'][key]} != ''" for key in journey_info["touch"]]
            )

            journey_query = f"""{journey_query} 
                                {union_op} 
                                {select_info} 
                                {from_info} 
                                {where_info}"""
            union_op = f"UNION ALL "
            counter += 1

        # creating user conversion
        conversion_query = ""
        for conversion_info in conversions:
            select_info = (
                f"SELECT {id_column_name}, {conversion_info['timestamp']} AS converted_date, "
                + (
                    f"{conversion_info['value']} AS conversion_value"
                    if "value" in conversion_info
                    else "1 AS conversion_value"
                )
            )
            from_info = f"FROM {{{{entityVarTable}}}}"
            where_info = f"WHERE {conversion_info['timestamp']} is not NULL"

            # TODO: need to change this to multi-conversion
            conversion_query = f"""
                                {select_info} 
                                {from_info} 
                                {where_info}"""
            break

        # model_creator_sql
        input_material_template = "this.DeRef('entity/user/var_table')"
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% with entityVarTable = {input_material_template} {set_jouney_ref} %}}
                    WITH user_view AS
                        (
                        SELECT distinct
                                journey.{id_column_name} as {id_column_name},
                                first_value(DATE(journey.timestamp)) over (partition by journey.{id_column_name} order by journey.timestamp asc) as first_touch_date,
                                first_value(DATE(journey.timestamp)) over (partition by journey.{id_column_name} order by journey.timestamp desc) as last_touch_date,
                                first_value(conversion_value) over (partition by journey.{id_column_name} order by journey.timestamp asc) as first_touch_conversion_value,
                                first_value(conversion_value) over (partition by journey.{id_column_name} order by journey.timestamp desc) as last_touch_conversion_value,
                                first_value(utm_source) over (partition by journey.{id_column_name} order by journey.timestamp asc) as first_touch_source,
                                first_value(utm_campaign) over (partition by journey.{id_column_name} order by journey.timestamp asc) as first_touch_campaign,
                                first_value(utm_source) over (partition by journey.{id_column_name} order by journey.timestamp desc) as last_touch_source,
                                first_value(utm_campaign) over (partition by journey.{id_column_name} order by journey.timestamp desc) as last_touch_campaign,
                        FROM (
                            {conversion_query}
                        ) AS conversion_tbl
                        JOIN 
                        (
                            {journey_query}
                        ) AS journey
                        ON conversion_tbl.{id_column_name} = journey.{id_column_name} and journey.timestamp <= conversion_tbl.converted_date),
                    first_touch_view as
                        (
                        select first_touch_date as timestamp, 
                            first_touch_source as source, 
                            first_touch_campaign as campaign, 
                            count(distinct {id_column_name}) as first_touch_count, 
                            sum(first_touch_conversion_value) as first_touch_conversion_value
                        from user_view 
                        group by first_touch_date, first_touch_source, first_touch_campaign
                        order by first_touch_date desc),
                    last_touch_view as
                        (
                        select last_touch_date as timestamp, 
                            last_touch_source as source, 
                            last_touch_campaign as campaign, 
                            count(distinct {id_column_name}) as last_touch_count, 
                            sum(last_touch_conversion_value) as last_touch_conversion_value
                        from user_view 
                        group by last_touch_date, last_touch_source, last_touch_campaign
                        order by last_touch_date desc)


                    select coalesce(first_touch_view.timestamp, last_touch_view.timestamp) as timestamp, 
                    coalesce(first_touch_view.source, last_touch_view.source) as source, 
                    coalesce(first_touch_view.campaign, last_touch_view.campaign) as campaign,
                    coalesce(first_touch_count, 0) as first_touch_count,
                    coalesce(last_touch_count, 0) as last_touch_count,
                    coalesce(first_touch_conversion_value, 0) AS first_touch_conversion_value,
                    coalesce(last_touch_conversion_value, 0) AS last_touch_conversion_value,
                    from first_touch_view 
                    full outer join last_touch_view 
                    on first_touch_view.timestamp = last_touch_view.timestamp
                    AND first_touch_view.source = last_touch_view.source
                    AND first_touch_view.campaign = last_touch_view.campaign
                    ORDER BY timestamp desc

                    {{% endwith %}}
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this.Name(), selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}
            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""

        # file_name = "sql_query_output.txt"
        # with open(file_name, "w") as file:
        #     file.write(query_template)
        # print(f"String has been written to ================= {file_name}")

        self.sql = this.execute_text_template(query_template)
        return

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)
