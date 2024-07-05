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

        for obj in self.config["conversions"]:
            for key, value in obj.items():
                if key != "name":
                    self.inputs[value] = f'entity/{self.config["entity_key"]}/{value}'

    def describe(self, this: WhtMaterial):
        description = """You can see the output table in the warehouse where each touchpoint has an attribution score."""
        return description, ".txt"

    def _create_with_query_template(
        self,
        conversion_name: str,
        id_column_name: str,
        value_flag: bool,
        journey_query: str,
        conversion_query: str,
    ):
        user_view_query = (
            f"""
                    {conversion_name}_user_view AS
                        (
                        SELECT distinct
                                journey.{id_column_name} as {id_column_name},
                                first_value(DATE(journey.timestamp)) over (partition by journey.{id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_date,
                                first_value(DATE(journey.timestamp)) over (partition by journey.{id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_date,
                                """
            + (
                f"""first_value(conversion_value) over (partition by journey.{id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_conversion_value,
                                first_value(conversion_value) over (partition by journey.{id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_conversion_value,"""
                if value_flag
                else ""
            )
            + f"""
                                first_value(utm_source) over (partition by journey.{id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_source,
                                first_value(utm_campaign) over (partition by journey.{id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_campaign,
                                first_value(utm_source) over (partition by journey.{id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_source,
                                first_value(utm_campaign) over (partition by journey.{id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_campaign,
                        FROM (
                            {conversion_query}
                        ) AS conversion_tbl
                        JOIN 
                        (
                            {journey_query}
                        ) AS journey
                        ON conversion_tbl.{id_column_name} = journey.{id_column_name} and journey.timestamp <= conversion_tbl.converted_date)        
                    """
        )
        first_touch_view_query = (
            f"""
                                    {conversion_name}_first_touch_view as
                                    (
                                    select first_touch_date as date, 
                                        first_touch_source as source, 
                                        first_touch_campaign as campaign, 
                                        count(distinct {id_column_name}) as first_touch_count, 
                                        """
            + (
                f"""sum(first_touch_conversion_value) as first_touch_conversion_value"""
                if value_flag
                else ""
            )
            + f"""
                                    from {conversion_name}_user_view 
                                    group by first_touch_date, first_touch_source, first_touch_campaign)
                                    """
        )
        last_touch_view_query = (
            f"""
                                    {conversion_name}_last_touch_view as
                                    (
                                    select last_touch_date as date, 
                                        last_touch_source as source, 
                                        last_touch_campaign as campaign, 
                                        count(distinct {id_column_name}) as last_touch_count,
                                        """
            + (
                f"""sum(last_touch_conversion_value) as last_touch_conversion_value"""
                if value_flag
                else ""
            )
            + f"""
                                    from {conversion_name}_user_view 
                                    group by last_touch_date, last_touch_source, last_touch_campaign)
                                    """
        )
        conversion_view_query = (
            f"""
                                {conversion_name}_conversion_view AS
                                (
                                    select coalesce({conversion_name}_first_touch_view.date, {conversion_name}_last_touch_view.date) as date, 
                                            coalesce({conversion_name}_first_touch_view.source, {conversion_name}_last_touch_view.source) as source, 
                                            coalesce({conversion_name}_first_touch_view.campaign, {conversion_name}_last_touch_view.campaign) as campaign,
                                            coalesce(first_touch_count, 0) as {conversion_name}_first_touch_count,
                                            coalesce(last_touch_count, 0) as {conversion_name}_last_touch_count,
                                            """
            + (
                f"""coalesce(first_touch_conversion_value, 0) AS {conversion_name}_first_touch_conversion_value,
                                            coalesce(last_touch_conversion_value, 0) AS {conversion_name}_last_touch_conversion_value,"""
                if value_flag
                else ""
            )
            + f"""
                                    from {conversion_name}_first_touch_view 
                                    full outer join {conversion_name}_last_touch_view 
                                    on {conversion_name}_first_touch_view.date = {conversion_name}_last_touch_view.date
                                        AND {conversion_name}_first_touch_view.source = {conversion_name}_last_touch_view.source
                                        AND {conversion_name}_first_touch_view.campaign = {conversion_name}_last_touch_view.campaign)
                                """
        )
        with_query_template = f"""
                                {user_view_query},
                                {first_touch_view_query},
                                {last_touch_view_query},
                                {conversion_view_query}
                                """
        return with_query_template

    def _get_index_cte(self, conversion_name_list):
        select_index_list = list()
        for conversion_name in conversion_name_list:
            query = f"""SELECT date, source, campaign FROM {conversion_name}_conversion_view"""
            select_index_list.append(query)

        select_query = """
                            UNION ALL 
                            """.join(
            select_index_list
        )
        index_cte_query = f"""
                        index_cte AS 
                        (
                            SELECT DISTINCT date, source, campaign
                            FROM (
                            {select_query})
                        )
                        """
        return index_cte_query

    def _get_final_selector_sql(self, conversion_name_list, value_flag_list):
        select_query = f"""
                        SELECT a.date, a.source, a.campaign,"""
        from_query = f"""
                        FROM index_cte a"""
        for conversion_name, value_flag in zip(conversion_name_list, value_flag_list):
            select_query = (
                select_query
                + f"""
                                    coalesce({conversion_name}_first_touch_count, 0) as {conversion_name}_first_touch_count,
                                    coalesce({conversion_name}_last_touch_count, 0) as {conversion_name}_last_touch_count,
                                    """
                + (
                    f"""coalesce({conversion_name}_first_touch_conversion_value, 0) AS {conversion_name}_first_touch_conversion_value,
                                    coalesce({conversion_name}_last_touch_conversion_value, 0) AS {conversion_name}_last_touch_conversion_value,"""
                    if value_flag
                    else ""
                )
            )

            from_query = (
                from_query
                + f"""
                                LEFT JOIN {conversion_name}_conversion_view ON a.date = {conversion_name}_conversion_view.date and a.source = {conversion_name}_conversion_view.source and a.campaign = {conversion_name}_conversion_view.campaign """
            )

        final_selector_sql = (
            select_query
            + from_query
            + f"""
                                            ORDER BY a.date desc"""
        )
        return final_selector_sql

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
        cte_query_list, conversion_name_list, value_flag_list = list(), list(), list()
        for conversion_info in conversions:
            conversion_name = conversion_info["name"]
            value_flag = "value" in conversion_info
            conversion_name_list.append(conversion_name)
            value_flag_list.append(value_flag)

            select_info = (
                f"SELECT {id_column_name}, {conversion_info['timestamp']} AS converted_date, "
                + (
                    f"{conversion_info['value']} AS conversion_value"
                    if value_flag
                    else ""
                )
            )
            from_info = f"FROM {{{{entityVarTable}}}}"
            where_info = f"WHERE {conversion_info['timestamp']} is not NULL"

            conversion_query = f"""
                                {select_info} 
                                {from_info} 
                                {where_info}"""

            with_query_template = self._create_with_query_template(
                conversion_name,
                id_column_name,
                value_flag,
                journey_query,
                conversion_query,
            )
            cte_query_list.append(with_query_template)

        multiconversion_cte_query = """,
                                        """.join(
            cte_query_list
        )

        index_cte_query = self._get_index_cte(conversion_name_list)

        selector_sql = self._get_final_selector_sql(
            conversion_name_list, value_flag_list
        )

        # model_creator_sql
        input_material_template = "this.DeRef('entity/user/var_table')"
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% with entityVarTable = {input_material_template} {set_jouney_ref} %}}
                    WITH {multiconversion_cte_query}
                    
                        , {index_cte_query}
                    
                        {selector_sql}


                    {{% endwith %}}
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this.Name(), selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}
            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""

        self.sql = this.execute_text_template(query_template)
        return

    def execute(self, this: WhtMaterial):
        this.wht_ctx.client.query_sql_without_result(self.sql)
