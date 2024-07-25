from typing import List, Dict, Tuple, Union
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
)
from profiles_rudderstack.logger import Logger
import re
import pandas as pd
from datetime import datetime, timedelta

# build spec constants
CONVERSION = "conversion"
ENTITY_KEY = "entity_key"
TOUCHPOINTS = "touchpoints"
CONVERSION_VARS = "conversion_vars"
CAMPAIGN = "campaign"
CAMPAIGN_START_DATE = "campaign_start_date"
CAMPAIGN_END_DATE = "campaign_end_date"
CAMPAIGN_VARS = "campaign_vars"
CAMPAIGN_DETAILS = "campaign_details"


class AttributionModel(BaseModelType):
    TypeName = "attribution"  # the name of the model type

    # json schema for the build spec
    BuildSpecSchema = {
        "type": "object",
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            CONVERSION: {
                "type": "object",
                "properties": {
                    TOUCHPOINTS: {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "from": {"type": "string"},
                                "timestamp": {"type": "string"},
                            },
                            "required": ["from", "timestamp"],
                            "additionalProperties": False,
                        },
                    },
                    CONVERSION_VARS: {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "timestamp": {"type": "string"},
                                "value": {"type": "string"},
                            },
                            "required": ["name", "timestamp"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": [TOUCHPOINTS, CONVERSION_VARS],
                "additionalProperties": False,
            },
            CAMPAIGN: {
                "type": "object",
                "properties": {
                    ENTITY_KEY: {"type": "string"},
                    CAMPAIGN_START_DATE: {"type": "string"},
                    CAMPAIGN_END_DATE: {"type": "string"},
                    CAMPAIGN_VARS: {"type": "array", "items": {"type": "string"}},
                    CAMPAIGN_DETAILS: {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "patternProperties": {
                                "^.*$": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "from": {"type": "string"},
                                            "date": {"type": "string"},
                                            "select": {"type": "string"},
                                        },
                                        "required": ["from", "date", "select"],
                                        "additionalProperties": False,
                                    },
                                },
                            },
                            "additionalProperties": False,
                        },
                    },
                },
                "required": [
                    ENTITY_KEY,
                    CAMPAIGN_START_DATE,
                    CAMPAIGN_END_DATE,
                    CAMPAIGN_VARS,
                    CAMPAIGN_DETAILS,
                ],
                "additionalProperties": False,
            },
        },
        "required": EntityKeyBuildSpecSchema["required"] + [CONVERSION, CAMPAIGN],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return AttributionModelRecipe(self.build_spec)

    def validate(self) -> Tuple[bool, str]:
        campaign_vars = self.build_spec[CAMPAIGN][CAMPAIGN_VARS]
        campaign_details = self.build_spec[CAMPAIGN][CAMPAIGN_DETAILS]

        campaign_details_keys = [next(iter(obj)) for obj in campaign_details]
        for val in campaign_details_keys:
            if val in campaign_vars:
                return (
                    False,
                    f"campaign_vars and campaign_details have same var: {val}. Please provide unique vars in campaign_vars and campaign_details.",
                )
        return True, "Validated successfully"


class AttributionModelRecipe(PyNativeRecipe):
    def __init__(self, config: Dict) -> None:
        self.logger = Logger("attribution_model")
        self.config = config

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def _create_index_table(
        self,
        this: WhtMaterial,
        campaign_id_column: str,
        campaign_start_dt_col: str,
        campaign_end_dt_col: str,
        campaign_entity_var: str,
    ):
        # Fetch the start date, end date, campaign id from campaign var table
        campaign_var_table = this.de_ref(
            f"entity/{campaign_entity_var}/var_table"
        ).string()
        query = f"select {campaign_id_column}, {campaign_start_dt_col}, {campaign_end_dt_col} from {campaign_var_table};"
        campaign_start_end_dates = this.wht_ctx.client.query_sql_with_result(query)
        campaign_start_end_dates.columns = [
            campaign_id_column,
            campaign_start_dt_col,
            campaign_end_dt_col,
        ]  # To handle case sensitivity

        ## Create a table with all the dates between start and end date
        # Function to generate date range
        def date_range(start, end):
            today = datetime.now().date()
            end = min(end.date(), today)
            for n in range(int((end - start.date()).days) + 1):
                yield start + timedelta(n)

        # Create the expanded DataFrame
        expanded_df = pd.DataFrame(
            [
                (row[campaign_id_column], date)
                for _, row in campaign_start_end_dates.iterrows()
                for date in date_range(
                    row[campaign_start_dt_col], row[campaign_end_dt_col]
                )
            ],
            columns=[campaign_id_column, "date"],
        )
        schema = this.wht_ctx.client.schema
        # Sort the DataFrame
        expanded_df = expanded_df.sort_values([campaign_id_column, "date"])
        this.wht_ctx.client.write_df_to_table(
            expanded_df, self.index_table_name, schema=schema
        )

        if this.wht_ctx.client.is_snowpark_enabled:
            # Snowpark write_df_to_table converts date to int by default. So, we need to convert it back to date
            update_query = f"CREATE OR REPLACE TABLE {schema}.{self.index_table_name} AS SELECT {campaign_id_column}, date(date) as date FROM {schema}.{self.index_table_name};"
            this.wht_ctx.client.query_sql_without_result(update_query)
        # Check that the table is created
        check_query = f"select * from {schema}.{self.index_table_name} limit 5;"
        res = this.wht_ctx.client.query_sql_with_result(check_query)
        # print(f"Checking the table {self.index_table_name} is created. Top 5 rows:{res.head()}")
        _ = res.head()

    def _create_with_query_template(
        self,
        conversion_name: str,
        entity_id_column_name: str,
        campaign_id_column_name: str,
        value_flag: bool,
        journey_query: str,
        conversion_query: str,
    ):
        user_view_query = (
            f"""
                    {conversion_name}_user_view AS
                        (
                        SELECT distinct
                                journey.{entity_id_column_name} as {entity_id_column_name},
                                first_value(DATE(journey.timestamp)) over (partition by journey.{entity_id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_date,
                                first_value(DATE(journey.timestamp)) over (partition by journey.{entity_id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_date,
                                """
            + (
                f"""first_value(conversion_value) over (partition by journey.{entity_id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_conversion_value,
                                first_value(conversion_value) over (partition by journey.{entity_id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_conversion_value,"""
                if value_flag
                else ""
            )
            + f"""
                                first_value({campaign_id_column_name}) over (partition by journey.{entity_id_column_name} order by journey.timestamp asc rows between unbounded preceding and unbounded following) as first_touch_{campaign_id_column_name},
                                first_value({campaign_id_column_name}) over (partition by journey.{entity_id_column_name} order by journey.timestamp desc rows between unbounded preceding and unbounded following) as last_touch_{campaign_id_column_name},
                        FROM (
                            {conversion_query}
                        ) AS conversion_tbl
                        JOIN 
                        (
                            {journey_query}
                        ) AS journey
                        ON conversion_tbl.{entity_id_column_name} = journey.{entity_id_column_name} and journey.timestamp <= conversion_tbl.converted_date)        
                    """
        )
        first_touch_view_query = (
            f"""
                                    {conversion_name}_first_touch_view as
                                    (
                                    select first_touch_date as date,
                                        first_touch_{campaign_id_column_name} as {campaign_id_column_name}, 
                                        count(distinct {entity_id_column_name}) as first_touch_count, 
                                        """
            + (
                f"""sum(first_touch_conversion_value) as first_touch_conversion_value"""
                if value_flag
                else ""
            )
            + f"""
                                    from {conversion_name}_user_view 
                                    group by first_touch_date, first_touch_{campaign_id_column_name})
                                    """
        )
        last_touch_view_query = (
            f"""
                                    {conversion_name}_last_touch_view as
                                    (
                                    select last_touch_date as date,
                                        last_touch_{campaign_id_column_name} as {campaign_id_column_name}, 
                                        count(distinct {entity_id_column_name}) as last_touch_count,
                                        """
            + (
                f"""sum(last_touch_conversion_value) as last_touch_conversion_value"""
                if value_flag
                else ""
            )
            + f"""
                                    from {conversion_name}_user_view 
                                    group by last_touch_date, last_touch_{campaign_id_column_name})
                                    """
        )
        conversion_view_query = (
            f"""
                                {conversion_name}_conversion_view AS
                                (
                                    select coalesce({conversion_name}_first_touch_view.date, {conversion_name}_last_touch_view.date) as date, 
                                            coalesce({conversion_name}_first_touch_view.{campaign_id_column_name}, {conversion_name}_last_touch_view.{campaign_id_column_name}) as {campaign_id_column_name}, 
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
                                        AND {conversion_name}_first_touch_view.{campaign_id_column_name} = {conversion_name}_last_touch_view.{campaign_id_column_name})
                                """
        )
        with_query_template = f"""
                                {user_view_query},
                                {first_touch_view_query},
                                {last_touch_view_query},
                                {conversion_view_query}
                                """
        return with_query_template

    def _get_journey_aggregation_cte(
        self, journey_query, campaign_id_column_name, entity_id_column_name
    ):
        journey_cte_query = f"""journey_views_cte as 
                                (
                                    SELECT date(timestamp) as date, 
                                           {campaign_id_column_name}, 
                                           count(distinct {entity_id_column_name}) as count_distinct_views, 
                                           count(*) as count_total_views 
                                           from (
                                           {journey_query}
                                           ) 
                                           group by date(timestamp), 
                                           {campaign_id_column_name})
                                """
        return journey_cte_query

    def _get_index_cte(self, campaign_id_column_name):
        index_cte_query = f"""
                         index_cte as 
                         (select date, {campaign_id_column_name} from {self.index_table_name})
                          """
        return index_cte_query

    def _get_final_selector_sql(
        self,
        campaign_id_column_name,
        conversion_name_list,
        value_flag_list,
        spend_key_behaviours,
        campaign_vars,
        campaign_var_table,
    ):
        select_query = f"""
                        SELECT a.date, a.{campaign_id_column_name},"""
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
                + f"""coalesce(coalesce(cost, 0) / nullif(coalesce({conversion_name}_first_touch_count, 0), 0),0) AS {conversion_name}_first_touch_cost_per_conv,
                                    coalesce(coalesce(cost, 0) / nullif(coalesce({conversion_name}_last_touch_count, 0), 0),0) AS {conversion_name}_last_touch_cost_per_conv,"""
                + (
                    f"""coalesce(coalesce(cost, 0) / nullif(coalesce({conversion_name}_first_touch_conversion_value, 0), 0),0) AS {conversion_name}_first_touch_roas,
                                        coalesce(coalesce(cost, 0) / nullif(coalesce({conversion_name}_last_touch_conversion_value, 0), 0),0) AS {conversion_name}_last_touch_roas,"""
                    if value_flag
                    else ""
                )
            )

            from_query = (
                from_query
                + f"""
                                LEFT JOIN {conversion_name}_conversion_view ON a.date = {conversion_name}_conversion_view.date and a.{campaign_id_column_name} = {conversion_name}_conversion_view.{campaign_id_column_name} """
            )
        # Adding journey_cte
        select_query = (
            select_query
            + f""" 
                coalesce(count_distinct_views, 0) as count_distinct_views,
                coalesce(count_total_views, 0) as count_total_views, """
        )
        from_query = (
            from_query
            + f""" 
                LEFT JOIN journey_views_cte ON a.date = journey_views_cte.date and a.{campaign_id_column_name} = journey_views_cte.{campaign_id_column_name} """
        )

        # Adding daily_spend_cte
        for key_behaviour in spend_key_behaviours:
            select_query = (
                select_query
                + f""" 
                coalesce({key_behaviour}, 0) as {key_behaviour}, """
            )
            from_query = (
                from_query
                + f""" 
                LEFT JOIN daily_{key_behaviour}_cte ON a.date = daily_{key_behaviour}_cte.date and a.{campaign_id_column_name} = daily_{key_behaviour}_cte.{campaign_id_column_name} """
            )

        campaign_vars_cte = f"SELECT {campaign_id_column_name}, {', '.join(campaign_vars)} FROM {campaign_var_table}"
        select_query = (
            select_query
            + f""" {', '.join([f'campaign_var_cte.{var} as {var}' for var in campaign_vars])}, """
        )
        from_query = (
            from_query
            + f"""
                LEFT JOIN ({campaign_vars_cte}) AS campaign_var_cte ON a.{campaign_id_column_name} = campaign_var_cte.{campaign_id_column_name}"""
        )

        final_selector_sql = select_query + from_query
        return final_selector_sql

    def _create_spend_cte(self, campaign_details, campaign_id_column_name):
        # creating campaign daily spend view
        spend_query_list, spend_key_behaviours, set_spend_ref = list(), list(), ""

        for campaign_detail in campaign_details:
            spend_query_temp, spend_union_op = "", ""
            key_behaviour = next(iter(campaign_detail))
            prefix, counter = f"{key_behaviour}_source", 1

            for spend_info in campaign_detail[key_behaviour]:
                set_spend_ref = (
                    set_spend_ref
                    + f"{prefix}{counter} = this.DeRef('{spend_info['from']}/var_table') "
                )
                select_info = f"SELECT {campaign_id_column_name}, {spend_info['date']} AS date, {spend_info['select']} AS {key_behaviour}"
                from_info = f"FROM {{{{{prefix}{counter}}}}}"
                group_by_info = (
                    f"GROUP BY {campaign_id_column_name}, {spend_info['date']}"
                )
                spend_query_temp = f"""{spend_query_temp}
                                    {spend_union_op}
                                    {select_info}
                                    {from_info}
                                    {group_by_info}
                                    """
                spend_union_op = " UNION ALL "
                counter += 1

            spend_query_temp = f""" 
                                daily_{key_behaviour}_cte AS 
                                    (SELECT {campaign_id_column_name}, date, 
                                            SUM({key_behaviour}) AS {key_behaviour} FROM ({spend_query_temp})
                                            GROUP BY 
                                            {campaign_id_column_name}, date) """
            spend_query_list.append(spend_query_temp)
            spend_key_behaviours.append(key_behaviour)

        spend_query = """,
                """.join(
            spend_query_list
        )
        return spend_query, spend_key_behaviours, set_spend_ref

    def _create_user_journey_cte(
        self, user_journeys, entity_id_column_name, campaign_id_column_name
    ):
        # creating user journeys
        journey_query, union_op, set_jouney_ref = "", "", ""
        prefix, counter = "Table", 1
        for journey_info in user_journeys:
            set_jouney_ref = (
                set_jouney_ref
                + f"{prefix}{counter} = this.DeRef('{journey_info['from']}/var_table') "
            )
            select_info = f"SELECT {entity_id_column_name}, {campaign_id_column_name}, {journey_info['timestamp']} AS timestamp"
            from_info = f"FROM {{{{{prefix}{counter}}}}}"
            where_info = f"WHERE {campaign_id_column_name} is not NULL"

            journey_query = f"""{journey_query} 
                                {union_op} 
                                {select_info} 
                                {from_info} 
                                {where_info}"""
            union_op = f"UNION ALL "
            counter += 1
        return journey_query, set_jouney_ref

    def _define_input_dependency(self, user_journeys, campaign_details):
        self.inputs.add(f"{self.conversion_entity}/all/var_table")
        for obj in user_journeys:
            tbl = obj["from"]
            self.inputs.update(
                (
                    tbl,
                    f"{tbl}/var_table",
                    f"{tbl}/var_table/{self.campaign_id_column_name}",
                    f"{tbl}/var_table/{self.entity_id_column_name}",
                )
            )

        for campaign_detail in campaign_details:
            for _, values in campaign_detail.items():
                for obj in values:
                    tbl = obj["from"]
                    self.inputs.update(
                        (
                            tbl,
                            f"{tbl}/var_table",
                            f"{tbl}/var_table/{self.campaign_id_column_name}",
                        )
                    )

        self.inputs.add(f"entity/{self.campaign_entity}/{self.campaign_start_date}")
        self.inputs.add(f"entity/{self.campaign_entity}/{self.campaign_end_date}")

    def register_dependencies(self, this: WhtMaterial):
        user_journeys = self.config[CONVERSION][TOUCHPOINTS]
        conversion_vars = self.config[CONVERSION][CONVERSION_VARS]
        campaign_details = self.config[CAMPAIGN][CAMPAIGN_DETAILS]
        campaign_vars = self.config[CAMPAIGN][CAMPAIGN_VARS]

        self.conversion_entity = self.config[ENTITY_KEY]
        self.campaign_entity = self.config[CAMPAIGN][ENTITY_KEY]
        self.campaign_start_date = self.config[CAMPAIGN][CAMPAIGN_START_DATE]
        self.campaign_end_date = self.config[CAMPAIGN][CAMPAIGN_END_DATE]
        self.index_table_name = f"{this.name()}_index_table_temp".upper()

        entities = this.base_wht_project.entities()
        self.entity_id_column_name = entities[self.conversion_entity]["IdColumnName"]
        self.campaign_id_column_name = entities[self.campaign_entity]["IdColumnName"]

        self.inputs = set()
        self._define_input_dependency(user_journeys, campaign_details)
        campaign_var_table = this.de_ref(
            f"entity/{self.campaign_entity}/var_table"
        ).string()

        for dependency in self.inputs:
            this.de_ref(dependency)

        journey_query, set_jouney_ref = self._create_user_journey_cte(
            user_journeys, self.entity_id_column_name, self.campaign_id_column_name
        )
        spend_query, spend_key_behaviours, set_spend_ref = self._create_spend_cte(
            campaign_details, self.campaign_id_column_name
        )

        # creating user conversion
        conversion_query = ""
        cte_query_list, conversion_name_list, value_flag_list = list(), list(), list()
        for conversion_info in conversion_vars:
            conversion_name = conversion_info["name"]
            conversion_info_column_name_timestamp = (
                f"{{{{{conversion_info['timestamp']}.Model.DbObjectNamePrefix()}}}}"
            )
            value_flag = "value" in conversion_info
            conversion_name_list.append(conversion_name)
            value_flag_list.append(value_flag)

            select_info = f"SELECT {self.entity_id_column_name}, {conversion_info_column_name_timestamp} AS converted_date"

            if value_flag:
                conversion_info_column_name_value = (
                    f"{{{{{conversion_info['value']}.Model.DbObjectNamePrefix()}}}}"
                )
                select_info += (
                    f", {conversion_info_column_name_value} AS conversion_value"
                )

            from_info = f"FROM {{{{entityVarTable}}}}"
            where_info = f"WHERE {conversion_info_column_name_timestamp} is not NULL"

            conversion_query = f"""
                                {select_info} 
                                {from_info} 
                                {where_info}"""

            with_query_template = self._create_with_query_template(
                conversion_name,
                self.entity_id_column_name,
                self.campaign_id_column_name,
                value_flag,
                journey_query,
                conversion_query,
            )
            cte_query_list.append(with_query_template)

        multiconversion_cte_query = """,
                                        """.join(
            cte_query_list
        )

        journey_cte_query = self._get_journey_aggregation_cte(
            journey_query, self.campaign_id_column_name, self.entity_id_column_name
        )

        index_cte_query = self._get_index_cte(self.campaign_id_column_name)

        selector_sql = self._get_final_selector_sql(
            self.campaign_id_column_name,
            conversion_name_list,
            value_flag_list,
            spend_key_behaviours,
            campaign_vars,
            campaign_var_table,
        )

        input_material_template = f"this.DeRef(makePath({conversion_vars[0]['timestamp']}.Model.GetVarTableRef()))"
        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% with entityVarTable = {input_material_template} {set_jouney_ref} {set_spend_ref} %}}
                    WITH {multiconversion_cte_query}
                        , {journey_cte_query}
                        , {spend_query}
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
        self._create_index_table(
            this,
            self.campaign_id_column_name,
            self.campaign_start_date,
            self.campaign_end_date,
            self.campaign_entity,
        )
        this.wht_ctx.client.query_sql_without_result(self.sql)
        query_drop_index_table = f"drop table if exists {this.wht_ctx.client.schema}.{self.index_table_name};"
        this.wht_ctx.client.query_sql_without_result(query_drop_index_table)
