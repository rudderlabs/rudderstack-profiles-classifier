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
CAMPAIGN_INFO = "campaign_info"


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
                        },
                    },
                },
                "required": [TOUCHPOINTS, CONVERSION_VARS],
            },
            CAMPAIGN: {
                "type": "object",
                "properties": {
                    ENTITY_KEY: {"type": "string"},
                    CAMPAIGN_START_DATE: {"type": "string"},
                    CAMPAIGN_END_DATE: {"type": "string"},
                    CAMPAIGN_VARS: {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    ENTITY_KEY,
                    CAMPAIGN_START_DATE,
                    CAMPAIGN_END_DATE,
                    CAMPAIGN_VARS,
                ],
            },
            CAMPAIGN_INFO: {
                "type": "object",
                "properties": {
                    "spend_inputs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "from": {"type": "string"},
                                "date": {"type": "string"},
                                "cost": {"type": "string"},
                            },
                            "required": ["from", "date", "cost"],
                        },
                    },
                },
                "required": ["spend_inputs"],
            },
        },
        "required": EntityKeyBuildSpecSchema["required"]
        + [CONVERSION, CAMPAIGN, CAMPAIGN_INFO],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return AttributionModelRecipe(self.build_spec)

    def validate(self) -> Tuple[bool, str]:
        return True, "Validated successfully"


class AttributionModelRecipe(PyNativeRecipe):
    def __init__(self, config: Dict) -> None:
        self.logger = Logger("attribution_model")
        self.config = config
        self.inputs = [
            f"{self.config[ENTITY_KEY]}/all/var_table",
            f"{self.config[CAMPAIGN][ENTITY_KEY]}/all/var_table",
        ]
        campaign_id_column = f"{self.config[CAMPAIGN][ENTITY_KEY]}_profile_id"  # TODO: need to change it through wht_ctx
        entity_id_column = f"{self.config[ENTITY_KEY]}_main_id"  # TODO: need to change it through wht_ctx
        for obj in self.config[CONVERSION][TOUCHPOINTS]:
            tbl = obj["from"]
            self.inputs.extend(
                [
                    tbl,
                    f"{tbl}/var_table",
                    f"{tbl}/var_table/{campaign_id_column}",
                    f"{tbl}/var_table/{entity_id_column}",
                ]
            )

        for obj in self.config[CAMPAIGN_INFO]["spend_inputs"]:
            tbl = obj["from"]
            self.inputs.extend(
                [tbl, f"{tbl}/var_table", f"{tbl}/var_table/{campaign_id_column}"]
            )
        self.inputs.append(
            f"entity/{self.config[CAMPAIGN][ENTITY_KEY]}/{self.config[CAMPAIGN][CAMPAIGN_START_DATE]}"
        )
        self.inputs.append(
            f"entity/{self.config[CAMPAIGN][ENTITY_KEY]}/{self.config[CAMPAIGN][CAMPAIGN_END_DATE]}"
        )

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
        self, campaign_id_column_name, conversion_name_list, value_flag_list
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
        # Adding journey_cte and daily_spend_cte
        select_query = (
            select_query
            + """ coalesce(count_distinct_views, 0) as count_distinct_views,
                  coalesce(count_total_views, 0) as count_total_views, coalesce(cost, 0) as cost """
        )
        from_query = (
            from_query
            + f""" LEFT JOIN journey_views_cte ON a.date = journey_views_cte.date and a.{campaign_id_column_name} = journey_views_cte.{campaign_id_column_name} 
                   LEFT JOIN daily_spend_cte ON a.date = daily_spend_cte.date and a.{campaign_id_column_name} = daily_spend_cte.{campaign_id_column_name} """
        )
        final_selector_sql = select_query + from_query
        return final_selector_sql

    def _create_spend_cte(self, campaign_info, campaign_id_column_name):
        # creating campaign daily spend view
        spend_query, spend_union_op, set_spend_ref = "", "", ""
        prefix, counter = "adspend_source", 1
        for spend_info in campaign_info["spend_inputs"]:
            set_spend_ref = (
                set_spend_ref
                + f"{prefix}{counter} = this.DeRef('{spend_info['from']}/var_table') "
            )
            select_info = f"SELECT {campaign_id_column_name}, {spend_info['date']} AS date, {spend_info['cost']} AS cost"
            from_info = f"FROM {{{{{prefix}{counter}}}}}"
            spend_query = f"""{spend_query}
                              {spend_union_op}
                              {select_info}
                              {from_info}
                              """
            spend_union_op = " UNION ALL "
            counter += 1
        spend_query = f""" daily_spend_cte as 
                                  (select {campaign_id_column_name}, date, 
                                           sum(cost) as cost from ({spend_query})
                                            group by 
                                            {campaign_id_column_name}, date) """
        return spend_query, set_spend_ref

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

    def register_dependencies(self, this: WhtMaterial):
        for dependency in self.inputs:
            this.de_ref(dependency)

        entity_id_column_name = f"{self.config[ENTITY_KEY]}_main_id"  # TODO: need to change it through wht_ctx
        campaign_id_column_name = f"{self.config[CAMPAIGN][ENTITY_KEY]}_profile_id"  # TODO: need to change it through wht_ctx
        user_journeys = self.config[CONVERSION][TOUCHPOINTS]
        conversions = self.config[CONVERSION][CONVERSION_VARS]
        campaign_info = self.config[CAMPAIGN_INFO]
        self.index_table_name = f"{this.name()}_index_table_temp".upper()

        journey_query, set_jouney_ref = self._create_user_journey_cte(
            user_journeys, entity_id_column_name, campaign_id_column_name
        )
        spend_query, set_spend_ref = self._create_spend_cte(
            campaign_info, campaign_id_column_name
        )

        # creating user conversion
        conversion_query = ""
        cte_query_list, conversion_name_list, value_flag_list = list(), list(), list()
        for conversion_info in conversions:
            conversion_name = conversion_info["name"]
            conversion_info_column_name_timestamp = (
                f"{{{{{conversion_info['timestamp']}.Model.DbObjectNamePrefix()}}}}"
            )
            value_flag = "value" in conversion_info
            conversion_name_list.append(conversion_name)
            value_flag_list.append(value_flag)

            select_info = f"SELECT {entity_id_column_name}, {conversion_info_column_name_timestamp} AS converted_date"

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
                entity_id_column_name,
                campaign_id_column_name,
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
            journey_query, campaign_id_column_name, entity_id_column_name
        )

        index_cte_query = self._get_index_cte(campaign_id_column_name)

        selector_sql = self._get_final_selector_sql(
            campaign_id_column_name, conversion_name_list, value_flag_list
        )

        input_material_template = f"this.DeRef(makePath({self.config[CONVERSION][CONVERSION_VARS][0]['timestamp']}.Model.GetVarTableRef()))"
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
            f"{self.config[CAMPAIGN][ENTITY_KEY]}_profile_id",  # TODO: need to change it through wht_ctx
            self.config[CAMPAIGN][CAMPAIGN_START_DATE],
            self.config[CAMPAIGN][CAMPAIGN_END_DATE],
            self.config[CAMPAIGN][ENTITY_KEY],
        )
        this.wht_ctx.client.query_sql_without_result(self.sql)
        query_drop_index_table = f"drop table if exists {this.wht_ctx.client.schema}.{self.index_table_name};"
        this.wht_ctx.client.query_sql_without_result(query_drop_index_table)
