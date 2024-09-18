from typing import List, Dict, Tuple
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
)
from profiles_rudderstack.logger import Logger
import re

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
                                "where": {"type": "string"},
                            },
                            "required": ["from"],
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
                                "conversion_window": {"type": "string"},
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
        return AttributionModelRecipe(self.build_spec, Logger("attribution_model"))

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
    def __init__(self, config: Dict, logger: Logger) -> None:
        self.logger = logger
        self.config = config

    def describe(self, this: WhtMaterial):
        return self.sql, ".sql"

    def _validate_conversion_timestamp_column_type(
        self, this: WhtMaterial, conversion_vars: List[Dict]
    ):
        for conversion_info in conversion_vars:
            timestamp_column = conversion_info["timestamp"]
            query = f"""
                    {{% exec %}}
                        {{% with entity_var_table = {self.input_material_template} %}}
                            SELECT {self.entity_id_column_name}, DATEDIFF(day, {{{{{timestamp_column}}}}}, GETDATE()) AS days_since_conversion
                            FROM {{{{entity_var_table}}}}
                            WHERE {{{{{timestamp_column}}}}} IS NOT NULL;
                        {{% endwith %}}
                    {{% endexec %}}"""
            try:
                this.wht_ctx.client.query_sql_without_result(
                    this.execute_text_template(query)
                )
            except:
                self.logger.error(
                    f"Conversion timestamp column {timestamp_column} should be of type timestamp."
                )
                raise ValueError(
                    f"Conversion timestamp column {timestamp_column} should be of type timestamp."
                )

    def _generate_date_case(
        self, field, conversion_flag, conversion_granularity, conversion_window
    ):
        if conversion_flag:
            return f"""CASE 
                WHEN DATEDIFF({conversion_granularity}, {field}_timestamp, converted_date) <= {conversion_window} THEN DATE({field}_timestamp) 
                ELSE NULL 
            END AS {field}_date"""
        else:
            return f"DATE({field}_timestamp) AS {field}_date"

    def _generate_value_case(
        self,
        field,
        conversion_flag,
        value_flag,
        conversion_granularity,
        conversion_window,
    ):
        if conversion_flag and value_flag:
            return f"""CASE 
                WHEN DATEDIFF({conversion_granularity}, {field}_timestamp, converted_date) <= {conversion_window} THEN {field}_conversion_value 
                ELSE NULL 
            END AS {field}_conversion_value"""
        elif value_flag:
            return f"{field}_conversion_value AS {field}_conversion_value"
        else:
            return ""

    def generate_query_part(
        self,
        campaign_id_column_name,
        conversion_flag,
        value_flag,
        conversion_granularity,
        conversion_window,
    ):
        query_parts = []

        for attribution_type in ["first_touch", "last_touch"]:
            query_parts.append(
                self._generate_date_case(
                    attribution_type,
                    conversion_flag,
                    conversion_granularity,
                    conversion_window,
                )
            )
            query_parts.append(f"{attribution_type}_{campaign_id_column_name}")
            query_parts.append(
                self._generate_value_case(
                    attribution_type,
                    conversion_flag,
                    value_flag,
                    conversion_granularity,
                    conversion_window,
                )
            )

        return ",\n".join(filter(None, query_parts))

    def _create_with_query_template(
        self,
        conversion_name: str,
        entity_id_column_name: str,
        campaign_id_column_name: str,
        value_flag: bool,
        journey_query: str,
        conversion_query: str,
        conversion_window: str = 0,
        conversion_granularity: str = "None",
        conversion_flag: bool = False,
    ):
        user_view_query = (
            f"""
            sub_{conversion_name}_user_view AS
            (
                SELECT DISTINCT
                    journey.{entity_id_column_name} AS {entity_id_column_name},
                    first_value(journey.timestamp) OVER (PARTITION BY journey.{entity_id_column_name} ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_timestamp,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.{entity_id_column_name} ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_timestamp,
                    """
            + (
                f"""first_value(conversion_value) OVER (PARTITION BY journey.{entity_id_column_name} ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_conversion_value,
                    first_value(conversion_value) OVER (PARTITION BY journey.{entity_id_column_name} ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_conversion_value,"""
                if value_flag
                else ""
            )
            + f"""
                    first_value({campaign_id_column_name}) OVER (PARTITION BY journey.{entity_id_column_name} ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_{campaign_id_column_name},
                    first_value({campaign_id_column_name}) OVER (PARTITION BY journey.{entity_id_column_name} ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_{campaign_id_column_name},
                    conversion_tbl.converted_date AS converted_date
                FROM (
                    {conversion_query}
                ) AS conversion_tbl
                JOIN 
                (
                    {journey_query}
                ) AS journey
                ON conversion_tbl.{entity_id_column_name} = journey.{entity_id_column_name} AND journey.timestamp <= conversion_tbl.converted_date
            ), 
            {conversion_name}_user_view AS (
                SELECT 
                    {entity_id_column_name},
                    {self.generate_query_part(campaign_id_column_name, conversion_flag, value_flag, conversion_granularity, conversion_window)},
                    converted_date
                FROM sub_{conversion_name}_user_view
            )
        """
        )
        first_touch_view_query = (
            f"""
                                    {conversion_name}_first_touch_view as
                                    (
                                    select first_touch_date as date,
                                        first_touch_{campaign_id_column_name} as {campaign_id_column_name}, 
                                        count(distinct {entity_id_column_name}) as first_touch_count
                                        """
            + (
                f""", sum(first_touch_conversion_value) as first_touch_conversion_value"""
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
                                        count(distinct {entity_id_column_name}) as last_touch_count
                                        """
            + (
                f""", sum(last_touch_conversion_value) as last_touch_conversion_value"""
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
                                            coalesce(last_touch_count, 0) as {conversion_name}_last_touch_count
                                            """
            + (
                f""", coalesce(first_touch_conversion_value, 0) AS {conversion_name}_first_touch_conversion_value,
                                            coalesce(last_touch_conversion_value, 0) AS {conversion_name}_last_touch_conversion_value"""
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

    def _get_index_cte(
        self, campaign_id_column_name, campaign_start_date, campaign_end_date
    ):
        index_cte_query = f"""
                                RECURSIVE date_range(date) AS (
                                        SELECT DATE '2000-01-01' AS date
                                        UNION ALL
                                        SELECT (date + INTERVAL '1 day')::DATE
                                        FROM date_range
                                        WHERE date < CURRENT_DATE
                                ),
                                CAMPAIGN_INFO AS (
                                        SELECT {campaign_id_column_name},  DATE({campaign_start_date}) as start_date, DATE({campaign_end_date}) as end_date 
                                        FROM {{{{campaign_var_table}}}}
                                ),
                                index_cte as (
                                        SELECT {campaign_id_column_name}, date 
                                        FROM CAMPAIGN_INFO a 
                                            LEFT OUTER JOIN date_range b ON start_date <= date AND date <= end_date 
                                        WHERE {campaign_id_column_name} is not NULL
                                )
                                """
        return index_cte_query

    def _query_cost_calculated_fields(
        self, conversion_name: str, value_flag: bool
    ) -> str:
        """
        Generates the SQL segment for cost-per-conversion and ROAS calculations.
        Only called when self.has_cost is True.
        """
        if self.has_cost:
            cost_fields = f"""
            , coalesce(coalesce(cost, 0) / nullif(coalesce({conversion_name}_first_touch_count, 0), 0),0) AS {conversion_name}_first_touch_cost_per_conv
            , coalesce(coalesce(cost, 0) / nullif(coalesce({conversion_name}_last_touch_count, 0), 0),0) AS {conversion_name}_last_touch_cost_per_conv
            """

            if value_flag:
                cost_fields += f"""
                , coalesce(coalesce({conversion_name}_first_touch_conversion_value, 0) / nullif(coalesce(cost, 0),0), 0) AS {conversion_name}_first_touch_roas
                , coalesce(coalesce({conversion_name}_last_touch_conversion_value, 0) / nullif(coalesce(cost, 0),0), 0) AS {conversion_name}_last_touch_roas
                """

            return cost_fields

        return ""

    def _get_final_selector_sql(
        self,
        campaign_id_column_name,
        conversion_name_list,
        value_flag_list,
        daily_campaign_details_key_behaviours,
        campaign_vars,
        conversion_vars,
        end_time,
    ):
        # Defining User_conversion_days_cte
        user_conversion_days_cte = "user_conversion_days_cte AS ("
        union_all_needed = False
        for conversion_info in conversion_vars:
            table = conversion_info["name"]
            if union_all_needed:
                user_conversion_days_cte += " UNION ALL "
            user_conversion_days_cte += f"""
                SELECT 
                    user_main_id,
                    '{table}' AS conversion_type,
                    DATEDIFF(day, first_touch_date, converted_date) AS conversion_days,
                    {table}_user_view.first_touch_date as date,
                    {table}_user_view.first_touch_{campaign_id_column_name} as {campaign_id_column_name}
                FROM {table}_user_view
                WHERE converted_date IS NOT NULL"""
            union_all_needed = True
        user_conversion_days_cte += ")"
        conversion_days_cte = "conversion_days_cte AS ("
        conversion_days_cte += f"""
            SELECT date, {campaign_id_column_name}"""
        for conversion_info in conversion_vars:
            conversion_type = conversion_info["name"]
            conversion_days_cte += f"""
                , SUM(CASE WHEN conversion_type = '{conversion_type}' THEN conversion_days ELSE 0 END) AS {conversion_type}_total_days_to_convert_from_first_touch_across_users
                , AVG(CASE WHEN conversion_type = '{conversion_type}' THEN conversion_days ELSE 0 END) AS {conversion_type}_avg_days_to_convert_from_first_touch"""
        conversion_days_cte += f"""
            FROM user_conversion_days_cte
            GROUP BY date, {campaign_id_column_name}
        )"""

        # Starting the SELECT query
        select_query = f"""
            , {user_conversion_days_cte}, {conversion_days_cte} 
            SELECT a.date as campaign_date, DATE('{end_time}') as report_date,a.{campaign_id_column_name}"""
        from_query = f"""
                        FROM index_cte a"""
        for conversion_name, value_flag in zip(conversion_name_list, value_flag_list):
            select_query = (
                select_query
                + f"""
                                    , coalesce({conversion_name}_first_touch_count, 0) as {conversion_name}_first_touch_count,
                                    coalesce({conversion_name}_last_touch_count, 0) as {conversion_name}_last_touch_count
                                    """
                + (
                    f""", coalesce({conversion_name}_first_touch_conversion_value, 0) AS {conversion_name}_first_touch_conversion_value,
                                    coalesce({conversion_name}_last_touch_conversion_value, 0) AS {conversion_name}_last_touch_conversion_value"""
                    if value_flag
                    else ""
                )
                + self._query_cost_calculated_fields(conversion_name, value_flag)
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
                , coalesce(count_distinct_views, 0) as count_distinct_views,
                coalesce(count_total_views, 0) as count_total_views """
        )
        from_query = (
            from_query
            + f"""
                LEFT JOIN journey_views_cte ON a.date = journey_views_cte.date and a.{campaign_id_column_name} = journey_views_cte.{campaign_id_column_name} """
        )

        # Adding daily_campaign_details_cte
        for key_behaviour in daily_campaign_details_key_behaviours:
            select_query = (
                select_query
                + f"""
                , coalesce({key_behaviour}, 0) as {key_behaviour} """
            )
            from_query = (
                from_query
                + f"""
                LEFT JOIN daily_{key_behaviour}_cte ON a.date = daily_{key_behaviour}_cte.date and a.{campaign_id_column_name} = daily_{key_behaviour}_cte.{campaign_id_column_name} """
            )

        campaign_vars_cte = f"SELECT {campaign_id_column_name}, {', '.join(campaign_vars)} FROM {{{{campaign_var_table}}}}"
        select_query = (
            select_query
            + f""" , {', '.join([f'campaign_var_cte.{var} as {var}' for var in campaign_vars])} """
        )
        from_query = (
            from_query
            + f"""
                        LEFT JOIN ({campaign_vars_cte}) AS campaign_var_cte ON a.{campaign_id_column_name} = campaign_var_cte.{campaign_id_column_name}
                        LEFT JOIN conversion_days_cte 
                    ON a.date = conversion_days_cte.date
                    AND a.{campaign_id_column_name} = conversion_days_cte.{campaign_id_column_name}
        """
        )

        for conversion_info in conversion_vars:
            conversion_type = conversion_info["name"]
            select_query += f"""
            , CASE
                WHEN COALESCE({conversion_type}_conversion_view.{conversion_type}_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.{conversion_type}_total_days_to_convert_from_first_touch_across_users, 0)
              END AS {conversion_type}_total_days_to_convert_from_first_touch_across_users
            , CASE
                WHEN COALESCE({conversion_type}_conversion_view.{conversion_type}_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.{conversion_type}_avg_days_to_convert_from_first_touch, 0)
              END AS {conversion_type}_avg_days_to_convert_from_first_touch"""

        final_selector_sql = select_query + from_query
        return final_selector_sql

    def _create_daily_campaign_details_cte(
        self, campaign_details, campaign_id_column_name
    ):
        # creating daily campaign details view
        (
            daily_campaign_details_query_list,
            daily_campaign_details_key_behaviours,
            set_daily_campaign_details_ref,
        ) = (list(), list(), "")

        for campaign_detail in campaign_details:
            daily_campaign_details_query_temp, union_op = "", ""
            key_behaviour = next(iter(campaign_detail))
            prefix, counter = f"{key_behaviour}_source", 1

            for campaign_info_granular in campaign_detail[key_behaviour]:
                set_daily_campaign_details_ref = (
                    set_daily_campaign_details_ref
                    + f"{prefix}{counter} = this.DeRef('{campaign_info_granular['from']}/var_table') "
                )
                select_info = f"SELECT {campaign_id_column_name}, {campaign_info_granular['date']} AS date, {campaign_info_granular['select']} AS {key_behaviour}"
                from_info = f"FROM {{{{{prefix}{counter}}}}}"
                group_by_info = f"GROUP BY {campaign_id_column_name}, {campaign_info_granular['date']}"
                daily_campaign_details_query_temp = f"""{daily_campaign_details_query_temp}
                                    {union_op}
                                    {select_info}
                                    {from_info}
                                    {group_by_info}
                                    """
                union_op = " UNION ALL "
                counter += 1

            daily_campaign_details_query_temp = f""" 
                                daily_{key_behaviour}_cte AS 
                                    (SELECT {campaign_id_column_name}, date, SUM({key_behaviour}) AS {key_behaviour} 
                                     FROM ({daily_campaign_details_query_temp})
                                     GROUP BY {campaign_id_column_name}, date) """
            daily_campaign_details_query_list.append(daily_campaign_details_query_temp)
            daily_campaign_details_key_behaviours.append(key_behaviour)

        daily_campaign_details_query = """,
                """.join(
            daily_campaign_details_query_list
        )
        return (
            daily_campaign_details_query,
            daily_campaign_details_key_behaviours,
            set_daily_campaign_details_ref,
        )

    def _create_user_journey_cte(
        self,
        material: WhtMaterial,
        user_journeys,
        entity_id_column_name,
        campaign_id_column_name,
    ):
        # creating user journeys
        journey_query, union_op, set_jouney_ref = "", "", ""
        prefix, counter = "Table", 1
        for journey_info in user_journeys:
            set_jouney_ref = (
                set_jouney_ref
                + f"{prefix}{counter} = this.DeRef('{journey_info['from']}/var_table') "
            )
            journey_info_timestamp = material.de_ref(
                journey_info["from"]
            ).model.time_filtering_column()
            select_info = f"SELECT a.{entity_id_column_name}, a.{campaign_id_column_name}, a.{journey_info_timestamp} AS timestamp"
            from_info = f"FROM {{{{{prefix}{counter}}}}} a"
            where_info = f"WHERE (a.{campaign_id_column_name} is not NULL)"

            if "where" in journey_info:
                if ".Var" in journey_info["where"]:
                    join_info = f"JOIN {{{{entity_var_table}}}} ON a.{entity_id_column_name} = {{{{entity_var_table}}}}.{entity_id_column_name}"
                else:
                    join_info = ""

                from_info += f" {join_info}"
                where_info += f" AND ( {journey_info['where']} )"

            journey_query = f"""{journey_query} 
                                {union_op} 
                                {select_info} 
                                {from_info} 
                                {where_info}"""
            union_op = f"UNION ALL "
            counter += 1
        return journey_query, set_jouney_ref

    def _define_input_dependency(self, user_journeys, campaign_details, campaign_vars):
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

        for var in campaign_vars:
            self.inputs.add(f"entity/{self.campaign_entity}/{var}")

        self.inputs.add(f"entity/{self.campaign_entity}/{self.campaign_start_date}")
        self.inputs.add(f"entity/{self.campaign_entity}/{self.campaign_end_date}")

    def register_dependencies(self, this: WhtMaterial):
        user_journeys = self.config[CONVERSION][TOUCHPOINTS]
        conversion_vars = self.config[CONVERSION][CONVERSION_VARS]
        campaign_details = self.config[CAMPAIGN][CAMPAIGN_DETAILS]
        self.has_cost = False
        for campaign in campaign_details:
            if "cost" in campaign.keys():
                self.has_cost = True
                break

        if not self.has_cost:
            self.logger.warn(
                f"Campaign cost details are missing in {this.model.name()} model. "
                f"So RoAS, CAC will not be calculated."
            )
        campaign_vars = self.config[CAMPAIGN][CAMPAIGN_VARS]

        self.conversion_entity = self.config[ENTITY_KEY]
        self.campaign_entity = self.config[CAMPAIGN][ENTITY_KEY]
        self.campaign_start_date = self.config[CAMPAIGN][CAMPAIGN_START_DATE]
        self.campaign_end_date = self.config[CAMPAIGN][CAMPAIGN_END_DATE]

        entities = this.base_wht_project.entities()
        self.entity_id_column_name = entities[self.conversion_entity]["IdColumnName"]
        self.campaign_id_column_name = entities[self.campaign_entity]["IdColumnName"]

        self.input_material_template = f"this.DeRef(makePath({conversion_vars[0]['timestamp']}.Model.GetVarTableRef()))"
        self.campaign_var_template = (
            f"this.DeRef('entity/{self.campaign_entity}/var_table')"
        )

        self.inputs = set()
        self._define_input_dependency(user_journeys, campaign_details, campaign_vars)

        for dependency in self.inputs:
            this.de_ref(dependency)

        journey_query, set_jouney_ref = self._create_user_journey_cte(
            this,
            user_journeys,
            self.entity_id_column_name,
            self.campaign_id_column_name,
        )
        (
            daily_campaign_details_query,
            daily_campaign_details_key_behaviours,
            set_daily_campaign_details_ref,
        ) = self._create_daily_campaign_details_cte(
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

            from_info = f"FROM {{{{entity_var_table}}}}"
            where_info = f"WHERE {conversion_info_column_name_timestamp} is not NULL"

            conversion_query = f"""
                                {select_info} 
                                {from_info} 
                                {where_info}"""
            if "conversion_window" not in conversion_info:
                with_query_template = self._create_with_query_template(
                    conversion_name,
                    self.entity_id_column_name,
                    self.campaign_id_column_name,
                    value_flag,
                    journey_query,
                    conversion_query,
                )
            else:
                conversion_window = conversion_info["conversion_window"]
                match = re.match(r"^\d+[mhd]$", conversion_window)
                if not match:
                    raise ValueError(
                        "Invalid conversion window format. Use formats like 30m, 2h, 7d."
                    )

                n_conversion_window = int(
                    conversion_window[:-1]
                )  # All characters except the last one (the number part)
                granularity = conversion_window[-1]

                # Convert conversion window to days for SQL comparison
                if granularity == "m":
                    conversion_granularity = "minute"
                elif granularity == "h":
                    conversion_granularity = "hour"
                elif granularity == "d":
                    conversion_granularity = "day"

                with_query_template = self._create_with_query_template(
                    conversion_name,
                    self.entity_id_column_name,
                    self.campaign_id_column_name,
                    value_flag,
                    journey_query,
                    conversion_query,
                    str(n_conversion_window),
                    conversion_granularity,
                    True,
                )
            cte_query_list.append(with_query_template)

        multiconversion_cte_query = """,
                                        """.join(
            cte_query_list
        )

        journey_cte_query = self._get_journey_aggregation_cte(
            journey_query, self.campaign_id_column_name, self.entity_id_column_name
        )

        index_cte_query = self._get_index_cte(
            self.campaign_id_column_name,
            self.campaign_start_date,
            self.campaign_end_date,
        )
        _, end_time = this.wht_ctx.time_info()
        selector_sql = self._get_final_selector_sql(
            self.campaign_id_column_name,
            conversion_name_list,
            value_flag_list,
            daily_campaign_details_key_behaviours,
            campaign_vars,
            conversion_vars,
            end_time,
        )

        query_template = f"""
            {{% macro begin_block() %}}
                {{% macro selector_sql() %}}
                    {{% with entity_var_table = {self.input_material_template} campaign_var_table = {self.campaign_var_template} {set_jouney_ref} {set_daily_campaign_details_ref} %}}
                    WITH {index_cte_query}
                        , {multiconversion_cte_query}
                        , {journey_cte_query}
                        , {daily_campaign_details_query}
                        {selector_sql}
                    {{% endwith %}}
                {{% endmacro %}}
                {{% exec %}} {{{{warehouse.CreateReplaceTableAs(this.Name(), selector_sql())}}}} {{% endexec %}}
            {{% endmacro %}}
            {{% exec %}} {{{{warehouse.BeginEndBlock(begin_block())}}}} {{% endexec %}}"""

        self.sql = this.execute_text_template(query_template)
        return

    def execute(self, this: WhtMaterial):
        self._validate_conversion_timestamp_column_type(
            this, self.config[CONVERSION][CONVERSION_VARS]
        )
        this.wht_ctx.client.query_sql_without_result(self.sql)
