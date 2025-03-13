
            {% macro begin_block() %}
                {% macro selector_sql() %}
                    {% with entity_var_table = this.DeRef(makePath(user.Var('first_order_date').Model.GetVarTableRef())) campaign_var_table = this.DeRef('entity/campaign/var_table') Table1 = this.DeRef('inputs/rsMarketingPages/var_table') Table2 = this.DeRef('inputs/rsMarketingPages1/var_table')  impressions_source1 = this.DeRef('inputs/ga_campaign_stats/var_table') impressions_source2 = this.DeRef('inputs/lkdn_ad_analytic_campaign/var_table') impressions_source3 = this.DeRef('inputs/fb_basic_campaign/var_table') clicks_source1 = this.DeRef('inputs/ga_campaign_stats/var_table')  %}
                    WITH 
                                
                                date_range AS (
                                        SELECT date
                                        FROM
                                            UNNEST(GENERATE_DATE_ARRAY(DATE('2000-01-01'), CURRENT_DATE())) AS date
                                )
            ,
                                CAMPAIGN_INFO AS (
                                        SELECT campaign_def_id,  DATE(campaign_start_date) as start_date, DATE(campaign_end_date) as end_date 
                                        FROM {{campaign_var_table}}
                                ),
                                index_cte as (
                                        SELECT campaign_def_id, date 
                                        FROM CAMPAIGN_INFO a 
                                            LEFT OUTER JOIN date_range b ON start_date <= date AND date <= end_date 
                                        WHERE campaign_def_id is not NULL
                                )
                                
                        , 
                                
            sub_sf_order_user_view AS
            (
                SELECT DISTINCT
                    journey.user_abc_id AS user_abc_id,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_timestamp,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_timestamp,
                    
                    first_value(campaign_def_id) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_campaign_def_id,
                    first_value(campaign_def_id) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_campaign_def_id,
                    conversion_tbl.converted_date AS converted_date
                FROM (
                    
                                SELECT user_abc_id, {{user.Var('first_order_date').Model.DbObjectNamePrefix()}} AS converted_date 
                                FROM {{entity_var_table}} 
                                WHERE {{user.Var('first_order_date').Model.DbObjectNamePrefix()}} is not NULL
                ) AS conversion_tbl
                JOIN 
                (
                     
                                 
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table1}} a JOIN {{entity_var_table}} b ON a.user_abc_id = b.user_abc_id 
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < {{user.Var('first_invoice_amount')}} ) 
                                UNION ALL  
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table2}} a  
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < first_invoice_amount1 )
                ) AS journey
                ON conversion_tbl.user_abc_id = journey.user_abc_id AND DATE(journey.timestamp) <= conversion_tbl.converted_date
            ), 
            sf_order_user_view AS (
                SELECT 
                    user_abc_id,
                    CASE 
                WHEN DATE_DIFF(converted_date, first_touch_timestamp, day) <= 60 THEN DATE(first_touch_timestamp) 
                ELSE NULL 
            END AS first_touch_date,
first_touch_campaign_def_id,
CASE 
                WHEN DATE_DIFF(converted_date, last_touch_timestamp, day) <= 60 THEN DATE(last_touch_timestamp) 
                ELSE NULL 
            END AS last_touch_date,
last_touch_campaign_def_id,
                    converted_date
                FROM sub_sf_order_user_view
            )
        ,
                                
                                    sf_order_first_touch_view as
                                    (
                                    select first_touch_date as date,
                                        first_touch_campaign_def_id as campaign_def_id, 
                                        count(distinct user_abc_id) as first_touch_count
                                        
                                    from sf_order_user_view 
                                    group by first_touch_date, first_touch_campaign_def_id)
                                    ,
                                
                                    sf_order_last_touch_view as
                                    (
                                    select last_touch_date as date,
                                        last_touch_campaign_def_id as campaign_def_id, 
                                        count(distinct user_abc_id) as last_touch_count
                                        
                                    from sf_order_user_view 
                                    group by last_touch_date, last_touch_campaign_def_id)
                                    ,
                                
                                sf_order_conversion_view AS
                                (
                                    select coalesce(sf_order_first_touch_view.date, sf_order_last_touch_view.date) as date, 
                                            coalesce(sf_order_first_touch_view.campaign_def_id, sf_order_last_touch_view.campaign_def_id) as campaign_def_id, 
                                            coalesce(first_touch_count, 0) as sf_order_first_touch_count,
                                            coalesce(last_touch_count, 0) as sf_order_last_touch_count
                                            
                                    from sf_order_first_touch_view 
                                    full outer join sf_order_last_touch_view 
                                    on sf_order_first_touch_view.date = sf_order_last_touch_view.date
                                        AND sf_order_first_touch_view.campaign_def_id = sf_order_last_touch_view.campaign_def_id)
                                
                                ,
                                        
                                
            sub_sf_order1_user_view AS
            (
                SELECT DISTINCT
                    journey.user_abc_id AS user_abc_id,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_timestamp,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_timestamp,
                    first_value(conversion_value) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_conversion_value,
                    first_value(conversion_value) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_conversion_value,
                    first_value(campaign_def_id) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_campaign_def_id,
                    first_value(campaign_def_id) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_campaign_def_id,
                    conversion_tbl.converted_date AS converted_date
                FROM (
                    
                                SELECT user_abc_id, {{user.Var('first_order_date').Model.DbObjectNamePrefix()}} AS converted_date, {{user.Var('first_invoice_amount').Model.DbObjectNamePrefix()}} AS conversion_value 
                                FROM {{entity_var_table}} 
                                WHERE {{user.Var('first_order_date').Model.DbObjectNamePrefix()}} is not NULL
                ) AS conversion_tbl
                JOIN 
                (
                     
                                 
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table1}} a JOIN {{entity_var_table}} b ON a.user_abc_id = b.user_abc_id 
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < {{user.Var('first_invoice_amount')}} ) 
                                UNION ALL  
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table2}} a  
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < first_invoice_amount1 )
                ) AS journey
                ON conversion_tbl.user_abc_id = journey.user_abc_id AND DATE(journey.timestamp) <= conversion_tbl.converted_date
            ), 
            sf_order1_user_view AS (
                SELECT 
                    user_abc_id,
                    CASE 
                WHEN DATE_DIFF(converted_date, first_touch_timestamp, hour) <= 60 THEN DATE(first_touch_timestamp) 
                ELSE NULL 
            END AS first_touch_date,
first_touch_campaign_def_id,
CASE 
                WHEN DATE_DIFF(converted_date, first_touch_timestamp, hour) <= 60 THEN first_touch_conversion_value 
                ELSE NULL 
            END AS first_touch_conversion_value,
CASE 
                WHEN DATE_DIFF(converted_date, last_touch_timestamp, hour) <= 60 THEN DATE(last_touch_timestamp) 
                ELSE NULL 
            END AS last_touch_date,
last_touch_campaign_def_id,
CASE 
                WHEN DATE_DIFF(converted_date, last_touch_timestamp, hour) <= 60 THEN last_touch_conversion_value 
                ELSE NULL 
            END AS last_touch_conversion_value,
                    converted_date
                FROM sub_sf_order1_user_view
            )
        ,
                                
                                    sf_order1_first_touch_view as
                                    (
                                    select first_touch_date as date,
                                        first_touch_campaign_def_id as campaign_def_id, 
                                        count(distinct user_abc_id) as first_touch_count
                                        , sum(first_touch_conversion_value) as first_touch_conversion_value
                                    from sf_order1_user_view 
                                    group by first_touch_date, first_touch_campaign_def_id)
                                    ,
                                
                                    sf_order1_last_touch_view as
                                    (
                                    select last_touch_date as date,
                                        last_touch_campaign_def_id as campaign_def_id, 
                                        count(distinct user_abc_id) as last_touch_count
                                        , sum(last_touch_conversion_value) as last_touch_conversion_value
                                    from sf_order1_user_view 
                                    group by last_touch_date, last_touch_campaign_def_id)
                                    ,
                                
                                sf_order1_conversion_view AS
                                (
                                    select coalesce(sf_order1_first_touch_view.date, sf_order1_last_touch_view.date) as date, 
                                            coalesce(sf_order1_first_touch_view.campaign_def_id, sf_order1_last_touch_view.campaign_def_id) as campaign_def_id, 
                                            coalesce(first_touch_count, 0) as sf_order1_first_touch_count,
                                            coalesce(last_touch_count, 0) as sf_order1_last_touch_count
                                            , coalesce(first_touch_conversion_value, 0) AS sf_order1_first_touch_conversion_value,
                                            coalesce(last_touch_conversion_value, 0) AS sf_order1_last_touch_conversion_value
                                    from sf_order1_first_touch_view 
                                    full outer join sf_order1_last_touch_view 
                                    on sf_order1_first_touch_view.date = sf_order1_last_touch_view.date
                                        AND sf_order1_first_touch_view.campaign_def_id = sf_order1_last_touch_view.campaign_def_id)
                                
                                ,
                                        
                                
            sub_sf_subscription_user_view AS
            (
                SELECT DISTINCT
                    journey.user_abc_id AS user_abc_id,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_timestamp,
                    first_value(journey.timestamp) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_timestamp,
                    first_value(conversion_value) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_conversion_value,
                    first_value(conversion_value) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_conversion_value,
                    first_value(campaign_def_id) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch_campaign_def_id,
                    first_value(campaign_def_id) OVER (PARTITION BY journey.user_abc_id ORDER BY journey.timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touch_campaign_def_id,
                    conversion_tbl.converted_date AS converted_date
                FROM (
                    
                                SELECT user_abc_id, {{user.Var('subscription_start_date').Model.DbObjectNamePrefix()}} AS converted_date, {{user.Var('first_invoice_amount').Model.DbObjectNamePrefix()}} AS conversion_value 
                                FROM {{entity_var_table}} 
                                WHERE {{user.Var('subscription_start_date').Model.DbObjectNamePrefix()}} is not NULL
                ) AS conversion_tbl
                JOIN 
                (
                     
                                 
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table1}} a JOIN {{entity_var_table}} b ON a.user_abc_id = b.user_abc_id 
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < {{user.Var('first_invoice_amount')}} ) 
                                UNION ALL  
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table2}} a  
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < first_invoice_amount1 )
                ) AS journey
                ON conversion_tbl.user_abc_id = journey.user_abc_id AND DATE(journey.timestamp) <= conversion_tbl.converted_date
            ), 
            sf_subscription_user_view AS (
                SELECT 
                    user_abc_id,
                    DATE(first_touch_timestamp) AS first_touch_date,
first_touch_campaign_def_id,
first_touch_conversion_value AS first_touch_conversion_value,
DATE(last_touch_timestamp) AS last_touch_date,
last_touch_campaign_def_id,
last_touch_conversion_value AS last_touch_conversion_value,
                    converted_date
                FROM sub_sf_subscription_user_view
            )
        ,
                                
                                    sf_subscription_first_touch_view as
                                    (
                                    select first_touch_date as date,
                                        first_touch_campaign_def_id as campaign_def_id, 
                                        count(distinct user_abc_id) as first_touch_count
                                        , sum(first_touch_conversion_value) as first_touch_conversion_value
                                    from sf_subscription_user_view 
                                    group by first_touch_date, first_touch_campaign_def_id)
                                    ,
                                
                                    sf_subscription_last_touch_view as
                                    (
                                    select last_touch_date as date,
                                        last_touch_campaign_def_id as campaign_def_id, 
                                        count(distinct user_abc_id) as last_touch_count
                                        , sum(last_touch_conversion_value) as last_touch_conversion_value
                                    from sf_subscription_user_view 
                                    group by last_touch_date, last_touch_campaign_def_id)
                                    ,
                                
                                sf_subscription_conversion_view AS
                                (
                                    select coalesce(sf_subscription_first_touch_view.date, sf_subscription_last_touch_view.date) as date, 
                                            coalesce(sf_subscription_first_touch_view.campaign_def_id, sf_subscription_last_touch_view.campaign_def_id) as campaign_def_id, 
                                            coalesce(first_touch_count, 0) as sf_subscription_first_touch_count,
                                            coalesce(last_touch_count, 0) as sf_subscription_last_touch_count
                                            , coalesce(first_touch_conversion_value, 0) AS sf_subscription_first_touch_conversion_value,
                                            coalesce(last_touch_conversion_value, 0) AS sf_subscription_last_touch_conversion_value
                                    from sf_subscription_first_touch_view 
                                    full outer join sf_subscription_last_touch_view 
                                    on sf_subscription_first_touch_view.date = sf_subscription_last_touch_view.date
                                        AND sf_subscription_first_touch_view.campaign_def_id = sf_subscription_last_touch_view.campaign_def_id)
                                
                                
                        , journey_views_cte as 
                                (
                                    SELECT date(timestamp) as date, 
                                           campaign_def_id, 
                                           count(distinct user_abc_id) as count_distinct_views, 
                                           count(*) as count_total_views 
                                           from (
                                            
                                 
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table1}} a JOIN {{entity_var_table}} b ON a.user_abc_id = b.user_abc_id 
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < {{user.Var('first_invoice_amount')}} ) 
                                UNION ALL  
                                SELECT a.user_abc_id, a.campaign_def_id, a.timestamp_mock AS timestamp 
                                FROM {{Table2}} a  
                                WHERE (a.campaign_def_id is not NULL) AND ( 4 < first_invoice_amount1 )
                                           ) 
                                           group by date(timestamp), 
                                           campaign_def_id)
                                
                        ,  
                                daily_impressions_cte AS 
                                    (SELECT campaign_def_id, date, SUM(impressions) AS impressions 
                                     FROM (
                                    
                                    SELECT campaign_def_id, CAST(date AS date) AS date, sum(impressions) AS impressions
                                    FROM {{impressions_source1}}
                                    GROUP BY campaign_def_id, date
                                    
                                     UNION ALL 
                                    SELECT campaign_def_id, CAST(day AS date) AS date, sum(impressions) AS impressions
                                    FROM {{impressions_source2}}
                                    GROUP BY campaign_def_id, day
                                    
                                     UNION ALL 
                                    SELECT campaign_def_id, CAST(date AS date) AS date, sum(impressions) AS impressions
                                    FROM {{impressions_source3}}
                                    GROUP BY campaign_def_id, date
                                    )
                                     GROUP BY campaign_def_id, date) ,
                 
                                daily_clicks_cte AS 
                                    (SELECT campaign_def_id, date, SUM(clicks) AS clicks 
                                     FROM (
                                    
                                    SELECT campaign_def_id, CAST(date AS date) AS date, sum(clicks) AS clicks
                                    FROM {{clicks_source1}}
                                    GROUP BY campaign_def_id, date
                                    )
                                     GROUP BY campaign_def_id, date) 
                        
            , user_conversion_days_cte AS (
                SELECT 
                    user_main_id,
                    'sf_order' AS conversion_type,
                    DATE_DIFF(converted_date, first_touch_date, day) AS conversion_days,
                    sf_order_user_view.first_touch_date as date,
                    sf_order_user_view.first_touch_campaign_def_id as campaign_def_id
                FROM sf_order_user_view
                WHERE converted_date IS NOT NULL UNION ALL 
                SELECT 
                    user_main_id,
                    'sf_order1' AS conversion_type,
                    DATE_DIFF(converted_date, first_touch_date, day) AS conversion_days,
                    sf_order1_user_view.first_touch_date as date,
                    sf_order1_user_view.first_touch_campaign_def_id as campaign_def_id
                FROM sf_order1_user_view
                WHERE converted_date IS NOT NULL UNION ALL 
                SELECT 
                    user_main_id,
                    'sf_subscription' AS conversion_type,
                    DATE_DIFF(converted_date, first_touch_date, day) AS conversion_days,
                    sf_subscription_user_view.first_touch_date as date,
                    sf_subscription_user_view.first_touch_campaign_def_id as campaign_def_id
                FROM sf_subscription_user_view
                WHERE converted_date IS NOT NULL), conversion_days_cte AS (
            SELECT date, campaign_def_id
                , SUM(CASE WHEN conversion_type = 'sf_order' THEN conversion_days ELSE 0 END) AS sf_order_total_days_to_convert_from_first_touch_across_users
                , AVG(CASE WHEN conversion_type = 'sf_order' THEN conversion_days ELSE 0 END) AS sf_order_avg_days_to_convert_from_first_touch
                , SUM(CASE WHEN conversion_type = 'sf_order1' THEN conversion_days ELSE 0 END) AS sf_order1_total_days_to_convert_from_first_touch_across_users
                , AVG(CASE WHEN conversion_type = 'sf_order1' THEN conversion_days ELSE 0 END) AS sf_order1_avg_days_to_convert_from_first_touch
                , SUM(CASE WHEN conversion_type = 'sf_subscription' THEN conversion_days ELSE 0 END) AS sf_subscription_total_days_to_convert_from_first_touch_across_users
                , AVG(CASE WHEN conversion_type = 'sf_subscription' THEN conversion_days ELSE 0 END) AS sf_subscription_avg_days_to_convert_from_first_touch
            FROM user_conversion_days_cte
            GROUP BY date, campaign_def_id
        ) 
            SELECT a.date as campaign_date, DATE('2021-01-31') as report_date,a.campaign_def_id
                                    , coalesce(sf_order_first_touch_count, 0) as sf_order_first_touch_count,
                                    coalesce(sf_order_last_touch_count, 0) as sf_order_last_touch_count
                                    
                                    , coalesce(sf_order1_first_touch_count, 0) as sf_order1_first_touch_count,
                                    coalesce(sf_order1_last_touch_count, 0) as sf_order1_last_touch_count
                                    , coalesce(sf_order1_first_touch_conversion_value, 0) AS sf_order1_first_touch_conversion_value,
                                    coalesce(sf_order1_last_touch_conversion_value, 0) AS sf_order1_last_touch_conversion_value
                                    , coalesce(sf_subscription_first_touch_count, 0) as sf_subscription_first_touch_count,
                                    coalesce(sf_subscription_last_touch_count, 0) as sf_subscription_last_touch_count
                                    , coalesce(sf_subscription_first_touch_conversion_value, 0) AS sf_subscription_first_touch_conversion_value,
                                    coalesce(sf_subscription_last_touch_conversion_value, 0) AS sf_subscription_last_touch_conversion_value
                , coalesce(count_distinct_views, 0) as count_distinct_views,
                coalesce(count_total_views, 0) as count_total_views 
                , coalesce(impressions, 0) as impressions 
                , coalesce(clicks, 0) as clicks  , campaign_var_cte.google_ads_utm_campaign as google_ads_utm_campaign, campaign_var_cte.google_ads_clicks as google_ads_clicks 
            , CASE
                WHEN COALESCE(sf_order_conversion_view.sf_order_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.sf_order_total_days_to_convert_from_first_touch_across_users, 0)
              END AS sf_order_total_days_to_convert_from_first_touch_across_users
            , CASE
                WHEN COALESCE(sf_order_conversion_view.sf_order_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.sf_order_avg_days_to_convert_from_first_touch, 0)
              END AS sf_order_avg_days_to_convert_from_first_touch
            , CASE
                WHEN COALESCE(sf_order1_conversion_view.sf_order1_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.sf_order1_total_days_to_convert_from_first_touch_across_users, 0)
              END AS sf_order1_total_days_to_convert_from_first_touch_across_users
            , CASE
                WHEN COALESCE(sf_order1_conversion_view.sf_order1_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.sf_order1_avg_days_to_convert_from_first_touch, 0)
              END AS sf_order1_avg_days_to_convert_from_first_touch
            , CASE
                WHEN COALESCE(sf_subscription_conversion_view.sf_subscription_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.sf_subscription_total_days_to_convert_from_first_touch_across_users, 0)
              END AS sf_subscription_total_days_to_convert_from_first_touch_across_users
            , CASE
                WHEN COALESCE(sf_subscription_conversion_view.sf_subscription_first_touch_count, 0) = 0 THEN NULL
                ELSE COALESCE(conversion_days_cte.sf_subscription_avg_days_to_convert_from_first_touch, 0)
              END AS sf_subscription_avg_days_to_convert_from_first_touch
                        FROM index_cte a
                                LEFT JOIN sf_order_conversion_view ON a.date = sf_order_conversion_view.date and a.campaign_def_id = sf_order_conversion_view.campaign_def_id 
                                LEFT JOIN sf_order1_conversion_view ON a.date = sf_order1_conversion_view.date and a.campaign_def_id = sf_order1_conversion_view.campaign_def_id 
                                LEFT JOIN sf_subscription_conversion_view ON a.date = sf_subscription_conversion_view.date and a.campaign_def_id = sf_subscription_conversion_view.campaign_def_id 
                LEFT JOIN journey_views_cte ON a.date = journey_views_cte.date and a.campaign_def_id = journey_views_cte.campaign_def_id 
                LEFT JOIN daily_impressions_cte ON a.date = daily_impressions_cte.date and a.campaign_def_id = daily_impressions_cte.campaign_def_id 
                LEFT JOIN daily_clicks_cte ON a.date = daily_clicks_cte.date and a.campaign_def_id = daily_clicks_cte.campaign_def_id 
                        LEFT JOIN (SELECT campaign_def_id, google_ads_utm_campaign, google_ads_clicks FROM {{campaign_var_table}}) AS campaign_var_cte ON a.campaign_def_id = campaign_var_cte.campaign_def_id
                        LEFT JOIN conversion_days_cte 
                    ON a.date = conversion_days_cte.date
                    AND a.campaign_def_id = conversion_days_cte.campaign_def_id
        
                    {% endwith %}
                {% endmacro %}
                {% exec %} {{warehouse.CreateReplaceTableAs(this, selector_sql())}} {% endexec %}
            {% endmacro %}
            {% exec %} {{warehouse.BeginEndBlock(begin_block())}} {% endexec %}