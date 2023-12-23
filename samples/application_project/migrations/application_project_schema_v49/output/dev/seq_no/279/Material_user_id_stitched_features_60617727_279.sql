 
   CREATE OR REPLACE VIEW MATERIAL_USER_ID_STITCHED_FEATURES_60617727_279 AS 






















  




    
        
            
    
         WITH dummy_variable as (select 1) 
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    

        
    

			


/* Macros */



/* endMacros */


    
    , distinct_served_ids AS (
        SELECT DISTINCT
        
            other_id
            AS user_id
        
        FROM MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279
        
            WHERE other_id_type = 'user_id'
        
    )

    
        
            
            ,Material_user_var_table_30b422f7_279_ID_MERGED AS (
                
    
    
        
        
                SELECT DISTINCT MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.other_id AS user_id
                
                    ,ML_TEST1.Material_user_var_table_30b422f7_279.days_since_account_creation
                
                    ,ML_TEST1.Material_user_var_table_30b422f7_279.days_since_last_seen
                
                    ,ML_TEST1.Material_user_var_table_30b422f7_279.is_churned_7_days
                
                    ,ML_TEST1.Material_user_var_table_30b422f7_279.is_churned_90_days
                
                    ,ML_TEST1.Material_user_var_table_30b422f7_279.max_timestamp_bw_tracks_pages_1
                
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279
                INNER JOIN ML_TEST1.Material_user_var_table_30b422f7_279
                ON MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.user_main_id = ML_TEST1.Material_user_var_table_30b422f7_279.user_main_id
                WHERE MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.other_id_type = 'user_id'
        
    

            )
            
        
    
        
            
            ,Material_shopify_churn_39d922f5_279_ID_MERGED AS (
                
    
    
        
        
                SELECT DISTINCT MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.other_id AS user_id
                
                    ,ML_TEST1.Material_shopify_churn_39d922f5_279.percentile_churn_score_7_days
                
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279
                INNER JOIN ML_TEST1.Material_shopify_churn_39d922f5_279
                ON MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.user_main_id = ML_TEST1.Material_shopify_churn_39d922f5_279.user_main_id
                WHERE MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.other_id_type = 'user_id'
        
    

            )
            
        
    
        
            
            ,Material_churn_90_days_model_b78d19f2_279_ID_MERGED AS (
                
    
    
        
        
                SELECT DISTINCT MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.other_id AS user_id
                
                    ,ML_TEST1.Material_churn_90_days_model_b78d19f2_279.percentile_churn_score_90_days
                
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279
                INNER JOIN ML_TEST1.Material_churn_90_days_model_b78d19f2_279
                ON MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.user_main_id = ML_TEST1.Material_churn_90_days_model_b78d19f2_279.user_main_id
                WHERE MATERIAL_RUDDER_USER_ID_STITCHER_9CC87C26_279.other_id_type = 'user_id'
        
    

            )
            
        
    

    SELECT
    distinct_served_ids.user_id
    
        
            
            
                , Material_user_var_table_30b422f7_279_ID_MERGED.days_since_account_creation
            
                , Material_user_var_table_30b422f7_279_ID_MERGED.days_since_last_seen
            
                , Material_user_var_table_30b422f7_279_ID_MERGED.is_churned_7_days
            
                , Material_user_var_table_30b422f7_279_ID_MERGED.is_churned_90_days
            
                , Material_user_var_table_30b422f7_279_ID_MERGED.max_timestamp_bw_tracks_pages_1
            
            
        
    
        
            
            
                , Material_shopify_churn_39d922f5_279_ID_MERGED.percentile_churn_score_7_days
            
            
        
    
        
            
            
                , Material_churn_90_days_model_b78d19f2_279_ID_MERGED.percentile_churn_score_90_days
            
            
        
    
    FROM
    distinct_served_ids
    
        
        
            FULL OUTER JOIN Material_user_var_table_30b422f7_279_ID_MERGED
            ON Material_user_var_table_30b422f7_279_ID_MERGED.user_id = distinct_served_ids.user_id
        
        
    
        
        
            FULL OUTER JOIN Material_shopify_churn_39d922f5_279_ID_MERGED
            ON Material_shopify_churn_39d922f5_279_ID_MERGED.user_id = distinct_served_ids.user_id
        
        
    
        
        
            FULL OUTER JOIN Material_churn_90_days_model_b78d19f2_279_ID_MERGED
            ON Material_churn_90_days_model_b78d19f2_279_ID_MERGED.user_id = distinct_served_ids.user_id
        
        
    


			
    
        
            
        
    

			;  