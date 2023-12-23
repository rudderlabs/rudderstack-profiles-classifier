 
   CREATE OR REPLACE VIEW MATERIAL_MAIN_ID_STITCHED_FEATURES_BB95D0F6_241 AS 






















  




    
        
            
    
         WITH dummy_variable as (select 1) 
        
        
    
        
        
        
    
        
        
        
    
        
        
        
    

        
    

			


/* Macros */



/* endMacros */


    
    , distinct_served_ids AS (
        SELECT DISTINCT
        
            other_id
            AS main_id
        
        FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241
        
            WHERE other_id_type = 'main_id'
        
    )

    
        
            
            ,Material_rudder_user_base_features_3576fcbd_241_ID_MERGED AS (
                
    
    
        
                SELECT DISTINCT MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.other_id AS main_id
                
                    ,Material_rudder_user_base_features_3576fcbd_241.days_since_last_seen
                
                    ,Material_rudder_user_base_features_3576fcbd_241.days_since_account_creation
                
                    ,Material_rudder_user_base_features_3576fcbd_241.is_churned_7_days
                
                    ,Material_rudder_user_base_features_3576fcbd_241.is_churned_90_days
                
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241
                INNER JOIN Material_rudder_user_base_features_3576fcbd_241
                ON MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.user_main_id = Material_rudder_user_base_features_3576fcbd_241.user_main_id
                WHERE MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.other_id_type = 'main_id'
        
    
    

            )
            
        
    
        
            
            ,Material_rudder_user_base_features_1_02117404_241_ID_MERGED AS (
                
    
    
        
                SELECT DISTINCT MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.other_id AS main_id
                
                    ,Material_rudder_user_base_features_1_02117404_241.max_timestamp_bw_tracks_pages_1
                
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241
                INNER JOIN Material_rudder_user_base_features_1_02117404_241
                ON MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.user_main_id = Material_rudder_user_base_features_1_02117404_241.user_main_id
                WHERE MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.other_id_type = 'main_id'
        
    
    

            )
            
        
    
        
            
            ,Material_shopify_churn_30a1d390_241_ID_MERGED AS (
                
    
    
        
                SELECT DISTINCT MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.other_id AS main_id
                
                    ,Material_shopify_churn_30a1d390_241.percentile_churn_score_7_days
                
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241
                INNER JOIN Material_shopify_churn_30a1d390_241
                ON MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.user_main_id = Material_shopify_churn_30a1d390_241.user_main_id
                WHERE MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_241.other_id_type = 'main_id'
        
    
    

            )
            
        
    

    SELECT
    distinct_served_ids.main_id
    
        
            
            
                , Material_rudder_user_base_features_3576fcbd_241_ID_MERGED.days_since_last_seen
            
                , Material_rudder_user_base_features_3576fcbd_241_ID_MERGED.days_since_account_creation
            
                , Material_rudder_user_base_features_3576fcbd_241_ID_MERGED.is_churned_7_days
            
                , Material_rudder_user_base_features_3576fcbd_241_ID_MERGED.is_churned_90_days
            
            
        
    
        
            
            
                , Material_rudder_user_base_features_1_02117404_241_ID_MERGED.max_timestamp_bw_tracks_pages_1
            
            
        
    
        
            
            
                , Material_shopify_churn_30a1d390_241_ID_MERGED.percentile_churn_score_7_days
            
            
        
    
    FROM
    distinct_served_ids
    
        
        
            FULL OUTER JOIN Material_rudder_user_base_features_3576fcbd_241_ID_MERGED
            ON Material_rudder_user_base_features_3576fcbd_241_ID_MERGED.main_id = distinct_served_ids.main_id
        
        
    
        
        
            FULL OUTER JOIN Material_rudder_user_base_features_1_02117404_241_ID_MERGED
            ON Material_rudder_user_base_features_1_02117404_241_ID_MERGED.main_id = distinct_served_ids.main_id
        
        
    
        
        
            FULL OUTER JOIN Material_shopify_churn_30a1d390_241_ID_MERGED
            ON Material_shopify_churn_30a1d390_241_ID_MERGED.main_id = distinct_served_ids.main_id
        
        
    


			
    
        
            
        
    

			;  