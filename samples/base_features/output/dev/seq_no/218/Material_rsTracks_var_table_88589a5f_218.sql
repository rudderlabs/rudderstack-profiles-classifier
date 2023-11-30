
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_3795DC99_218 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-11-28T16:16:29.448488Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    

        
    

			
    

   CREATE OR REPLACE TABLE MATERIAL_RSTRACKS_VAR_TABLE_88589A5F_218 AS (
        SELECT 
        left(sha1(random()::varchar),32) AS input_row_id, Material_rsTracks_3795dc99_218.*
                    , COALESCE(NULL, Material_rudder_user_id_stitcher_9e783531_218_1.user_main_id, Material_rudder_user_id_stitcher_9e783531_218_2.user_main_id) AS user_main_id
                
            
        
        FROM Material_rsTracks_3795dc99_218
                        
                            LEFT JOIN Material_rudder_user_id_stitcher_9e783531_218 AS Material_rudder_user_id_stitcher_9e783531_218_1
                        
                        
                            ON user_id = Material_rudder_user_id_stitcher_9e783531_218_1.other_id
                            AND 'user_id' = Material_rudder_user_id_stitcher_9e783531_218_1.other_id_type
                        
                    
                        
                            LEFT JOIN Material_rudder_user_id_stitcher_9e783531_218 AS Material_rudder_user_id_stitcher_9e783531_218_2
                        
                        
                            ON anonymous_id = Material_rudder_user_id_stitcher_9e783531_218_2.other_id
                            AND 'anonymous_id' = Material_rudder_user_id_stitcher_9e783531_218_2.other_id_type
                        
                    
                
            );  



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_3795DC99_218;  
            
        
    
        
    

        
    
 
	
	END;  
	