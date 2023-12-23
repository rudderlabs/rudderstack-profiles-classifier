
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_593D5A66_235 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-21T06:47:32.805669Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    

        
    

			
    

   CREATE OR REPLACE TABLE MATERIAL_RSTRACKS_VAR_TABLE_1955C7E1_235 AS (
        SELECT 
        left(sha1(random()::varchar),32) AS input_row_id, Material_rsTracks_593d5a66_235.*
                    , COALESCE(NULL, Material_rudder_user_id_stitcher_9f838bb3_235_1.user_main_id, Material_rudder_user_id_stitcher_9f838bb3_235_2.user_main_id) AS user_main_id
                
            
        
        FROM Material_rsTracks_593d5a66_235
                        
                            LEFT JOIN Material_rudder_user_id_stitcher_9f838bb3_235 AS Material_rudder_user_id_stitcher_9f838bb3_235_1
                        
                        
                            ON user_id = Material_rudder_user_id_stitcher_9f838bb3_235_1.other_id
                            AND 'user_id' = Material_rudder_user_id_stitcher_9f838bb3_235_1.other_id_type
                        
                    
                        
                            LEFT JOIN Material_rudder_user_id_stitcher_9f838bb3_235 AS Material_rudder_user_id_stitcher_9f838bb3_235_2
                        
                        
                            ON anonymous_id = Material_rudder_user_id_stitcher_9f838bb3_235_2.other_id
                            AND 'anonymous_id' = Material_rudder_user_id_stitcher_9f838bb3_235_2.other_id_type
                        
                    
                
            );  



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_593D5A66_235;  
            
        
    
        
    

        
    
 
	
	END;  
	