
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_593D5A66_242 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-21T07:27:21.078118Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    

        
    

			
    

   CREATE OR REPLACE TABLE MATERIAL_RSTRACKS_VAR_TABLE_1955C7E1_242 AS (
        SELECT 
        left(sha1(random()::varchar),32) AS input_row_id, Material_rsTracks_593d5a66_242.*
                    , COALESCE(NULL, Material_rudder_user_id_stitcher_9f838bb3_242_1.user_main_id, Material_rudder_user_id_stitcher_9f838bb3_242_2.user_main_id) AS user_main_id
                
            
        
        FROM Material_rsTracks_593d5a66_242
                        
                            LEFT JOIN Material_rudder_user_id_stitcher_9f838bb3_242 AS Material_rudder_user_id_stitcher_9f838bb3_242_1
                        
                        
                            ON user_id = Material_rudder_user_id_stitcher_9f838bb3_242_1.other_id
                            AND 'user_id' = Material_rudder_user_id_stitcher_9f838bb3_242_1.other_id_type
                        
                    
                        
                            LEFT JOIN Material_rudder_user_id_stitcher_9f838bb3_242 AS Material_rudder_user_id_stitcher_9f838bb3_242_2
                        
                        
                            ON anonymous_id = Material_rudder_user_id_stitcher_9f838bb3_242_2.other_id
                            AND 'anonymous_id' = Material_rudder_user_id_stitcher_9f838bb3_242_2.other_id_type
                        
                    
                
            );  



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_593D5A66_242;  
            
        
    
        
    

        
    
 
	
	END;  
	