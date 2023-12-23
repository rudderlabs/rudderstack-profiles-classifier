
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_13D0F24A_250 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-22T09:17:33.743074Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    

        
    

			


    

   CREATE OR REPLACE TABLE MATERIAL_RSTRACKS_VAR_TABLE_4E18CE2D_250 AS (
        SELECT 
        left(sha1(random()::varchar),32) AS input_row_id, ML_TEST1.Material_rsTracks_13d0f24a_250.*
                    
                        , COALESCE(NULL, TempIdsAlias_user_1.user_main_id, TempIdsAlias_user_2.user_main_id) AS user_main_id
                    
                
            
        
        FROM ML_TEST1.Material_rsTracks_13d0f24a_250
                            LEFT JOIN ML_TEST1.Material_rudder_user_id_stitcher_9cc87c26_250 AS TempIdsAlias_user_1
                            ON user_id = TempIdsAlias_user_1.other_id
                            AND 'user_id' = TempIdsAlias_user_1.other_id_type
                        
                            LEFT JOIN ML_TEST1.Material_rudder_user_id_stitcher_9cc87c26_250 AS TempIdsAlias_user_2
                            ON anonymous_id = TempIdsAlias_user_2.other_id
                            AND 'anonymous_id' = TempIdsAlias_user_2.other_id_type
                        
                    
                
            )
    ;  



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_13D0F24A_250;  
            
        
    
        
    

        
    
 
	
	END;  
	