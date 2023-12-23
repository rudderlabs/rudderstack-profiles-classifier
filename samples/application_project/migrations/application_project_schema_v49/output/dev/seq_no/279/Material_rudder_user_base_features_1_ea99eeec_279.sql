
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE VIEW MATERIAL_RUDDER_USER_BASE_FEATURES_1_EA99EEEC_279 AS 
            SELECT user_main_id
                , COALESCE('2023-12-23T18:14:56.856257Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_30b422f7_279.max_timestamp_bw_tracks_pages_1
                    
                
            
            FROM ML_TEST1.Material_user_var_table_30b422f7_279
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	