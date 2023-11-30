
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE TABLE MATERIAL_RUDDER_USER_BASE_FEATURES_0ABCF2D7_219 AS 
            SELECT user_main_id
                , COALESCE('2023-08-30T00:00:00Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_cfc56553_219.days_since_last_seen
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_219.days_since_account_creation
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_219.is_churned_90_days
                    
                
            
            FROM Material_user_var_table_cfc56553_219
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	