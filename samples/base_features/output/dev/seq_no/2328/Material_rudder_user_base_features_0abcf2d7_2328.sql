
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE TABLE MATERIAL_RUDDER_USER_BASE_FEATURES_0ABCF2D7_2328 AS 
            SELECT user_main_id
                , COALESCE('2023-12-05T15:19:58.093905Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_cfc56553_2328.days_since_last_seen
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_2328.days_since_account_creation
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_2328.is_churned_90_days
                    
                
            
            FROM Material_user_var_table_cfc56553_2328
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	