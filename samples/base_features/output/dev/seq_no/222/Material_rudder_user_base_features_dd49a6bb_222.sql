
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE TABLE MATERIAL_RUDDER_USER_BASE_FEATURES_DD49A6BB_222 AS 
            SELECT user_main_id
                , COALESCE('2023-12-20T10:54:58.899887Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_cfc56553_222.days_since_last_seen
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_222.days_since_account_creation
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_222.is_churned_7_days
                    
                
            
                
                    
                    , Material_user_var_table_cfc56553_222.is_churned_90_days
                    
                
            
            FROM Material_user_var_table_cfc56553_222
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	