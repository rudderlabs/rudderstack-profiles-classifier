
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE TABLE MATERIAL_RUDDER_USER_BASE_FEATURES_3576FCBD_243 AS 
            SELECT user_main_id
                , COALESCE('2023-12-21T07:30:21.287135Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_79cc6be9_243.days_since_last_seen
                    
                
            
                
                    
                    , Material_user_var_table_79cc6be9_243.days_since_account_creation
                    
                
            
                
                    
                    , Material_user_var_table_79cc6be9_243.is_churned_7_days
                    
                
            
                
                    
                    , Material_user_var_table_79cc6be9_243.is_churned_90_days
                    
                
            
            FROM Material_user_var_table_79cc6be9_243
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	