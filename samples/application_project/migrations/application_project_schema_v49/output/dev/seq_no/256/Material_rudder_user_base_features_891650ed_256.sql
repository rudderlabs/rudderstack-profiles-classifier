
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE VIEW MATERIAL_RUDDER_USER_BASE_FEATURES_891650ED_256 AS 
            SELECT user_main_id
                , COALESCE('2023-12-22T14:21:56.693214Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_30b422f7_256.days_since_last_seen
                    
                
            
                
                    
                    , Material_user_var_table_30b422f7_256.days_since_account_creation
                    
                
            
                
                    
                    , Material_user_var_table_30b422f7_256.is_churned_7_days
                    
                
            
                
                    
                    , Material_user_var_table_30b422f7_256.is_churned_90_days
                    
                
            
            FROM ML_TEST1.Material_user_var_table_30b422f7_256
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	