
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */












/* endMacros */


    

        /* Create output table */
        
        
   CREATE OR REPLACE TABLE MATERIAL_RUDDER_USER_BASE_FEATURES_1_02117404_239 AS 
            SELECT user_main_id
                , COALESCE('2023-12-21T07:19:44.649541Z',   

  SYSDATE()

)::timestamp AS valid_at
            
                
                    
                    , Material_user_var_table_a4f5aa3d_239.max_timestamp_bw_tracks_pages_1
                    
                
            
            FROM Material_user_var_table_a4f5aa3d_239
        ;  

        
    

			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	