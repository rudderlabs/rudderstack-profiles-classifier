
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_IS_CHURNED_90_DAYS_8C6F72AB_218_INTERNAL_EV_IS_CHURNED_90_DAYS AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                case when Material_user_var_table_cfc56553_218.days_since_last_seen > 90 then 1 else 0 end
	
	 AS is_churned_90_days
            FROM Material_user_var_table_cfc56553_218
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_CFC56553_218NEW AS (
              SELECT *
              FROM
              Material_user_var_table_cfc56553_218
              LEFT JOIN MATERIAL_IS_CHURNED_90_DAYS_8C6F72AB_218_INTERNAL_EV_IS_CHURNED_90_DAYS USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_cfc56553_218;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_CFC56553_218 AS SELECT * FROM Material_user_var_table_cfc56553_218New;  

          
          
          DROP TABLE Material_user_var_table_cfc56553_218New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_IS_CHURNED_90_DAYS_8C6F72AB_218_INTERNAL_EV_IS_CHURNED_90_DAYS;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	