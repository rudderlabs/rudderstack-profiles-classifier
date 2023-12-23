
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_IS_CHURNED_7_DAYS_876B40CE_231_INTERNAL_EV_IS_CHURNED_7_DAYS AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                case when Material_user_var_table_79cc6be9_231.days_since_last_seen > 7 then 1 else 0 end
	
	 AS is_churned_7_days
            FROM Material_user_var_table_79cc6be9_231
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_79CC6BE9_231NEW AS (
              SELECT *
              FROM
              Material_user_var_table_79cc6be9_231
              LEFT JOIN MATERIAL_IS_CHURNED_7_DAYS_876B40CE_231_INTERNAL_EV_IS_CHURNED_7_DAYS USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_79cc6be9_231;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_79CC6BE9_231 AS SELECT * FROM Material_user_var_table_79cc6be9_231New;  

          
          
          DROP TABLE Material_user_var_table_79cc6be9_231New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_IS_CHURNED_7_DAYS_876B40CE_231_INTERNAL_EV_IS_CHURNED_7_DAYS;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	