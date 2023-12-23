
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_IS_CHURNED_90_DAYS_AD23AB0A_268_INTERNAL_EV_IS_CHURNED_90_DAYS AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                case when Material_user_var_table_30b422f7_268.days_since_last_seen > 90 then 1 else 0 end
	
	 AS is_churned_90_days
            FROM ML_TEST1.Material_user_var_table_30b422f7_268
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_268NEW AS (
              SELECT *
              FROM
              ML_TEST1.Material_user_var_table_30b422f7_268
              LEFT JOIN MATERIAL_IS_CHURNED_90_DAYS_AD23AB0A_268_INTERNAL_EV_IS_CHURNED_90_DAYS USING (user_main_id)
          );  
          

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_268;
          

          
          
   CREATE OR REPLACE TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_268 AS SELECT * FROM ML_TEST1.Material_user_var_table_30b422f7_268New;  

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_268New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_IS_CHURNED_90_DAYS_AD23AB0A_268_INTERNAL_EV_IS_CHURNED_90_DAYS;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	