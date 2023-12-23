
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_DAYS_SINCE_LAST_SEEN_AB8DF419_274_INTERNAL_EV_DAYS_SINCE_LAST_SEEN AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(Material_user_var_table_30b422f7_274.max_timestamp_bw_tracks_pages), date('2023-12-23 13:07:04')) 
	
	 AS days_since_last_seen
            FROM ML_TEST1.Material_user_var_table_30b422f7_274
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_274NEW AS (
              SELECT *
              FROM
              ML_TEST1.Material_user_var_table_30b422f7_274
              LEFT JOIN MATERIAL_DAYS_SINCE_LAST_SEEN_AB8DF419_274_INTERNAL_EV_DAYS_SINCE_LAST_SEEN USING (user_main_id)
          );  
          

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_274;
          

          
          
   CREATE OR REPLACE TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_274 AS SELECT * FROM ML_TEST1.Material_user_var_table_30b422f7_274New;  

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_274New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_DAYS_SINCE_LAST_SEEN_AB8DF419_274_INTERNAL_EV_DAYS_SINCE_LAST_SEEN;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	