
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_DAYS_SINCE_LAST_SEEN_0EF5CA13_235_INTERNAL_EV_DAYS_SINCE_LAST_SEEN AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(Material_user_var_table_79cc6be9_235.max_timestamp_bw_tracks_pages), date('2023-12-21 06:47:32')) 
	
	 AS days_since_last_seen
            FROM Material_user_var_table_79cc6be9_235
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_79CC6BE9_235NEW AS (
              SELECT *
              FROM
              Material_user_var_table_79cc6be9_235
              LEFT JOIN MATERIAL_DAYS_SINCE_LAST_SEEN_0EF5CA13_235_INTERNAL_EV_DAYS_SINCE_LAST_SEEN USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_79cc6be9_235;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_79CC6BE9_235 AS SELECT * FROM Material_user_var_table_79cc6be9_235New;  

          
          
          DROP TABLE Material_user_var_table_79cc6be9_235New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_DAYS_SINCE_LAST_SEEN_0EF5CA13_235_INTERNAL_EV_DAYS_SINCE_LAST_SEEN;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	