
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_DAYS_SINCE_LAST_SEEN_97361C40_221_INTERNAL_EV_DAYS_SINCE_LAST_SEEN AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(Material_user_var_table_cfc56553_221.max_timestamp_bw_tracks_pages), date('2023-11-28 21:55:44')) 
	
	 AS days_since_last_seen
            FROM Material_user_var_table_cfc56553_221
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_CFC56553_221NEW AS (
              SELECT *
              FROM
              Material_user_var_table_cfc56553_221
              LEFT JOIN MATERIAL_DAYS_SINCE_LAST_SEEN_97361C40_221_INTERNAL_EV_DAYS_SINCE_LAST_SEEN USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_cfc56553_221;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_CFC56553_221 AS SELECT * FROM Material_user_var_table_cfc56553_221New;  

          
          
          DROP TABLE Material_user_var_table_cfc56553_221New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_DAYS_SINCE_LAST_SEEN_97361C40_221_INTERNAL_EV_DAYS_SINCE_LAST_SEEN;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
    
        
    

        
    
 
	
	END;  
	