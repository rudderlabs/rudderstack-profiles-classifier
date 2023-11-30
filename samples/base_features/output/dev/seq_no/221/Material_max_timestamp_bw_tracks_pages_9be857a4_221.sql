
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_3795DC99_221 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-11-28T21:55:44.765243Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_9BE857A4_221_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_bw_tracks_pages) AS max_timestamp_bw_tracks_pages
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_bw_tracks_pages
            FROM Material_user_var_table_cfc56553_221
            
                
                    RIGHT JOIN Material_rsTracks_var_table_88589a5f_221 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_CFC56553_221NEW AS (
              SELECT *
              FROM
              Material_user_var_table_cfc56553_221
              LEFT JOIN MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_9BE857A4_221_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_cfc56553_221;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_CFC56553_221 AS SELECT * FROM Material_user_var_table_cfc56553_221New;  

          
          
          DROP TABLE Material_user_var_table_cfc56553_221New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_9BE857A4_221_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_3795DC99_221;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	