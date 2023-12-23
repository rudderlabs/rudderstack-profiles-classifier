
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_593D5A66_228 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-20T11:13:29.548273Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_1_5106D588_228_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES_1 AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_bw_tracks_pages_1) AS max_timestamp_bw_tracks_pages_1
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_bw_tracks_pages_1
            FROM Material_user_var_table_a4f5aa3d_228
            
                
                    RIGHT JOIN Material_rsTracks_var_table_1955c7e1_228 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_A4F5AA3D_228NEW AS (
              SELECT *
              FROM
              Material_user_var_table_a4f5aa3d_228
              LEFT JOIN MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_1_5106D588_228_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES_1 USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_a4f5aa3d_228;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_A4F5AA3D_228 AS SELECT * FROM Material_user_var_table_a4f5aa3d_228New;  

          
          
          DROP TABLE Material_user_var_table_a4f5aa3d_228New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_1_5106D588_228_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES_1;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_593D5A66_228;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	