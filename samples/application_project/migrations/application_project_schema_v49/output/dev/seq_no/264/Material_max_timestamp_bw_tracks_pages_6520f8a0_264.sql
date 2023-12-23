
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_13D0F24A_264 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-23T12:11:36.807698Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_6520F8A0_264_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_bw_tracks_pages) AS max_timestamp_bw_tracks_pages
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_bw_tracks_pages
            FROM ML_TEST1.Material_user_var_table_30b422f7_264
            
                
                    RIGHT JOIN ML_TEST1.Material_rsTracks_var_table_4e18ce2d_264 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_264NEW AS (
              SELECT *
              FROM
              ML_TEST1.Material_user_var_table_30b422f7_264
              LEFT JOIN MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_6520F8A0_264_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES USING (user_main_id)
          );  
          

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_264;
          

          
          
   CREATE OR REPLACE TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_264 AS SELECT * FROM ML_TEST1.Material_user_var_table_30b422f7_264New;  

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_264New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_6520F8A0_264_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_13D0F24A_264;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	