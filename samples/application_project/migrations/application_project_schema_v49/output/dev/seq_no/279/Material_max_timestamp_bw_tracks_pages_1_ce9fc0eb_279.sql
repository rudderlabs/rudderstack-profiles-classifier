
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_13D0F24A_279 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-23T18:14:56.856257Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_1_CE9FC0EB_279_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES_1 AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_bw_tracks_pages_1) AS max_timestamp_bw_tracks_pages_1
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_bw_tracks_pages_1
            FROM ML_TEST1.Material_user_var_table_30b422f7_279
            
                
                    RIGHT JOIN ML_TEST1.Material_rsTracks_var_table_4e18ce2d_279 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_279NEW AS (
              SELECT *
              FROM
              ML_TEST1.Material_user_var_table_30b422f7_279
              LEFT JOIN MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_1_CE9FC0EB_279_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES_1 USING (user_main_id)
          );  
          

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_279;
          

          
          
   CREATE OR REPLACE TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_279 AS SELECT * FROM ML_TEST1.Material_user_var_table_30b422f7_279New;  

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_279New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_MAX_TIMESTAMP_BW_TRACKS_PAGES_1_CE9FC0EB_279_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES_1;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_13D0F24A_279;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	