
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_3795DC99_218 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-11-28T16:16:29.448488Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_9BE857A4_218_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (days_since_account_creation) AS days_since_account_creation
                FROM (
            SELECT
                user_main_id,
                 datediff(day, date(min(timestamp)), date('2023-11-28 16:16:29')) 
	
	 AS days_since_account_creation
            FROM Material_user_var_table_cfc56553_218
            
                
                    RIGHT JOIN Material_rsTracks_var_table_88589a5f_218 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_CFC56553_218NEW AS (
              SELECT *
              FROM
              Material_user_var_table_cfc56553_218
              LEFT JOIN MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_9BE857A4_218_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_cfc56553_218;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_CFC56553_218 AS SELECT * FROM Material_user_var_table_cfc56553_218New;  

          
          
          DROP TABLE Material_user_var_table_cfc56553_218New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_9BE857A4_218_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_3795DC99_218;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	