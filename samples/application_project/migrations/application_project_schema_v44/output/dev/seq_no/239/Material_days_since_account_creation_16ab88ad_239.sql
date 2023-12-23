
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_593D5A66_239 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-21T07:19:44.649541Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_16AB88AD_239_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (days_since_account_creation) AS days_since_account_creation
                FROM (
            SELECT
                user_main_id,
                 datediff(day, date(min(timestamp)), date('2023-12-21 07:19:44')) 
	
	 AS days_since_account_creation
            FROM Material_user_var_table_79cc6be9_239
            
                
                    RIGHT JOIN Material_rsTracks_var_table_1955c7e1_239 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_USER_VAR_TABLE_79CC6BE9_239NEW AS (
              SELECT *
              FROM
              Material_user_var_table_79cc6be9_239
              LEFT JOIN MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_16AB88AD_239_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION USING (user_main_id)
          );  
          

          
          
          DROP TABLE Material_user_var_table_79cc6be9_239;
          

          
          
   CREATE OR REPLACE TABLE MATERIAL_USER_VAR_TABLE_79CC6BE9_239 AS SELECT * FROM Material_user_var_table_79cc6be9_239New;  

          
          
          DROP TABLE Material_user_var_table_79cc6be9_239New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_16AB88AD_239_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_593D5A66_239;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	