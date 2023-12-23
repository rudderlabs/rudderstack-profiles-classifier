
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  






















  




    
        
            
    
        
    
        
            
                
                
   CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_13D0F24A_246 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-12-21T07:57:59.127878Z'
        )
         OR timestamp IS NULL )
    

;  
                
            
        
    
        
    
        
    
        
    

        
    

			



    
        
        

        
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_A9A73771_246_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (days_since_account_creation) AS days_since_account_creation
                FROM (
            SELECT
                user_main_id,
                 datediff(day, date(min(timestamp)), date('2023-12-21 07:57:59')) 
	
	 AS days_since_account_creation
            FROM ML_TEST1.Material_user_var_table_30b422f7_246
            
                
                    RIGHT JOIN ML_TEST1.Material_rsTracks_var_table_4e18ce2d_246 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );  

        
        
  
      
          
          
          
          /* Join with single entityvar table and replace old table */
          
   CREATE OR REPLACE TEMPORARY TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_246NEW AS (
              SELECT *
              FROM
              ML_TEST1.Material_user_var_table_30b422f7_246
              LEFT JOIN MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_A9A73771_246_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION USING (user_main_id)
          );  
          

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_246;
          

          
          
   CREATE OR REPLACE TABLE ML_TEST1.MATERIAL_USER_VAR_TABLE_30B422F7_246 AS SELECT * FROM ML_TEST1.Material_user_var_table_30b422f7_246New;  

          
          
          DROP TABLE ML_TEST1.Material_user_var_table_30b422f7_246New;
          
      
  


        
   DROP TABLE IF EXISTS MATERIAL_DAYS_SINCE_ACCOUNT_CREATION_A9A73771_246_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION;  

        /* Handle default setting */
        
    



			
    
        
            
    
        
    
        
            
                
   DROP VIEW IF EXISTS MATERIAL_RSTRACKS_13D0F24A_246;  
            
        
    
        
    
        
    
        
    

        
    
 
	
	END;  
	