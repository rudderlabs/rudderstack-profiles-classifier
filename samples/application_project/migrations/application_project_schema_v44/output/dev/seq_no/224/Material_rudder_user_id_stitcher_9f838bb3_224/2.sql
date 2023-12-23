
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  
		










   CREATE OR REPLACE TABLE MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_224_INTERNAL_MAPPING AS SELECT node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_224_INTERNAL_FINISHED_MAPPING UNION ALL SELECT node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_224_INTERNAL_ACTIVE_MAPPING;  


   CREATE OR REPLACE VIEW MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_224 AS 
    WITH ranked_mappings AS (
        WITH ranked_mappings_with_occurance_count AS (
            WITH temp_ranked_mapping AS (
                SELECT
                    user_main_id,
                    node_id AS other_id,
                    node_id_type AS other_id_type,
                    valid_at,
                    CASE
                        WHEN valid_at IS NULL THEN 1
                        ELSE 0
                    END AS ts_mapping,
                    row_number() OVER (
                        PARTITION BY node_id, node_id_type, user_main_id
                        ORDER BY valid_at ASC
                    ) AS row_number
                FROM MATERIAL_RUDDER_USER_ID_STITCHER_9F838BB3_224_INTERNAL_MAPPING
                ORDER BY ts_mapping ASC
                )
            SELECT
                user_main_id,
                other_id,
                other_id_type,
                valid_at,
                ROW_NUMBER() OVER(PARTITION BY other_id, other_id_type, user_main_id ORDER BY ts_mapping ASC, valid_at ASC) AS occurance_count
            FROM temp_ranked_mapping
            WHERE row_number = 1
            ORDER BY occurance_count
            )
        SELECT
            user_main_id,
            other_id,
            other_id_type,
            valid_at,
            occurance_count
        FROM ranked_mappings_with_occurance_count
        WHERE occurance_count = 1
        )
    SELECT
        user_main_id,
        other_id,
        other_id_type,
        valid_at
    FROM ranked_mappings

;  
 
	
	END;  
	