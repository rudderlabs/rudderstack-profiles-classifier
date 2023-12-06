
	
		
   BEGIN

		
   DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;  
		















    /* Run 1 round of cluster merging as follows
    select distinct new_user_main_id AS user_main_id, node_id into table FROM
        partition mappings by user_main_id (clusters), select min node_user_main_id as new_user_main_id FROM
            partition mappings by node, select min user_main_id as node_user_main_id
    */
    
    
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_ACTIVE_MAPPING AS (
        SELECT DISTINCT
            node_id,
            node_id_type,
            CASE
                WHEN new_user_main_id < user_main_id THEN new_user_main_id
                ELSE user_main_id
                END AS user_main_id,
                CASE
                    WHEN new_user_main_id < user_main_id THEN
                        CASE
                            WHEN new_valid_at is NULL or valid_at is NULL THEN NULL
                                ELSE GREATEST(new_valid_at, valid_at)
                        END
                    ELSE valid_at
                    END AS valid_at,
                CASE
                    WHEN new_user_main_id < user_main_id THEN new_user_main_id_dist + 1
                    ELSE user_main_id_dist
                    END AS user_main_id_dist,
                0 AS stitching_active
        FROM (
            SELECT
                node_id,
                node_id_type,
                user_main_id,
                valid_at,
                user_main_id_dist,
                FIRST_VALUE(new_user_main_id) over(
                    PARTITION BY user_main_id
                    ORDER BY
                        new_user_main_id ASC,
                        new_valid_at ASC,
                        new_user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_user_main_id,
                FIRST_VALUE(new_valid_at) over(
                    PARTITION BY user_main_id
                    ORDER BY
                        new_user_main_id ASC,
                        new_valid_at ASC,
                        new_user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_valid_at,
                FIRST_VALUE(new_user_main_id_dist) over(
                    PARTITION BY user_main_id
                    ORDER BY
                        new_user_main_id ASC,
                        new_valid_at ASC,
                        new_user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_user_main_id_dist
            FROM (
            SELECT
                *,
                FIRST_VALUE(user_main_id) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        user_main_id ASC,
                        valid_at ASC,
                        user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_user_main_id,
                FIRST_VALUE(valid_at) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        user_main_id ASC,
                        valid_at ASC,
                        user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_valid_at,
                FIRST_VALUE(user_main_id_dist) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        user_main_id ASC,
                        valid_at ASC,
                        user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_user_main_id_dist
            FROM
                MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_ACTIVE_MAPPING
            )
        )
    );  

    

    /* Set which clusters are actively stitching */
    
   CREATE OR REPLACE TEMPORARY TABLE MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        user_main_id_dist,
        valid_at,
        MAX(stitching_active) OVER (PARTITION BY user_main_id) AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            user_main_id,
            user_main_id_dist,
            valid_at,
            CASE
                WHEN min_user_main_id_node = max_user_main_id_node THEN 0
                ELSE 1
                END AS stitching_active
        FROM (
            SELECT
                node_id,
                node_id_type,
                user_main_id,
                user_main_id_dist,
                valid_at,
                MIN(user_main_id) OVER (PARTITION BY node_id, node_id_type) AS min_user_main_id_node,
                MAX(user_main_id) OVER (PARTITION BY node_id, node_id_type) AS max_user_main_id_node
            FROM MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_ACTIVE_MAPPING
        )
    )
);  

    /* Insert converged clusters into finished table */
    INSERT INTO MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_FINISHED_MAPPING (
        node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active
        )
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        valid_at,
        user_main_id_dist,
        stitching_active
    FROM MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_ACTIVE_MAPPING
    WHERE stitching_active = 0;

    /* Remove converged clusters from active table */
    DELETE FROM MATERIAL_RUDDER_USER_ID_STITCHER_9E783531_2328_INTERNAL_ACTIVE_MAPPING WHERE stitching_active = 0;



 
	
	END;  
	