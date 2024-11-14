import logging
import re
import sys
import os
import warnings
import subprocess
from typing import Dict
import pandas as pd

from .io_handler import IOHandler
from .database_manager import DatabaseManager
from .file_generator import YamlGenerator
from . import messages
from .config import (
    SAMPLE_DATA_DIR,
    TABLE_SUFFIX,
    ID_GRAPH_MODEL_SUFFIX,
)

from profiles_rudderstack.reader import Reader
from profiles_rudderstack.client.client_base import BaseClient

warnings.filterwarnings("ignore", category=UserWarning, module="snowflake.connector")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
logging.getLogger("snowflake.connector.network").setLevel(logging.ERROR)


class ProfileBuilder:
    def __init__(self, reader: Reader, fast_mode: bool):
        self.fast_mode = fast_mode
        self.io = IOHandler(reader, fast_mode)
        self.yaml_generator = YamlGenerator(fast_mode)

    def run(self, client: BaseClient):
        self.db_manager = DatabaseManager(client, self.io, self.fast_mode)

        self.display_welcome_message()
        new_table_names = self.upload_sample_data()
        relevant_tables = self.find_relevant_tables(new_table_names)

        conn_name, target = client.get_connection_and_target()
        self.display_about_project_files(conn_name)
        # pb_project.yaml
        entity_name, id_types, id_graph_model = self.generate_pb_project(conn_name)
        # inputs.yaml
        self.map_tables_to_id_types(relevant_tables, id_types, entity_name)
        # profiles.yaml
        self.build_id_stitcher_model(relevant_tables, entity_name, id_graph_model)

        self.pb_runs(entity_name, id_graph_model, target)

    def display_welcome_message(self):
        self.io.display_message(messages.WELCOME_MESSAGE)
        self.io.display_multiline_message(
            "Please read through the text in detail and press Enter to continue to each next step. Press “Enter” now"
        )
        self.io.display_multiline_message(messages.FICTIONAL_BUSINESS_OVERVIEW)

    def upload_sample_data(self):
        self.io.display_message(
            "Now, let's seed your warehouse with the sample data from Secure Solutions, LLC"
        )
        self.io.display_message(
            f"We will add a suffix `{TABLE_SUFFIX}` to the table names to avoid conflicts and not overwrite any existing tables."
        )
        return self.db_manager.upload_sample_data(SAMPLE_DATA_DIR, TABLE_SUFFIX)

    def find_relevant_tables(self, new_table_names: Dict[str, str]):
        self.io.display_message(
            f"\n** Searching the `{self.db_manager.schema}` schema in `{self.db_manager.db}` database for uploaded sample tables, that will act as sources for profiles **\n"
        )
        try:
            relevant_tables = self.db_manager.find_relevant_tables(new_table_names)
            if not relevant_tables:
                logger.error(
                    "No relevant tables found. Please check your inputs and try again."
                )
                sys.exit(1)
            self.io.display_message(
                f"Found {len(relevant_tables)} relevant tables: {relevant_tables}"
            )
            return relevant_tables
        except Exception as e:
            logger.exception(f"An error occurred while fetching tables: {e}")
            sys.exit(1)

    def display_about_project_files(self, connection_name: str):
        self.io.display_multiline_message(
            messages.ABOUT_PROFILES_FILES(connection_name)
        )

    def generate_pb_project(self, connection_name: str):
        # create a directory called profiles if doesn't exist
        if not os.path.exists("profiles"):
            os.makedirs("profiles")
        if not os.path.exists("profiles/models"):
            os.makedirs("profiles/models")

        entity_name = self.get_entity_name()
        id_graph_model = f"{entity_name}_{ID_GRAPH_MODEL_SUFFIX}"
        id_types = self.get_id_types(entity_name)
        self.yaml_generator.create_pb_project(
            entity_name, id_types, connection_name, id_graph_model
        )
        self.io.display_message(
            "We have updated the pb_project.yaml file with this info. Please check it out."
        )
        return entity_name, id_types, id_graph_model

    def pb_runs(self, entity_name: str, id_graph_model: str, target: str):
        self.io.display_multiline_message(messages.ABOUT_PB_COMPILE)
        self.io.get_user_input("Enter `pb compile`", options=["pb compile"])
        os.chdir("profiles")
        pb_compile_output = self._subprocess_run(["pb", "compile", "--target", target])
        os.chdir("..")
        _ = self.explain_pb_compile_results(pb_compile_output, id_graph_model)

        self.io.display_multiline_message(messages.ABOUT_PB_RUN)
        seq_no, id_stitcher_table_name = self.prompt_to_do_pb_run(
            id_graph_model, target
        )
        distinct_main_ids = self.explain_pb_run_results(
            entity_name, id_stitcher_table_name
        )
        seq_no, updated_id_stitcher_table_name = self.second_run(
            distinct_main_ids,
            entity_name,
            id_graph_model,
            id_stitcher_table_name,
            target,
        )
        self.third_run(
            entity_name,
            id_graph_model,
            id_stitcher_table_name,
            updated_id_stitcher_table_name,
            seq_no,
            target,
        )

    def get_entity_name(self):
        self.io.display_message(messages.ABOUT_ENTITY)
        return self.io.get_user_input(
            "For the purpose of this tutorial, we will define a 'user' entity. Please enter `user`",
            default="user",
            options=["user"],
        )

    def get_id_types(self, entity_name):
        self.io.display_multiline_message(messages.ABOUT_ID_TYPES)
        id_types = self.io.guide_id_type_input(entity_name)
        self.io.display_message(messages.ABOUT_ID_TYPES_CONCLUSSION)
        return id_types

    def map_tables_to_id_types(self, relevant_tables, id_types, entity_name):
        self.io.display_multiline_message(messages.ABOUT_INPUTS)
        id_mappings = {}
        table_index = 0
        while table_index < len(relevant_tables):
            table = relevant_tables[table_index]
            table_mappings, action = self.db_manager.map_columns_to_id_types(
                table, id_types, entity_name
            )
            if action == "back":
                if table_index > 0:
                    table_index -= 1
                    if relevant_tables[table_index] in id_mappings:
                        _ = id_mappings.pop(relevant_tables[table_index])
                else:
                    self.io.display_message("You are already at the first table.")
            else:
                if table_mappings:
                    id_mappings[table] = {
                        "mappings": table_mappings,
                        "full_table_name": f"{self.db_manager.get_qualified_name(table)}",
                    }
                table_index += 1
        self.yaml_generator.create_inputs_yaml(id_mappings)
        self.io.display_message(
            "Perfect! You can now examine your inputs.yaml file and see the input definitions."
        )

    def build_id_stitcher_model(self, table_names, entity_name, id_graph_model):
        self.io.display_multiline_message(messages.ABOUT_ID_STITCHER_1)
        id_graph_model_name = self.io.get_user_input(
            f"Enter a name for the model, Let's give the name `{id_graph_model}`",
            options=[id_graph_model],
            default=id_graph_model,
        )
        self.io.display_multiline_message(messages.ABOUT_ID_STITCHER_2)
        edge_sources = []
        for table in table_names:
            table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
            edge_source = self.io.get_user_input(
                f"Enter `inputs/{table_name}` as an edge source",
                options=[f"inputs/{table_name}"],
                default=f"inputs/{table_name}",
            )
            edge_sources.append(edge_source)
        self.yaml_generator.create_profiles_yaml(
            entity_name, edge_sources, id_graph_model_name
        )
        self.io.display_message(
            "Perfect! You can now examine your profiles.yaml file and see the model spec for the ID Stitcher."
        )

    def _subprocess_run(self, args):
        res = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        assert res.returncode == 0, f"Command {args} failed with error: {res.stderr}"
        return res.stdout

    def parse_pb_output_text(self, pb_output_text: str, id_graph_name: str):
        seq_no_pattern = r"--seq_no (\d+)"
        match = re.search(seq_no_pattern, pb_output_text)
        assert match, f"Failed to find seq_no in the pb  output {pb_output_text}"
        seq_no = match.group(1)
        pattern = rf"(Material_{id_graph_name}_[a-f0-9]+_\d+)"
        match = re.search(pattern, pb_output_text)
        assert (
            match
        ), f"Failed to find id_graph_table_name in the pb  output {pb_output_text}"
        id_graph_table_name = match.group(1)
        return int(seq_no), id_graph_table_name

    def prompt_to_do_pb_run(
        self, id_graph_name: str, target: str, command: str = "pb run"
    ):
        self.io.get_user_input(f"Enter `{command}` to continue", options=[command])
        self.io.display_message(
            "Running the profiles project...(This will take a few minutes)"
        )
        os.chdir("profiles")
        pb_run_output = self._subprocess_run([*command.split(), "--target", target])
        os.chdir("..")
        seq_no, id_graph_table_name = self.parse_pb_output_text(
            pb_run_output, id_graph_name
        )
        self.io.display_message("Done")
        return seq_no, id_graph_table_name

    def explain_pb_compile_results(self, pb_compile_output, id_graph_model):
        seq_no, _ = self.parse_pb_output_text(pb_compile_output, id_graph_model)
        self.io.display_multiline_message(messages.EXPLAIN_PB_COMPILE_RESULTS(seq_no))
        return seq_no

    def explain_pb_run_results(self, entity_name: str, id_stitcher_table_name: str):
        self.io.display_multiline_message(
            messages.EXPLAIN_PB_RUN_RESULTS(
                entity_name,
                id_stitcher_table_name,
                self.db_manager.schema,
                self.db_manager.db,
            )
        )
        distinct_ids = self._explain_first_run(entity_name, id_stitcher_table_name)
        return distinct_ids

    def id_stitcher_queries(self, entity_name: str, id_stitcher_table_name: str) -> int:
        query_distinct_main_ids = f"select count(distinct {entity_name}_main_id) from {self.db_manager.get_qualified_name(id_stitcher_table_name)}"
        # logger.info("You can check the total number of entities in your project by running the following query in your warehouse account:")
        self.io.display_message(query_distinct_main_ids)
        # run query
        result = self.db_manager.client.query_sql_with_result(query_distinct_main_ids)
        return result[0][0]

    def _explain_first_run(self, entity_name: str, id_stitcher_table_name: str):
        self.io.display_multiline_message(messages.EXPLAIN_FIRST_RUN_INTRO(entity_name))

        user_main_ids_query = f"""
        select {entity_name}_main_id, count(*) as num_of_other_ids 
        from {self.db_manager.get_qualified_name(id_stitcher_table_name)}
        group by 1 order by 2 desc"""
        print(user_main_ids_query)
        user_main_ids = self.db_manager.client.query_sql_with_result(
            user_main_ids_query
        )
        distinct_ids = len(user_main_ids)
        assert distinct_ids == 3
        alerting_text = f"""
        Oh no!! Something looks off here. It appears there are only 3 {entity_name}_main_ids. Let's bring in the id_type column and aggregate again so we can get a count per id_type, per {entity_name}_main_id.
        """
        self.io.display_multiline_message(alerting_text)

        query = f"""
        select {entity_name}_main_id, other_id_type, count(*) as cnt 
        from {self.db_manager.get_qualified_name(id_stitcher_table_name)} 
        group by 1,2 
        order by 1,3"""
        print(query)

        result = self.db_manager.client.query_sql_with_result(query)
        result.columns = [col.lower() for col in result.columns]
        self.io.display_message(result.to_string())
        self.io.display_multiline_message(
            messages.EXPLAIN_FIRST_RUN_PROMPT(entity_name)
        )
        self.io.get_user_input(
            "Enter 'done' to continue once you have made the changes",
            options=["done"],
        )
        # Validate whether the new yaml files have shopify_store_id
        while not self.yaml_generator.validate_shopify_store_id_is_removed():
            self.io.display_message(
                "Please make sure to remove shopify_store_id from pb_project.yaml, inputs.yaml and try again"
            )
            self.io.get_user_input(
                "Enter 'done' to continue once you have made the changes",
                options=["done"],
            )
        return distinct_ids

    def cluster_size_analysis(self, entity_name: str, id_stitcher_table_name: str):
        query = f"""
                select {entity_name}_main_id, count(*) as num_of_other_ids 
                from {self.db_manager.get_qualified_name(id_stitcher_table_name)}
                group by 1 
                order by 2 desc 
                """
        print(query)
        res = self.db_manager.client.query_sql_with_result(query)
        res.columns = [col.lower() for col in res.columns]
        return res

    def second_run(
        self,
        distinct_ids: int,
        entity_name: str,
        id_graph_model: str,
        id_stitcher_table_name: str,
        target: str,
    ):
        self.io.display_message("Now let's run the updated profiles project")
        seq_no, updated_id_stitcher_table_name = self.prompt_to_do_pb_run(
            id_graph_model, target
        )
        edge_table = f"{self.db_manager.get_qualified_name(updated_id_stitcher_table_name +'_internal_edges')}"
        # updated_id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        self.io.display_multiline_message(
            messages.EXPLAIN_SECOND_RUN_PROMPT(
                id_stitcher_table_name, updated_id_stitcher_table_name
            )
        )

        # prompt = f"""
        # You can see that the id stitcher table name has changed. It is now {updated_id_stitcher_table_name} (from earlier {id_stitcher_table_name}).
        # Notice how the hash has changed. This is because we removed shopify_store_id as an id_type, and hence the graph is different.
        # The hash is a fingerprint of the definition, so it changes if the profiles project changes.
        # What's the number of distinct {entity_name}_main_ids now?
        # """
        # self.io_handler.display_multiline_message(prompt)
        distinct_ids_upd = self.id_stitcher_queries(
            entity_name, updated_id_stitcher_table_name
        )
        # self.top_clusters_query = f"""
        #         select {entity_name}_main_id, count(*) as cluster_size
        #         from {self.config['output_database']}.{self.config['output_schema']}.{updated_id_stitcher_table_name}
        #         group by 1
        #         order by 2 desc
        #         """

        # It's always a good idea to do some sanity on the id stitcher table. Some common checks could be:
        # - Size of the top clusters. If you see some clusters having significantly more number of ids than others, may be there's some over stitching.
        #   A common next step here is to look at the exact ids in those clusters to give some hints.
        #   Occasionally, there may be some test accounts or internal emails that stitch a lot of users together.
        #   Or there may also be noisy data points - ex: a user_id called 'none', or a phone number with all zeros etc.
        #   You can use these observations to exclude such ids from your id stitcher.
        # - Size of singleton clusters - main_ids with just a single other_id. Often, these would be anonymous_ids - visitors that never converted to a user.
        #   But of there are other id types too, say, emails that were never linked to user_id etc, that may be worth investigating. There may be some understitching happening here.
        #   There may be some tables tying different id types that you may have missed. This can help you spot those.
        # - Also check the absolute numbers to see if they are in the ballpark you expect.
        #   This can be both the distinct main_ids after stitching, as well as the distinct other_ids within each other_id_type.
        # We have only a few hundreds of distinct main_ids, so we can check the count of all the clusters as the number is manageable.
        # We can use following query to check that:
        # """
        self.io.display_multiline_message(
            messages.EXPLAIN_SECOND_RUN_PROMPT_2(distinct_ids, distinct_ids_upd)
        )
        self.io.display_message(
            "You can run this query in your warehouse account to check the size of all the clusters"
        )
        result = self.cluster_size_analysis(entity_name, updated_id_stitcher_table_name)
        # result = self.db_manager.connector.run_query(self.top_clusters_query, output_type="pandas")
        self.io.display_message(result.head(10).to_string())
        self.io.display_multiline_message(messages.EXPLAIN_EDGES_TABLE(edge_table))

        # cluster_description = f"""
        # We should see the largest cluster having a count of {result.iloc[0]['num_of_other_ids']} and the smallest having a count of {result.iloc[-1]['num_of_other_ids']}
        # The average cluster size is {result['num_of_other_ids'].mean()}.
        # The top 10 clusters are as follows:
        # {result.head(10)}

        # There are three patterns we should see here:
        # 1. There are {len(result[result['num_of_other_ids']>100])} clusters which are huge.
        # 2. Then there are many clusters with around 20 ids
        # 3. And there are {len(result[result['num_of_other_ids']==1])} clusters with just a single id.

        # We will now introduce a new table in the warehouse - `{updated_id_stitcher_table_name}_internal_edges` which can be used to debug some of these.
        # This table is the input table from which the id stitcher model is built. You can always find it by adding a `_internal_edges` suffix to the id stitcher table name.
        # Each row in this table corresponds to two ids, id1 and id2, which come from different input tables. So these are the "direct edges" between various ids.
        # For example, if a user with anonymous_id 'a1' has signed in with a user_id 'u1' and this event is captured in the pages table, we see a corresponding row in this internal_edges table.
        # However, if this user 'u1' contacted us with a phone number 'p1' and this event is captured in some Salesforce table, we may not see a row having both 'a1' and 'p1' in the internal_edges table because they are from different tables. They are not "directly" related "yet"
        # So investigating this table often shows us noise in the input data. We can use following query to check the "bad" ids that stitched a lot of other ids in the top clusters:
        # """
        # self.io_handler.display_multiline_message(cluster_description)

        dense_edges_query = f"""
    WITH edge_table AS (    
        SELECT
        id1 AS id_val,
        id1_type AS id_type,
        id2 AS id_other,
        id2_type AS other_id_type
        FROM {edge_table}
        WHERE id1 <> id2

        UNION all

        SELECT
            id2 AS id_val,
            id2_type AS id_type,
            id1 AS id_other,
            id1_type AS other_id_type
        FROM {edge_table}
        WHERE id1 <> id2 
        )
    SELECT id_val,
        id_type,
        COUNT(*) as count_of_edges
    FROM edge_table
    GROUP BY ALL
    ORDER BY count_of_edges DESC;"""
        print(dense_edges_query)
        print("Running the query...")
        dense_edges = self.db_manager.client.query_sql_with_result(dense_edges_query)
        dense_edges.columns = [col.lower() for col in dense_edges.columns]
        self.io.display_message(dense_edges.head(20).to_string())
        self.io.display_multiline_message(messages.EXPLAIN_BAD_ANNOYMOUS_IDS)

        query_investigate_bad_anons = f"""
    WITH edge_table as (
            SELECT
            id1 AS id_val,
            id1_type AS id_type,
            id2 AS id_other,
            id2_type AS other_id_type
        FROM {edge_table}
        where id1 <> id2

        UNION all

        SELECT
            id2 AS id_val,
            id2_type AS id_type,
            id1 AS id_other,
            id1_type AS other_id_type
        FROM {edge_table}
        where id1 <> id2
        ),
    id_value_counts as (SELECT DISTINCT
            id_val,
            id_type,
            id_other,
            other_id_type,
            CASE WHEN other_id_type = 'anon_id' THEN 1 ELSE 0 END AS anonymous_id,
            CASE WHEN other_id_type = 'device_id' THEN 1 ELSE 0 END AS device_id,
            CASE WHEN other_id_type = 'user_id' THEN 1 ELSE 0 END AS user_id,
            CASE WHEN other_id_type = 'email' THEN 1 ELSE 0 END AS email,
            CASE WHEN other_id_type = 'shopify_customer_id' THEN 1 ELSE 0 END AS shopify_customer_id
    from edge_table )
    SELECT
        id_type,
        id_val,
        SUM(anonymous_id) AS anonymous_id_count,
        SUM(device_id) AS device_id_count,
        SUM(email) AS email_count,
        SUM(user_id) AS user_id_count,
        SUM(shopify_customer_id) AS shopify_customer_id_count,
        count(*) as total_count,
        ARRAY_SORT(ARRAY_AGG(OBJECT_CONSTRUCT(other_id_type, id_other))) AS id_list
    FROM id_value_counts
    group by 1,2
    order by email_count desc"""
        print(query_investigate_bad_anons)
        print("Running the query...")
        bad_anons = self.db_manager.client.query_sql_with_result(
            query_investigate_bad_anons
        )
        bad_anons.columns = [col.lower() for col in bad_anons.columns]
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        self.io.display_message(bad_anons.head(20).to_string())
        self.io.display_multiline_message(messages.EXPLAIN_BAD_ANNOYMOUS_IDS_2)
        self.io.display_multiline_message(
            "Click enter so we update your `pb_project.yaml` file with the regex filter to the anonymous_id type in the background"
        )
        self.yaml_generator.update_bad_anons_filter()
        self.io.display_message(
            "Now observe the filter in your pb_project.yaml file for anonymous_id. The ID stitcher model will now filter out these bad id values from the stitching process so these users will not be merged."
        )
        return seq_no, updated_id_stitcher_table_name

        # query_debug_top_clusters = f"""
        #     WITH top_clusters AS
        #     (SELECT {entity_name}_main_id,
        #             count(*) AS cnt
        #     FROM {updated_id_stitcher_table_name}
        #     GROUP BY user_main_id
        #     ORDER BY 2 DESC
        #     LIMIT 5),
        #         top_cluster_other_ids AS
        #     (SELECT other_id
        #     FROM {updated_id_stitcher_table_name} a
        #     INNER JOIN top_clusters b ON a.{entity_name}_main_id = b.{entity_name}_main_id),
        #         top_cluster_direct_edges AS
        #     (SELECT id1,
        #             id2
        #     FROM {updated_id_stitcher_table_name}_internal_edges
        #     WHERE id1 IN
        #         (SELECT *
        #             FROM top_cluster_other_ids)
        #         AND id1 != id2
        #     UNION ALL SELECT id2,
        #                         id1
        #     FROM {updated_id_stitcher_table_name}_internal_edges
        #     WHERE id1 IN
        #         (SELECT *
        #             FROM top_cluster_other_ids)
        #         AND id1 != id2)
        #     SELECT id1,
        #         count(DISTINCT id2) AS edge_count
        #     FROM top_cluster_direct_edges
        #     GROUP BY 1
        #     ORDER BY 2 DESC
        #     LIMIT 20
        # """
        # print(query_debug_top_clusters)
        # results = self.db_manager.connector.run_query(query_debug_top_clusters, output_type="pandas")
        # results.columns = [col.lower() for col in results.columns]
        # logger.info(results)
        # explain_bad_anon_ids = f"""
        # You can see 5 anon ids at the top, followed by many email ids.
        # These anon ids are inserted in the seed data on purpose. They are connecting too many emails/other identifiers together.
        # This is a scenario which often happens in real life.
        # A single customer sales rep for example logs into their customer accounts using one browser, linking many emails together through their anon id
        # We can exclude all these anonymous ids from the id stitcher.
        # This can be done by adding a 'filter' in the pb_project.yaml file, where we declare the id_types.
        # We can modify the `id_types` block for anonymous_id as follows:
        # """
        # self.io_handler.display_multiline_message(explain_bad_anon_ids)
        # filter_bad_ids = """
        #                 ```
        #                         id_types:
        #                             # Your other id_types - these need not be modified
        #                               - name: anonymous_id:
        #                                 filters:
        #                                     - type: exclude
        #                                       regex: "(c8bc33a0-7cb7-47f9-b24f-73e077346142|f0ed91a9-e1a9-46a5-9257-d590f45612fe|cbe0ea73-4878-4892-ac82-b9ad42797000|f4690568-e9e7-4182-abc6-6ea2791daba3|b369d6f5-c17a-457c-ab86-5649c1b53883)"
        #                         ```
        #                 """
        # logger.info(filter_bad_ids)
        # logger.info("This is only a simple filter, and the id stitcher offers a lot more flexibility in different filters. You can check our docs for more details.")
        # self.io_handler.get_user_input("Update your pb_project.yaml with above changes, and enter 'done' to continue once you have made the changes", options=["done"])
        # # check the above change is done or not
        # while not self.file_generator.validate_bad_anons_are_filtered():
        #     logger.info("That doesn't look right. Please make sure the above filters are added exactly as shown above, and try again")
        #     self.io_handler.get_user_input("Update your pb_project.yaml with above changes, and enter 'done' to continue once you have made the changes", options=["done"])

    def third_run(
        self,
        entity_name: str,
        id_graph_model: str,
        id_stitcher_table_1: str,
        id_stitcher_table_2: str,
        seq_no2: int,
        target: str,
    ):
        # Third run
        self.io.display_multiline_message(
            messages.EXPLAIN_SEQ_NO(seq_no2, id_stitcher_table_2)
        )
        seq_no3, id_stitcher_table_name_3 = self.prompt_to_do_pb_run(
            id_graph_model, target, command=f"pb run --seq_no {seq_no2}"
        )
        self.io.display_multiline_message(
            messages.EXPLAIN_THIRD_RUN_1(
                id_stitcher_table_1,
                id_stitcher_table_2,
                id_stitcher_table_name_3,
                entity_name,
            )
        )
        distinct_ids_3 = self.id_stitcher_queries(entity_name, id_stitcher_table_name_3)
        self.io.display_multiline_message(
            messages.EXPLAIN_THIRD_RUN_2(distinct_ids_3, entity_name)
        )

        result = self.cluster_size_analysis(entity_name, id_stitcher_table_name_3)
        self.io.display_message(result.head(20).to_string())

        self.io.display_multiline_message(messages.CONCLUSION_MESSAGE)

    # def execute_pb_run(self, first_run=True):
    #     if first_run:
    #         prompt = """
    #         Now let's run the generated profiles project. The command to run is `pb run`.
    #         You would normally do that in your terminal as a cli command.
    #         But for this guided demo, you can just enter the command and the tutorial will execute it for you.
    #         """
    #         self.io_handler.display_multiline_message(prompt)
    #     self.io_handler.get_user_input("Enter `pb run` to continue", options=["pb run"])
    #     logger.info("Running the profiles project...(This will take a few minutes)")
    #     os.chdir("profiles")
    #     self._subprocess_run(["pb", "run"])
    #     os.chdir("..")
    #     logger.info("Amazing! Profiles project run completed!")

    # def analyze_updated_results(self, entity_name: str, updated_id_stitcher_table_name: str, previous_distinct_ids: int):
    #     distinct_ids_upd = self.id_stitcher_queries(entity_name, updated_id_stitcher_table_name)
    #     self.explain_updated_results(entity_name, updated_id_stitcher_table_name, distinct_ids_upd, previous_distinct_ids)
    #     self.analyze_cluster_sizes(entity_name, updated_id_stitcher_table_name)
    #     self.debug_top_clusters(entity_name, updated_id_stitcher_table_name)

    # #def explain_updated_results(self, entity_name: str, updated_id_stitcher_table_name: str, distinct_ids_upd: int, previous_distinct_ids: int):

    # def execute_and_analyze_runs(self, entity_name: str):
    #     # First run
    #     self.execute_pb_run(first_run=True)
    #     id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
    #     self.explain_pb_run_results(entity_name, id_stitcher_table_name)
    #     distinct_main_ids = self.id_stitcher_queries(entity_name, id_stitcher_table_name)

    #     # Need for second run
    #     self._explain_first_run(distinct_main_ids, entity_name, id_stitcher_table_name)
    #     self.execute_pb_run(first_run=False)
    #     updated_id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
    #     self.analyze_updated_results(entity_name, updated_id_stitcher_table_name, distinct_main_ids)
