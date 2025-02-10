import logging
import re
import sys
import os
import warnings
import subprocess
import pandas as pd
from datetime import datetime

from .io_handler import IOHandler
from .database_manager import DatabaseManager
from .yaml_generator import YamlGenerator
from . import messages
from .config import (
    SAMPLE_DATA_DIR,
    TABLE_SUFFIX,
    ID_GRAPH_MODEL_SUFFIX,
    PRE_DEFINED_FEATURES,
    USER_DEFINED_FEATURES,
    PRE_DEFINED_MACROS,
    PROFILES_TUTORIAL_CLI_DIR,
)


from profiles_mlcorelib.utils.tracking import Analytics
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.reader import Reader

warnings.filterwarnings("ignore", category=UserWarning, module="snowflake.connector")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
logging.getLogger("snowflake.connector.network").setLevel(logging.ERROR)


class ProfileBuilder:
    def __init__(self, reader: Reader, fast_mode: bool, run_id: str):
        self.io = IOHandler(reader, fast_mode)
        self.yaml_generator = YamlGenerator(self.io)
        self.fast_mode = fast_mode
        self.run_id = run_id

    def run(self, material: WhtMaterial):
        analytics = Analytics()
        analytics.show_consent_message(logger)
        start_time = datetime.now()
        analytics.track(
            "model_run_start",
            {"run_id": self.run_id, "model_type": "profiles_tutorial"},
        )
        run_completed = False
        self.db_manager = DatabaseManager(material, self.io, self.fast_mode)
        self.display_welcome_message()
        new_table_names = self.upload_sample_data()
        relevant_tables = self.find_relevant_tables(new_table_names)
        conn_name, target = self.db_manager.client.get_connection_and_target()
        self.display_about_project_files(conn_name)
        # pb_project.yaml
        entity_name, id_types, id_graph_model = self.generate_pb_project(conn_name)
        # inputs.yaml
        self.map_tables_to_id_types(relevant_tables, id_types, entity_name)
        # profiles.yaml
        self.build_id_stitcher_model(relevant_tables, entity_name, id_graph_model)
        self.pb_runs(entity_name, id_types, id_graph_model, target)
        duration = (datetime.now() - start_time).total_seconds()
        run_completed = True
        analytics.track(
            "model_run_end",
            {
                "run_id": self.run_id,
                "model_type": "profiles_tutorial",
                "duration_in_sec": duration,
                "is_run_completed": run_completed,
            },
        )

    def pb_runs(
        self, entity_name, id_types: list[str], id_graph_model: str, target: str
    ):
        self.io.display_multiline_message(messages.ABOUT_PB_COMPILE)
        self.io.get_user_input("Enter `pb compile`", options=["pb compile"])
        os.chdir(PROFILES_TUTORIAL_CLI_DIR)
        pb_compile_output = self._subprocess_run(
            ["pb", "compile", "--target", target, "--migrate_on_load"]
        )
        os.chdir("..")
        _ = self.explain_pb_compile_results(target, pb_compile_output, id_graph_model)
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
        # Add features and create feature views
        self.fourth_run(entity_name, id_types, target)

    def explain_pb_compile_results(
        self, target: str, pb_compile_output: str, id_graph_model: str
    ):
        seq_no, _ = self.parse_material_name(pb_compile_output, id_graph_model)
        self.io.display_multiline_message(
            messages.EXPLAIN_PB_COMPILE_RESULTS(
                target, seq_no, PROFILES_TUTORIAL_CLI_DIR
            )
        )
        return seq_no

    def display_welcome_message(self):
        self.io.display_message(messages.WELCOME_MESSAGE)
        self.io.display_multiline_message(
            f"Please read through the text in detail{'.' if self.fast_mode else ' and press Enter to continue to each next step. Press “Enter” now'}"
        )
        self.io.display_multiline_message(
            messages.FICTIONAL_BUSINESS_OVERVIEW(self.fast_mode)
        )

    def get_entity_name(self):
        self.io.display_message(messages.ABOUT_ENTITY)
        return self.io.get_user_input(
            "For the purpose of this tutorial, we will define a 'user' entity. Please enter `user`",
            options=["user"],
        )

    def get_id_types(self, entity_name):
        self.io.display_multiline_message(messages.ABOUT_ID_TYPES)
        id_types = self.yaml_generator.guide_id_type_input(entity_name)
        self.io.display_message(messages.ABOUT_ID_TYPES_CONCLUSSION)
        return id_types

    def upload_sample_data(self):
        self.io.display_message(
            "Now, let's seed your warehouse with the sample data from Secure Solutions, LLC"
        )
        self.io.display_message(
            f"We will add a suffix `{TABLE_SUFFIX}` to the table names to avoid conflicts and not overwrite any existing tables."
        )
        return self.db_manager.upload_sample_data(SAMPLE_DATA_DIR, TABLE_SUFFIX)

    def find_relevant_tables(self, new_table_names):
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
                        "full_table_name": f"{self.db_manager.get_qualified_name(table, True)}",
                    }
                table_index += 1
        self.yaml_generator.create_inputs_yaml(id_mappings)
        self.io.display_message(
            "Perfect! You can now examine your inputs.yaml file and see the input definitions."
        )

    def build_id_stitcher_model(self, table_names, entity_name, id_graph_model):
        self.io.display_multiline_message(messages.ABOUT_ID_STITCHER_1)
        id_graph_model_name = self.io.get_user_input(
            f"Enter a name for the model, for example: `{id_graph_model}`",
            options=[id_graph_model],
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

    def display_about_project_files(self, connection_name):
        self.io.display_multiline_message(
            messages.ABOUT_PROFILES_FILES(connection_name)
        )

    def generate_pb_project(self, connection_name):
        # create a directory called profiles if doesn't exist
        if not os.path.exists(PROFILES_TUTORIAL_CLI_DIR):
            os.makedirs(PROFILES_TUTORIAL_CLI_DIR)
        if not os.path.exists(f"{PROFILES_TUTORIAL_CLI_DIR}/models"):
            os.makedirs(f"{PROFILES_TUTORIAL_CLI_DIR}/models")
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

    def _subprocess_run(self, args):
        response = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        assert (
            response.returncode == 0
        ), f"Command {args} failed with error: {response.stderr}\nMore info: {response.stdout}"
        return response.stdout

    def parse_material_name(self, pb_output_text: str, model_name: str):
        seq_no_pattern = r"--seq_no (\d+)"
        match = re.search(seq_no_pattern, pb_output_text)
        assert match, f"Failed to find seq_no in the pb  output {pb_output_text}"
        seq_no = match.group(1)
        pattern = rf"(Material_{model_name}_[a-f0-9]+_\d+)"
        match = re.search(pattern, pb_output_text)
        assert (
            match
        ), f"Failed to find id_graph_table_name in the pb  output {pb_output_text}"
        material_name = match.group(1)
        return int(seq_no), material_name

    def prompt_to_do_pb_run(
        self,
        model_name: str,
        target: str,
        command: str = "pb run",
        include_default: bool = False,
    ):
        self.io.get_user_input(
            f"Enter `{command}` to continue",
            options=[command],
            default=command if include_default else None,
        )
        self.io.display_message(
            "Running the profiles project...(This will take a few minutes)"
        )
        os.chdir(PROFILES_TUTORIAL_CLI_DIR)
        pb_run_output = self._subprocess_run(
            [*command.split(), "--target", target, "--migrate_on_load"]
        )
        os.chdir("..")
        seq_no, material_name = self.parse_material_name(pb_run_output, model_name)
        self.io.display_message("Done")
        return seq_no, material_name

    def explain_pb_run_results(self, entity_name: str, id_stitcher_table_name: str):
        self.io.display_multiline_message(
            messages.EXPLAIN_PB_RUN_RESULTS(
                id_stitcher_table_name,
                entity_name,
                self.db_manager.schema,
                self.db_manager.db,
                PROFILES_TUTORIAL_CLI_DIR,
            )
        )
        distinct_ids = self._explain_first_run(entity_name, id_stitcher_table_name)
        return distinct_ids

    def id_stitcher_queries(self, entity_name: str, id_stitcher_table_name: str) -> int:
        query_distinct_main_ids = f"select count(distinct {entity_name}_main_id) from {self.db_manager.get_qualified_name(id_stitcher_table_name)}"
        self.io.display_message(query_distinct_main_ids)
        # run query
        result = self.db_manager.client.query_sql_with_result(query_distinct_main_ids)
        return result.iloc[0][0]

    def _explain_first_run(self, entity_name: str, id_stitcher_table_name: str):
        self.io.display_multiline_message(messages.EXPLAIN_FIRST_RUN_INTRO(entity_name))
        user_main_ids_query = f"""
        select {entity_name}_main_id, 
                count(*) as num_of_other_ids 
        from {self.db_manager.get_qualified_name(id_stitcher_table_name)} 
        group by 1 order by 2 desc"""
        self.io.display_message(user_main_ids_query)
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
        select {entity_name}_main_id, 
               other_id_type, count(*) as cnt 
        from {self.db_manager.get_qualified_name(id_stitcher_table_name)} 
        group by 1,2 
        order by 1,3"""
        self.io.display_message(query)

        result = self.db_manager.client.query_sql_with_result(query)
        result.columns = [col.lower() for col in result.columns]
        self.io.display_message(result.to_string())
        self.io.display_multiline_message(
            messages.EXPLAIN_FIRST_RUN_PROMPT(entity_name)
        )

        if self.fast_mode:
            self.io.display_message(
                "Removing shopify_store_id from pb_project.yaml and inputs.yaml"
            )
            self.yaml_generator.remove_shopify_store_id()
        else:
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
        self.io.display_message(query)
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
        edge_table = f"{self.db_manager.get_qualified_name(updated_id_stitcher_table_name)}_internal_edges"
        # updated_id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        self.io.display_multiline_message(
            messages.EXPLAIN_SECOND_RUN_PROMPT(
                id_stitcher_table_name, updated_id_stitcher_table_name
            )
        )

        distinct_ids_upd = self.id_stitcher_queries(
            entity_name, updated_id_stitcher_table_name
        )
        self.io.display_multiline_message(
            messages.EXPLAIN_SECOND_RUN_PROMPT_2(distinct_ids, distinct_ids_upd)
        )
        self.io.display_message(
            "You can run this query in your warehouse account to check the size of all the clusters"
        )
        result = self.cluster_size_analysis(entity_name, updated_id_stitcher_table_name)
        self.io.display_message(result.head(10).to_string())
        self.io.display_multiline_message(messages.EXPLAIN_EDGES_TABLE(edge_table))

        dense_edges_query_template = f"""
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
{{% if warehouse.DatabaseType() == 'redshift' %}}
    GROUP BY id_val, id_type
{{% else %}}
    GROUP BY ALL
{{% endif %}}
    ORDER BY count_of_edges DESC;"""
        dense_edges_query = self.db_manager.material.execute_text_template(
            dense_edges_query_template, skip_material_wrapper=True
        )
        self.io.display_message(dense_edges_query)
        self.io.display_message("Running the query...")
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
        count(*) as total_count
    FROM id_value_counts
    group by 1,2
    order by email_count desc"""
        # Id Lists
        # {{% if warehouse.DatabaseType() == 'snowflake' %}}
        #     ARRAY_SORT(ARRAY_AGG(OBJECT_CONSTRUCT(other_id_type, id_other))) AS id_list
        # {{% elif warehouse.DatabaseType() == 'redhsift' %}}
        #     JSON_PARSE('[' || LISTAGG('{{"' || other_id_type || '"' || ':' || id_other || '}}', ', ') WITHIN GROUP (ORDER BY other_id_type) || ']') as id_list
        # {{% elif warehouse.DatabaseType() == 'databricks' %}}
        #     COLLECT_LIST(map(other_id_type, id_other)) as id_list
        # {{% elif warehouse.DatabaseType() == 'bigquery' %}}
        #     ARRAY_AGG(STRUCT(other_id_type, id_other) ORDER BY other_id_type) as id_list
        # {{% endif %}}
        query_investigate_bad_anons_sql = (
            self.db_manager.material.execute_text_template(
                query_investigate_bad_anons, skip_material_wrapper=True
            )
        )
        self.io.display_message(query_investigate_bad_anons_sql)
        self.io.display_message("Running the query...")
        bad_anons = self.db_manager.client.query_sql_with_result(
            query_investigate_bad_anons_sql
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
        # ToDo :Removing seq no from the prompt temporarily till the bug is fixed in pb
        # self.io.display_multiline_message(
        #     messages.EXPLAIN_SEQ_NO(seq_no2, id_stitcher_table_2)
        # )
        _, id_stitcher_table_name_3 = self.prompt_to_do_pb_run(
            id_graph_model,
            target,
            command="pb run",
            # command=f"pb run --seq_no {seq_no2}",
            # include_default=True,
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
        self.io.display_multiline_message(messages.THIRD_RUN_CONCLUSION_MESSAGE)

    def fourth_run(
        self,
        entity_name: str,
        id_types: list[str],
        target: str,
    ):
        self.io.display_multiline_message(messages.FEATURE_CREATION_INTRO(entity_name))

        for feature in USER_DEFINED_FEATURES:
            self.io.display_message(feature["user_prompt"])
            self.io.get_user_input(
                "Let's input the name of this feature for our c360 view:\n",
                options=[feature["name"]],
                default=feature["name"],
            )
            self.io.get_user_input(
                "Enter the aggregation function to use as well as the column to select from:\n",
                options=[feature["select"]],
                default=feature["select"],
            )
            if "order_by_prompt" in feature:
                self.io.get_user_input(
                    f"{feature['order_by_prompt']}\n",
                    options=[feature["window"]["order_by"][0]],
                    default=feature["window"]["order_by"][0],
                )

            self.io.get_user_input(
                "Now, let's enter the data source for this feature:\n",
                options=[feature["from"]],
                default=feature["from"],
            )

        self.yaml_generator.add_macros(PRE_DEFINED_MACROS)
        self.yaml_generator.add_features(
            entity_name, [*USER_DEFINED_FEATURES, *PRE_DEFINED_FEATURES]
        )
        self.io.display_multiline_message(messages.FEATURES_ADDED)
        self.io.display_multiline_message(messages.DEFINE_FEATURE_VIEW)

        feature_view_name = self.io.get_user_input(
            "Let's define this customer feature view with a name as well as the id_type we want to use as our primary key:\n",
            options=[f"customers_by_{id_type}" for id_type in id_types],
            default=f"customers_by_email",
        )
        feature_view_using_id = self.io.get_user_input(
            "Now, let's choose what id_type we want to use for the primary key:\n",
            options=id_types,
            default="email",
        )
        self.yaml_generator.add_feature_views(
            entity_name,
            [{"id": feature_view_using_id, "name": feature_view_name}],
        )
        self.io.display_message(
            "You can now look at your pb_project.yaml and see the defined feature view. Again, this will output 2 views. The default view created automatically, and the customer one you defined here. "
        )

        seq_no, feature_view_table_name = self.prompt_to_do_pb_run(
            feature_view_name, target
        )
        self.io.display_message(
            "Great job! You have now created two feature views. Let's query the custom feature view you created."
        )

        sql = f"select * from {self.db_manager.get_qualified_name(feature_view_table_name)}"
        self.io.display_message(sql)
        res = self.db_manager.client.query_sql_with_result(sql)
        self.io.display_message(res.head(10).to_string())

        self.io.display_multiline_message(messages.CONCLUSSION)
