import logging
import sys
import os
import warnings
import subprocess
from input_handler import InputHandler, InputSteps
from database_manager import DatabaseManager
from file_generator import FileGenerator
from config import SAMPLE_DATA_DIR, TABLE_SUFFIX
warnings.filterwarnings("ignore", category=UserWarning, module="snowflake.connector")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
logging.getLogger("snowflake.connector.network").setLevel(logging.ERROR)

class ProfileBuilder:
    def __init__(self):
        self.config = {}
        self.db_manager = None
        self.input_handler = InputHandler()
        self.file_generator = FileGenerator()

    def run(self):
        self.display_welcome_message()
        entity_name = self.get_entity_name()
        id_types = self.get_id_types(entity_name)
        self.connect_to_snowflake()
        new_table_names = self.upload_sample_data()
        relevant_tables = self.find_relevant_tables(new_table_names)
        id_mappings = self.map_tables_to_id_types(relevant_tables, id_types, entity_name)
        connection_name = self.create_siteconfig()
        self.generate_project_files(entity_name, id_types, connection_name, id_mappings)
        logger.info("Profile Builder project files have been created successfully!")
        self.prompt_to_do_pb_run(first_run=True)
        id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        self.explain_pb_run_results(entity_name, id_stitcher_table_name)
        distinct_main_ids = self.id_stitcher_queries(entity_name, id_stitcher_table_name)
        self.second_run(distinct_main_ids, entity_name, id_stitcher_table_name)

    def display_welcome_message(self):
        welcome_message = """
        This is a guided tutorial on RudderStack Profiles (Press Enter/↵ to continue). 
        This involves details on key concepts of Profiles. You will also build a basic profiles project as you go through this tutorial (Press Enter/↵ to continue).
        Please read through the text in detail and press Enter (↵) to continue to the next step. It asks you to enter a few details in between.
        """
        self.input_handler.display_multiline_message(welcome_message)

    def get_entity_name(self):
        return self.input_handler.get_user_input("What do you like to call your primary entity? (ex - user, customer, account etc)")

    def get_id_types(self, entity_name):
        about_id_types = """
        You would be collecting different identifiers for this entity, such as email, phone number, user_id, anonymous_id etc.
        These identifiers form digital footprints across your event tracking systems. An entity is identified by these identifiers. 
        For example, a user may have an anonymous_id, email, phone_number, user_id etc. Each of these are called id_types.
        An account may have a domain, account_id, organisation_id etc. 
        A product may have sku, product_id, product_name etc.
        """
        self.input_handler.display_multiline_message(about_id_types)
        
        logger.info("You can add more id_types later, but for now, let's get started with the most important ones - typically seen in .")
        id_types = self.input_handler.guide_id_type_input(entity_name)

        about_id_types_advanced = """
        Some times, these ids may also be very transient, like a session id or an order_id. As long as they uniquely map to one entity (ex: user), you can use them.
        For example, an order_id is likely won't be shared by two users, so it can uniquely identify a user, even if we typically associate order_id with an order.
        This gets very useful when we join data from different systems such as payment systems, marketing campaign systems, order management systems etc.
        But for this guided demo, we will stick to the most important id_types.
        """
        self.input_handler.display_multiline_message(about_id_types_advanced)
        return id_types

    def connect_to_snowflake(self):
        logger.info("We will now search your warehouse account for tables and columns that are relevant for building profiles for this entity.")
        logger.info("Please provide the necessary details for connecting to your warehouse.")
        bypass = True
        if bypass:
            # get password from user
            password = self.input_handler.get_user_input("Enter password for profiles_demo user", password=True)
            self.config = {     
                "account": "ina31471.us-east-1",
                "output_database": "DREW_PB_TUTORIAL_DB",
                "output_schema": "DILEEP_TEST",
                "input_database": "DREW_PB_TUTORIAL_DB",
                "input_schema": "DILEEP_TEST",
                "password": password,
                "type": "snowflake",
                "role": "DEMO_ROLE",
                "warehouse": "ADHOC_WH",
                "user": "profiles_demo",
                }
        else:
            inputs = {}
            inputs.update(self.input_handler.collect_user_inputs(InputSteps.common()))
            inputs.update(self.input_handler.collect_user_inputs(InputSteps.input()))
            inputs.update(self.input_handler.collect_user_inputs(InputSteps.output()))

            self.config = {
                "account": inputs[InputSteps.ACCOUNT],
                "user": inputs[InputSteps.USERNAME],
                "password": inputs[InputSteps.PASSWORD],
                "role": inputs[InputSteps.ROLE],
                "warehouse": inputs[InputSteps.WAREHOUSE],
                "input_database": inputs[InputSteps.INPUT_DATABASE],
                "input_schema": inputs[InputSteps.INPUT_SCHEMA],
                "output_database": inputs[InputSteps.OUTPUT_DATABASE],
                "output_schema": inputs[InputSteps.OUTPUT_SCHEMA]
            }
        self.db_manager = DatabaseManager(self.config, self.input_handler)

    def upload_sample_data(self):
        logger.info(f"Uploading sample data to your warehouse account. We will add a suffix `{TABLE_SUFFIX}` to the table names to avoid conflicts and not overwrite any existing tables.")
        return self.db_manager.upload_sample_data(SAMPLE_DATA_DIR, TABLE_SUFFIX)

    def find_relevant_tables(self, new_table_names):
        logger.info(f"\n** Searching the `{self.config['input_schema']}` schema in `{self.config['input_database']}` database for uploaded sample tables, that will act as sources for profiles **\n")
        try:
            relevant_tables = self.db_manager.find_relevant_tables(new_table_names)
            if not relevant_tables:
                logger.error("No relevant tables found. Please check your inputs and try again.")
                sys.exit(1)
            return relevant_tables
        except Exception as e:
            logger.exception(f"An error occurred while fetching tables: {e}")
            sys.exit(1)

    def map_tables_to_id_types(self, relevant_tables, id_types, entity_name):
        id_mappings = {}
        table_index = 0
        while table_index < len(relevant_tables):
            table = relevant_tables[table_index]
            table_mappings, action = self.db_manager.map_columns_to_id_types(table, id_types, entity_name)
            if action == "back":
                if table_index > 0:
                    table_index -= 1
                    if relevant_tables[table_index] in id_mappings:
                        _ = id_mappings.pop(relevant_tables[table_index])
                else:
                    logger.info("You are already at the first table.")
            else:
                if table_mappings:
                    id_mappings[table] = {
                        "mappings": table_mappings,
                        "full_table_name": f"{self.config['input_database']}.{self.config['input_schema']}.{table}"
                    }
                table_index += 1
        return id_mappings

    def create_siteconfig(self):
        return self.file_generator.create_siteconfig(self.config)

    def generate_project_files(self, entity_name, id_types, connection_name, id_mappings):
        # create a directory called profiles if doesn't exist
        if not os.path.exists("profiles"):
            os.makedirs("profiles")
        self.file_generator.create_pb_project(entity_name, id_types, connection_name)
        self.file_generator.create_inputs_yaml(id_mappings)
        self.file_generator.create_profiles_yaml(entity_name, id_mappings.keys())         
        about_files = f"""
        We will now discuss a couple of files that are needed to build profiles for this entity.
        First, the `siteconfig.yaml` file we just created.  You can check it and come back -  it's stored in a folder called `.pb` in your home directory.
        Next, we have created a folder called `profiles` in the current directory, with some files too. The contents look like this:
        ```
        .
        └── profiles
            ├── pb_project.yaml
            └── models
                └── inputs.yaml
                └── profiles.yaml
        
        Here's a brief description of what each file is:

        - `pb_project.yaml`: This file contains the project declaration - name of the project, with the  {entity_name} entity you defined, their id types etc. It also includes the warehouse connection info with the name {connection_name} (the same name as in siteconfig.yaml). 
        We have filled some of the details for you. You can check it and come back (please do not edit this file for now)
        - `models/inputs.yaml`: This file contains the input data sources - the tables and columns that map to the entities and their id types.
        - `models/profiles.yaml`: This is where we define the actual model configuration -  what all tables/sources it should look at for an id stitcher, what user features we should build etc.
        """
        self.input_handler.display_multiline_message(about_files)

    def _subprocess_run(self, args):
        response = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
    
    def prompt_to_do_pb_run(self, first_run=True):
        if first_run:
            prompt = """
            Now let's run the generated profiles project. The command to run is `pb run`. 
            You would normally do that in your terminal as a cli command.
            But for this guided demo, you can just enter the command and the tutorial will execute it for you.
            """
            self.input_handler.display_multiline_message(prompt)
        self.input_handler.get_user_input("Enter `pb run` to continue", options=["pb run"])
        logger.info("Running the profiles project...(This will take a few minutes)")
        os.chdir("profiles")
        self._subprocess_run(["pb", "run"])
        os.chdir("..")
        logger.info("Amazing! Profiles project run completed!")

    def get_latest_id_stitcher_table_name(self, entity_name: str) -> str:
        id_stitchers = []
        id_stitcher_model_name = f"{entity_name}_id_stitcher"
        for folder in os.listdir("profiles/output/prod/seq_no/latest/run"):
            if id_stitcher_model_name in folder:
                id_stitchers.append(folder)
        if len(id_stitchers) == 1:
            return id_stitchers[0]
        else:
            creation_time = 0
            for id_stitcher in id_stitchers:
                t = os.path.getctime(f"profiles/output/prod/seq_no/latest/run/{id_stitcher}")
                if t > creation_time:
                    creation_time = t
                    id_stitcher_table_name = id_stitcher
            return id_stitcher_table_name

    def explain_pb_run_results(self, entity_name: str, id_stitcher_table_name: str):
        prompt = f"""
        This should have created multiple tables in your warehouse account, and a lot of files in the profiles folder. 
        Let's explore them. First, the files. 
        In the profiles folder, you should see a folder structure like this:
        ```
        .
        └── profiles
            ├── output
            │   └── prod
            │       └── seq_no
            │           └── 1
            │              └── compile
            │              └── run
            |                 └── Material_{entity_name}_id_stitcher_<hash>_1.sql
            │                 └── Material_{entity_name}_all_<hash>_1.sql
            .....
        ```
        The output folder contains the results of the profiles project run. Each run creates a new seq_no folder, with the sql files that powered the run.
        You see multiple sql files, and also some folders (again containing sql files within them).
        The profiles-core generates these sql files, creates a dependency graph so that it knows the order in which these sql files should be run, and finally runs them.
        The sql files have the queries that ran on your warehouse account and have created various tables. 
        For this run, there is one important table created - {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name}, which has the id_stitcher output. 
        You can check it in your warehouse. There are three key columns in this table, along with some other timestamp meta-data columns we can ignore for now:
            - `{entity_name}_main_id` - This is an id that Profiles creates. It uniquely identifies an entity (ex: user) across systems.
            - `other_id` - the id stitched to the original id. This is what your data already has - email, anonymous_id etc. 
            - `other_id_type` - the type of id from the original table. These are all the id_types you had provided earlier.
        Basically, if two users have the same email and phone number, their {entity_name}_main_id will be the same, and we will map them to the same entity.
        So in this table, we can expect multiple rows for a unique {entity_name}, all sharing the same {entity_name}_main_id. But other_id + other_id_type should be unique. 
        """
        self.input_handler.display_multiline_message(prompt)
    
    def id_stitcher_queries(self, entity_name: str, id_stitcher_table_name: str) -> int:
        query_distinct_main_ids = f"select count(distinct {entity_name}_main_id) from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name}"
        logger.info("You can check the total number of entities in your project by running the following query in your warehouse account:")
        logger.info(query_distinct_main_ids)
        # run query
        result = self.db_manager.connector.run_query(query_distinct_main_ids)
        logger.info(f"We ran it for you, and the number you should see is: {result[0][0]}")
        return result[0][0]
        
    def _explain_first_run(self, distinct_ids: int, entity_name: str, id_stitcher_table_name: str):
        assert distinct_ids == 3
        query = f"""select {entity_name}_main_id, other_id_type, count(*) as cnt from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name}
        group by 1,2
        order by 1,3"""

        prompt = f"""
        You can see that the number of distinct {entity_name}_main_ids is 3. 
        That's very low for a real project. What's happening?
        Let's explore by first checking the number of distinct other ids of each type, within each of these 3 main ids.
        You can run the following query in your warehouse account to see the number of distinct other ids of each type, within each of these 3 main ids:
        {query}
        NOTE: This is feasible here as we just have 3 main ids. For a real project, you will have a lot more, so the exact query wouldn't be practical.
        """

        self.input_handler.display_multiline_message(prompt)
        logger.info(f"We are running it for you, and the result is:")
        result = self.db_manager.connector.run_query(query, output_type="pandas")
        logger.info(result)

        prompt = f"""
        You can see that for each of the three main ids, there are tens of emails, user_ids, device_ids etc all mapping to a single {entity_name}_main_id. 
        But interestingly, there's a single shopify_store_id per main_id
        This is an important clue. A shopify_store_id is not something that we associate at a {entity_name} level, but instead at a shop/store level.
        Adding this in the id stitcher has resulted in over-stitching of the {entity_name}s. 
        Now let's fix this. We need to make two changes to the project files. First in pb_project.yaml, and second in inputs.yaml.
        In `pb_project.yaml`, let's remove `shopify_store_id` as an id_type. This is in three places - 
            - under entities -> id_types
            - under feature_views -> using_ids
            - under id_types
        You can remove all the lines with `shopify_store_id` in them.
        This tells profiles that 'shopify_store_id' is not an id_type associated with {entity_name}.
        While this file has the declaration of the id graph that we now cleaned up, inputs.yaml has the actual data sources and their ids.
        So we need to remove the shopify_store_id from inputs.yaml too.
        In `inputs.yaml`, remove all references to `shopify_store_id` as an id. You should see it in identifies and pages tables.
        """
        self.input_handler.display_multiline_message(prompt)
        self.input_handler.get_user_input("Enter 'done' to continue once you have made the changes", options=["done"])

    def second_run(self, distinct_ids: int, entity_name: str, id_stitcher_table_name: str):
        self._explain_first_run(distinct_ids, entity_name, id_stitcher_table_name)
        logger.info("Now let's run the updated profiles project")
        self.prompt_to_do_pb_run(first_run=False)
        updated_id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        prompt = f"""
        You can see that the id stitcher table name has changed. It is now {updated_id_stitcher_table_name}.
        Notice how the hash has changed. This is because we removed shopify_store_id as an id_type, and hence the graph is different.
        The hash is a fingerprint of the definition, so it changes if the profiles project changes.
        What's the number of distinct {entity_name}_main_ids now?
        """
        self.input_handler.display_multiline_message(prompt)
        distinct_ids_upd = self.id_stitcher_queries(entity_name, updated_id_stitcher_table_name)
        prompt = f"""
        This is {distinct_ids_upd}. This is much better than the earlier number {distinct_ids}!
        It's important to note how adding a bad id type resulted in such a notorious id stitching, collapsing all users into just a handful of main ids
        But bad id types aren't the only reason for over stitching. Sometimes, a few bad data points can also cause this
        """
        self.input_handler.display_multiline_message(prompt)


def main():
    profile_builder = ProfileBuilder()
    profile_builder.run()

if __name__ == "__main__":
    main()