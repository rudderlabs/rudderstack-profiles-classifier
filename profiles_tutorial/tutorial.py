import logging
import sys
import os
import warnings
import subprocess
from input_handler import InputHandler, InputSteps
from database_manager import DatabaseManager
from file_generator import FileGenerator
import yaml
from config import SAMPLE_DATA_DIR, TABLE_SUFFIX, CONFIG_FILE_PATH, INPUTS_FILE_PATH
warnings.filterwarnings("ignore", category=UserWarning, module="snowflake.connector")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
logging.getLogger("snowflake.connector.network").setLevel(logging.ERROR)

class ProfileBuilder:
    def __init__(self, fast_mode: bool):
        self.config = {}
        self.db_manager = None
        self.input_handler = InputHandler(fast_mode)
        self.file_generator = FileGenerator(fast_mode)
        self.fast_mode = fast_mode

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
        #id_stitcher_table_name = "Material_user_id_stitcher_6a2cd3af_10" #FixMe: comment this line
        self.explain_pb_run_results(entity_name, id_stitcher_table_name)
        distinct_main_ids = self.id_stitcher_queries(entity_name, id_stitcher_table_name)
        self.second_run(distinct_main_ids, entity_name, id_stitcher_table_name)

    def display_welcome_message(self):
        welcome_message = """\tThis is a guided tutorial on RudderStack Profiles (Press Enter/↵ to continue). 
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
        self.db_manager = DatabaseManager(self.config, self.input_handler, self.fast_mode)

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
        if not os.path.exists("profiles/models"):
            os.makedirs("profiles/models")
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
        query = f"""select {entity_name}_main_id, other_id_type, count(*) as cnt from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name} group by 1,2 order by 1,3"""

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
        result.columns = [col.lower() for col in result.columns]
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
        # Validate whether the new yaml files have shopify_store_id
        while not self.file_generator.validate_shopify_store_id_is_removed():
            logger.info("Please make sure to remove shopify_store_id from pb_project.yaml, inputs.yaml and try again")
            self.input_handler.get_user_input("Enter 'done' to continue once you have made the changes", options=["done"])

    def cluster_size_analysis(self, entity_name: str, id_stitcher_table_name: str):
        query = f"""
                select {entity_name}_main_id, count(*) as cluster_size 
                from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name} 
                group by 1 
                order by 2 desc 
                """
        logger.info(query)
        res =  self.db_manager.connector.run_query(query, output_type="pandas")
        res.columns = [col.lower() for col in res.columns]
        return res

    def second_run(self, distinct_ids: int, entity_name: str, id_stitcher_table_name: str):
        self._explain_first_run(distinct_ids, entity_name, id_stitcher_table_name)
        logger.info("Now let's run the updated profiles project")
        self.prompt_to_do_pb_run(first_run=False) #FixMe: Uncomment 
        updated_id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        prompt = f"""
        You can see that the id stitcher table name has changed. It is now {updated_id_stitcher_table_name} (from earlier {id_stitcher_table_name}).
        Notice how the hash has changed. This is because we removed shopify_store_id as an id_type, and hence the graph is different.
        The hash is a fingerprint of the definition, so it changes if the profiles project changes.
        What's the number of distinct {entity_name}_main_ids now?
        """
        self.input_handler.display_multiline_message(prompt)
        distinct_ids_upd = self.id_stitcher_queries(entity_name, updated_id_stitcher_table_name)
        # self.top_clusters_query = f"""
        #         select {entity_name}_main_id, count(*) as cluster_size 
        #         from {self.config['output_database']}.{self.config['output_schema']}.{updated_id_stitcher_table_name} 
        #         group by 1 
        #         order by 2 desc 
        #         """   
        prompt = f"""
        This is {distinct_ids_upd}. This is much better than the earlier number {distinct_ids}!
        It's important to note how adding a bad id type resulted in such a notorious id stitching, collapsing all users into just a handful of main ids
        But bad id types aren't the only reason for over stitching. Sometimes, a few bad data points can also cause this. 
        It's always a good idea to do some sanity on the id stitcher table. Some common checks could be:
        - Size of the top clusters. If you see some clusters having significantly more number of ids than others, may be there's some over stitching. 
          A common next step here is to look at the exact ids in those clusters to give some hints.
          Occasionally, there may be some test accounts or internal emails that stitch a lot of users together.
          Or there may also be noisy data points - ex: a user_id called 'none', or a phone number with all zeros etc. 
          You can use these observations to exclude such ids from your id stitcher.
        - Size of singleton clusters - main_ids with just a single other_id. Often, these would be anonymous_ids - visitors that never converted to a user. 
          But of there are other id types too, say, emails that were never linked to user_id etc, that may be worth investigating. There may be some understitching happening here.
          There may be some tables tying different id types that you may have missed. This can help you spot those.
        - Also check the absolute numbers to see if they are in the ballpark you expect. 
          This can be both the distinct main_ids after stitching, as well as the distinct other_ids within each other_id_type. 
        We have only a few hundreds of distinct main_ids, so we can check the count of all the clusters as the number is manageable.
        We can use following query to check that:
        """
        self.input_handler.display_multiline_message(prompt)
        #logger.info(self.top_clusters_query)
        logger.info("You can run this query in your warehouse account to check the size of all the clusters")
        result = self.cluster_size_analysis(entity_name, updated_id_stitcher_table_name)
        #result = self.db_manager.connector.run_query(self.top_clusters_query, output_type="pandas")
        cluster_description = f"""
        We should see the largest cluster having a count of {result.iloc[0]['cluster_size']} and the smallest having a count of {result.iloc[-1]['cluster_size']}
        The average cluster size is {result['cluster_size'].mean()}. 
        The top 10 clusters are as follows:
        {result.head(10)}

        There are three patterns we should see here:
        1. There are {len(result[result['cluster_size']>100])} clusters which are huge. 
        2. Then there are many clusters with around 20 ids
        3. And there are {len(result[result['cluster_size']==1])} clusters with just a single id. 

        We will now introduce a new table in the warehouse - `{updated_id_stitcher_table_name}_internal_edges` which can be used to debug some of these.
        This table is the input table from which the id stitcher model is built. You can always find it by adding a `_internal_edges` suffix to the id stitcher table name.
        Each row in this table corresponds to two ids, id1 and id2, which come from different input tables. So these are the "direct edges" between various ids. 
        For example, if a user with anonymous_id 'a1' has signed in with a user_id 'u1' and this event is captured in the pages table, we see a corresponding row in this internal_edges table.
        However, if this user 'u1' contacted us with a phone number 'p1' and this event is captured in some Salesforce table, we may not see a row having both 'a1' and 'p1' in the internal_edges table because they are from different tables. They are not "directly" related "yet"
        So investigating this table often shows us noise in the input data. We can use following query to check the "bad" ids that stitched a lot of other ids in the top clusters:
        """      
        self.input_handler.display_multiline_message(cluster_description)
        query_debug_top_clusters = f"""
            WITH top_clusters AS
            (SELECT {entity_name}_main_id,
                    count(*) AS cnt
            FROM {updated_id_stitcher_table_name}
            GROUP BY user_main_id
            ORDER BY 2 DESC
            LIMIT 5),
                top_cluster_other_ids AS
            (SELECT other_id
            FROM {updated_id_stitcher_table_name} a
            INNER JOIN top_clusters b ON a.{entity_name}_main_id = b.{entity_name}_main_id),
                top_cluster_direct_edges AS
            (SELECT id1,
                    id2
            FROM {updated_id_stitcher_table_name}_internal_edges
            WHERE id1 IN
                (SELECT *
                    FROM top_cluster_other_ids)
                AND id1 != id2
            UNION ALL SELECT id2,
                                id1
            FROM {updated_id_stitcher_table_name}_internal_edges
            WHERE id1 IN
                (SELECT *
                    FROM top_cluster_other_ids)
                AND id1 != id2)
            SELECT id1,
                count(DISTINCT id2) AS edge_count
            FROM top_cluster_direct_edges
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 20
        """
        logger.info(query_debug_top_clusters)
        results = self.db_manager.connector.run_query(query_debug_top_clusters, output_type="pandas")
        results.columns = [col.lower() for col in results.columns]
        logger.info(results)
        explain_bad_anon_ids = f"""
        You can see 5 anon ids at the top, followed by many email ids. 
        These anon ids are inserted in the seed data on purpose. They are connecting too many emails/other identifiers together.
        This is a scenario which often happens in real life. 
        A single customer sales rep for example logs into their customer accounts using one browser, linking many emails together through their anon id
        We can exclude all these anonymous ids from the id stitcher. 
        This can be done by adding a 'filter' in the pb_project.yaml file, where we declare the id_types. 
        We can modify the `id_types` block for anonymous_id as follows:
        """
        self.input_handler.display_multiline_message(explain_bad_anon_ids)
        filter_bad_ids = """
                        ```
                                id_types:
                                    # Your other id_types - these need not be modified
                                      - name: anonymous_id:
                                        filters:
                                            - type: exclude
                                              regex: "(c8bc33a0-7cb7-47f9-b24f-73e077346142|f0ed91a9-e1a9-46a5-9257-d590f45612fe|cbe0ea73-4878-4892-ac82-b9ad42797000|f4690568-e9e7-4182-abc6-6ea2791daba3|b369d6f5-c17a-457c-ab86-5649c1b53883)"
                                ```
                        """
        logger.info(filter_bad_ids)
        logger.info("This is only a simple filter, and the id stitcher offers a lot more flexibility in different filters. You can check our docs for more details.")
        self.input_handler.get_user_input("Update your pb_project.yaml with above changes, and enter 'done' to continue once you have made the changes", options=["done"])
        # check the above change is done or not
        while not self.file_generator.validate_bad_anons_are_filtered():
            logger.info("That doesn't look right. Please make sure the above filters are added exactly as shown above, and try again")
            self.input_handler.get_user_input("Update your pb_project.yaml with above changes, and enter 'done' to continue once you have made the changes", options=["done"])
        # Third run
        self.prompt_to_do_pb_run(first_run=False)
        id_stitcher_table_name_3 = self.get_latest_id_stitcher_table_name(entity_name)
        prompt = f"""
        You can see that the id stitcher table name has changed again. It is now {id_stitcher_table_name_3} (from earlier {updated_id_stitcher_table_name} and {id_stitcher_table_name}).
        Let's check the number of distinct {entity_name}_main_ids now.
        """
        self.input_handler.display_multiline_message(prompt)
        distinct_ids_3 = self.id_stitcher_queries(entity_name, id_stitcher_table_name_3)
        final_prompt = f"""
        You can see that the number of distinct {entity_name}_main_ids is now {distinct_ids_3}.
        This is a good number for a real project. We can check the cluster sizes again to see if there are any outliers.
        """
        self.input_handler.display_multiline_message(final_prompt)
        result = self.cluster_size_analysis(entity_name, id_stitcher_table_name_3)
        cluster_description = f"""
        We should see the largest cluster having a count of {result.iloc[0]['cluster_size']} and the smallest having a count of {result.iloc[-1]['cluster_size']}
        The average cluster size is {result['cluster_size'].mean()}. 
        The top 10 clusters are as follows:
        {result.head(10)}

        We should see that the top clusters are all much smaller now, with the bad anon ids filtered out. 
        """
        self.input_handler.display_multiline_message(cluster_description)

    # def execute_pb_run(self, first_run=True):
    #     if first_run:
    #         prompt = """
    #         Now let's run the generated profiles project. The command to run is `pb run`. 
    #         You would normally do that in your terminal as a cli command.
    #         But for this guided demo, you can just enter the command and the tutorial will execute it for you.
    #         """
    #         self.input_handler.display_multiline_message(prompt)
    #     self.input_handler.get_user_input("Enter `pb run` to continue", options=["pb run"])
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


def main(fast_mode: bool):
    profile_builder = ProfileBuilder(fast_mode)
    profile_builder.run()

if __name__ == "__main__":
    # Add a 'fast' flag that sets bypass = True and also removes input() step in multi line prints
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast", help="Run the tutorial in fast mode", type=str, default='y')
    args = parser.parse_args()
    if args.fast == 'y':
        print("Fast mode is enabled. Normally, we print one line at a time, but for fast mode we stop only for user inputs. They too have defaults mostly.")
        bypass = True
    else:
        bypass = False
    main(bypass)