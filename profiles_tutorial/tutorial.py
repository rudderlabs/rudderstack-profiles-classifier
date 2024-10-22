import logging
import time
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
        # Connection
        self.connect_to_snowflake()
        connection_name = self.create_siteconfig()
        new_table_names = self.upload_sample_data()
        relevant_tables = self.find_relevant_tables(new_table_names)
        self.display_about_project_files(connection_name)
        # pb_project.yaml
        entity_name, id_types = self.generate_pb_project(connection_name)
        # inputs.yaml
        self.map_tables_to_id_types(relevant_tables, id_types, entity_name)

        # profiles.yaml
        self.build_id_stitcher_model(relevant_tables, entity_name)

        self.pb_runs(entity_name)

    def pb_runs(self, entity_name):
        self.prompt_to_do_pb_run(first_run=True)
        id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        #id_stitcher_table_name = "Material_user_id_stitcher_6a2cd3af_10" #FixMe: comment this line
        self.explain_pb_run_results(entity_name, id_stitcher_table_name)
        distinct_main_ids = self.id_stitcher_queries(entity_name, id_stitcher_table_name)
        self.second_run(distinct_main_ids, entity_name, id_stitcher_table_name)

    def display_welcome_message(self):
        messages = """
        This is a guided interactive tutorial on Rudderstack Profiles. This tutorial will walk through key concepts of profiles and how it works. 
        As a part of this tutorial, we will also build a basic project with an ID Stitcher Model ultimately producing an ID Graph in your warehouse.
        This tutorial will have multiple interactive components. Most of the interaction will take place within your terminal or command line. 
        However, we will prompt you to interact with a yaml configuration directly.
        And of course, we are producing output tables and views based on a fictional business in your output schema in your warehouse that you can query directly in your DBMS console or preferred IDE.
        The goal of this tutorial is to familiarize you with the profiles product in the rudderstack platform.
        This includes details on the yaml configuration, how data unification works, what the outputs look like after a run, and how to troubleshoot and build a solid ID Graph around a user entity. 
        Our goal is that you can then immediately put this knowledge to action with your own data by building your own profiles project and extending it further to unify your data around a defined entity, 
        building a c360 degree view of this entity, and putting that data into action for the benefit of your business!
        """
        print(messages)
        self.input_handler.display_multiline_message("Please read through the text in detail and press Enter to continue to each next step. Press “Enter” now")

        fitional_business_overview = """
        In a moment, we will seed your warehouse with fictional business data to run the profiles project on during this tutorial. (Press Enter to continue)
        The business in this tutorial is `Secure Solutions, LLC`. This fictional business sells security IOT devices as well as a security management subscription service. 
        They have a number of Shopify stores and a subscription management service, and one brick and motor store where customers can buy security equipment and checkout at a Kiosk. 
        But their pre and post sale messaging to their current and prospective customers are limited because they do not have a great view of their customers and how they interact within their business ecosystem. 
        They also struggle to allocate marketing and campaign money across their ad platforms because they do not have a great view on the user journey and what marketing initiatives are truly succesful, and what aren’t. 
        In order to improve their business messaging they have to have a solid 360 degree view of their customers so that they can send the right messages, at the right time, to the right people. 
        Additionally, in order to allocate marketing spend, they have to have solid campaign reporting that builds on this 360 view of the customer. 
        In order to do both of these, Secure Solutions has to build a solid ID Graph around their customer data.
        This tutorial will walk through how to setup a profiles project, bring in source data, and build a solid ID Graph around the customer. 
        Secure Solutions, LLC knows that they have around 319 customers. 171 of which represent known users and the remaining 148 are unknown"""
        self.input_handler.display_multiline_message(fitional_business_overview)
        
    def get_entity_name(self):
        about_entity_message = """
        We are now going to define an entity around which we want to model data. In every company there are business artifacts that are tracked across systems for the purpose of creating a complete data picture of the artifact.
        In Profiles, this artifact is called an entity. 
        `Entites` can be as common as users, accounts, and households. But can expand to be anything that you track across systems and want to gain a complete picture of, such as campagins, devices or accounts.
        Entities are the central concept that a profiles project is built around. The identity graphs and C360 tables will be built around an entity.
        """
        print(about_entity_message)
        return self.input_handler.get_user_input("For the purpose of this tutorial, we will define a 'user' entity. Please enter `user`", default="user", options=['user'])

    def get_id_types(self, entity_name):
        about_id_types = """
        You will be collecting different identifiers for this entity, such as email, phone number, user_id, anonymous_id etc.
        For your entity, some of these identifiers may represent anonymous activity while others represent known activity.

        These identifiers form digital footprints across the event tracking systems. An entity is identified by these identifiers. 
        An account may have a domain, account_id, organization_id etc. 
        A product may have sku, product_id, product_name etc.

        For our example, a user may have a few anonymous_ids, one email, and one user_id etc. Part of the profiles building process is qualifyign these id values as `id_types`.
        And in a subsequent step, doing a mapping execrise where we map specific columns from your input tables to these id_types connected to the entity defined above.

        Best pratices for defining id_types for an enitty is that the values need to be unique to a single instance of your entity. 
        When picking `id_types` consider the granularity of the `entity`. At the user grain, you will want to pick a unique `id_type` of the same grain.
        For higher level grains such as organization or account, you can include user level grain `id_type` as well as org level `id_type`.
        
        Some times, these ids may also be very transient, like a session id or an order_id. As long as they uniquely map to one entity (ex: user), you can use them.
        For example, an order_id is likely won't be shared by two users, so it can uniquely identify a user, even if we typically associate order_id with an order.
        This gets very useful when we join data from different systems such as payment systems, marketing campaign systems, order management systems etc.

        In this tutorial, we are going to define a pre-set list of id_types that belong to the customers (user entity) of Secure Solutions, LLC.
        
        We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested.
        """
        self.input_handler.display_multiline_message(about_id_types)
        id_types = self.input_handler.guide_id_type_input(entity_name)
        
        conclusion = """
        Great! We have now defined an entity called “user” along with the associated id_types that exist across our different source systems. 
        Now, let's move onto bringing in our data sources in order to run the ID Stitcher model and output an ID Graph"""
        print(conclusion)
        return id_types

    def connect_to_snowflake(self):
        connection_messages = """
        Profiles is a SQL based tool that's setup using a yaml configuration. 
        We will walk you through a basic profiles yaml configuration in order to generate the SQL that will run in your warehouse. 
        But first, we must build a connection between your configuration and your warehouse.
        RudderStack Profiles writes all the output tables and views to a single schema and database. 
        But the inputs can come from multiple databases and schemas.
        This is a very common scenario, given that your warehouse data come from different sources and systems. 
        Ex: you may have event stream data sitting in RudderStack database with schemas - mobile, web, server etc. Then Salesforce data may be in a different database called `SalesforceDB` and schema called `Salesforce`. 
        Profiles can ingest data from any number of different input databases and schemas as long as the user has access to them.
        For this demo though, we are ingesting the seed data into the same database and schema as the output. So you can create a sample schema in your warehouse and use that if you want to keep all your existing schemas clean.
        We recommend creating a new schema for this purpose - may be call it `profiles_tutorial`. If you prefer creating a new schema, please do that now and come back."""
        self.input_handler.display_multiline_message(connection_messages)
        if self.fast_mode:
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
            # inputs.update(self.input_handler.collect_user_inputs(InputSteps.input()))
            # inputs.update(self.input_handler.collect_user_inputs(InputSteps.output()))

            self.config = {
                "account": inputs[InputSteps.ACCOUNT],
                "user": inputs[InputSteps.USERNAME],
                "password": inputs[InputSteps.PASSWORD],
                "role": inputs[InputSteps.ROLE],
                "warehouse": inputs[InputSteps.WAREHOUSE],
                "input_database": inputs[InputSteps.OUTPUT_DATABASE],
                "input_schema": inputs[InputSteps.OUTPUT_SCHEMA],
                "output_database": inputs[InputSteps.OUTPUT_DATABASE],
                "output_schema": inputs[InputSteps.OUTPUT_SCHEMA]
            }
        self.db_manager = DatabaseManager(self.config, self.input_handler, self.fast_mode)
        
    def upload_sample_data(self):
        logger.info("Now, let's seed your warehouse with the sample data from Secure Solutions, LLC")
        logger.info(f"We will add a suffix `{TABLE_SUFFIX}` to the table names to avoid conflicts and not overwrite any existing tables.")
        return self.db_manager.upload_sample_data(SAMPLE_DATA_DIR, TABLE_SUFFIX)

    def find_relevant_tables(self, new_table_names):
        logger.info(f"\n** Searching the `{self.config['input_schema']}` schema in `{self.config['input_database']}` database for uploaded sample tables, that will act as sources for profiles **\n")
        try:
            relevant_tables = self.db_manager.find_relevant_tables(new_table_names)
            if not relevant_tables:
                logger.error("No relevant tables found. Please check your inputs and try again.")
                sys.exit(1)
            logger.info(f"Found {len(relevant_tables)} relevant tables: {relevant_tables}")
            return relevant_tables
        except Exception as e:
            logger.exception(f"An error occurred while fetching tables: {e}")
            sys.exit(1)

    def map_tables_to_id_types(self, relevant_tables, id_types, entity_name):
        about_input_definitions = """
        We need to bring in the warehouse tables that have the relevant data for us to build an id graph off of. For this part of the tutorial, we will be updating the inputs.yaml file we mentioned above.
        The beauty of profiles is that it can take any table in your warehouse, whether it's collected by Rudderstack or not. For the tutorial, we will keep it simple and define 3 event sources coming from Secure Solutions, LLC's Shopify stores.
        1. PAGES: this is an event table that tracks all behavior (including anonymous activity)  across their stores. 
        2. TRACKS: This event table tracks a specific event along with user information about that event. The event tracked in this table is an ORDER_COMPLETED event. 
        3. IDENTIFIES: Users can sign up for a newsletter from the business. This event captures that event along with the users anonymous id and email. Thereby linking their anonymous activity to a specific email address.
        Each table will have it's own input definition and we will define those here. This is also where we do the mapping exercise mention above. 
        Within each definition, we will map specific columns to the user entity's ID Types defined in the pb_project.yaml 
        We want to tell profiles exactly what columns to select in each table and map those columns to the id_types that we previously defined. 
        Note, the columns names of the input tables are irrelevant. 
        What is relevant is what the data represents and that you map it accordingly. 
        For example, you will notice that the anonymous_id in the input tables is called anonymous_id. 
        But the id type we defined in the pb project yaml is anon_id. So we need to connect those two for each identifier column in each input.
        """
        self.input_handler.display_multiline_message(about_input_definitions)

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
        self.file_generator.create_inputs_yaml(id_mappings)
        print("Perfect! You can now examine your inputs.yaml file and see the input definitions.")
    
    def build_id_stitcher_model(self, table_names, entity_name):
        id_stitcher_spec = """
        Now, let's define an ID Stitcher Model. This model type will generate SQL which will run in your data warehouse and ultimately output an ID Graph. 
        Let's first define a name for the model. This will also be the name of the output view within your warehouse after the run is completed."""
        self.input_handler.display_multiline_message(id_stitcher_spec)
        id_graph_model_name = self.input_handler.get_user_input("Enter a name for the model, Let's give the name `user_id_graph`", options=["user_id_graph"])
        id_stitcher_spec2 = """
        The id stitcher model has a concept known as an “edge”. An Edge is simply a direct connection between two nodes. 
        In profiles, a single node is a combination of your id value, and id type. So a full edge record would have 2 nodes.
        One side will have 1 id value and its associated id type. The other side having another id value and its associated id type. 
        The record itself is considered an edge. Which is the connection between the 2 nodes. Within the ID Stitcher Model, we want to define the edge sources, which are the data tables we will access in order to extract the edges to build your ID Graph. 
        The edge sources come from input sources, which we define in inputs.yaml file. These point to a table in the warehouse, but they can be referd with a different name within profiles project. 
        The "input model name" is different from the warehouse table name. In our demo, we give a `rs` prefix to the model names. You can see that in the inputs.yaml file too.       
        Since we have already defined our inputs sources, we can refer to them directly in the model spec here. Let's add our edge source:
        """
        edge_sources = []
        self.input_handler.display_multiline_message(id_stitcher_spec2)
        for table in table_names:
            table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
            edge_source = self.input_handler.get_user_input(f"Enter `inputs/{table_name}` as an edge source", options=[f"inputs/{table_name}"])
            edge_sources.append(edge_source)
        self.file_generator.create_profiles_yaml(entity_name, edge_sources, id_graph_model_name)
        print("Perfect! You can now examine your profiles.yaml file and see the model spec for the ID Stitcher.")
        
    def create_siteconfig(self):
        about_siteconfig = """
        We store the connection information in a file called `siteconfig.yaml`.
        This file is stored in a folder called `.pb` in your home directory. 
        This helps profile builder to connect to your warehouse account and run the project automatically.
        """
        print(about_siteconfig)
        return self.file_generator.create_siteconfig(self.config)
            
    def display_about_project_files(self, connection_name):
        about_files = f"""
        Now let's create a profiles project. 
        A profiles project contains mainly a few yaml files, of following structure:
        ```
        .
        └── <project_directory>
            ├── pb_project.yaml
            └── models
                └── inputs.yaml
                └── profiles.yaml
        
        Here's a brief description of what each file is:

        - `pb_project.yaml`: This file contains the project declaration - name of the project, with entity info, their id types etc. 
            It also includes the warehouse connection name - `{connection_name}`, which calls the connection config we created in the previous step (the same name as in siteconfig.yaml). 
        - `models/inputs.yaml`: This file will contain the input data sources - the tables and columns that map to the entities and their id types. We will explain this in more detail in the subsequent steps.
        - `models/profiles.yaml`: This is where we define the model configurations for the id stitcher and any features/traits you want to build for your defined entity. For the tutorial, we will only build an ID Graph using the ID Stitcher Model Type. 

        These files will be created in this tutorial, with the details you will provide in the next steps. 
        Also, for this tutorial, we will use a directory called `profiles` to store all the files. We will create it here in the current directory.
        """
        self.input_handler.display_multiline_message(about_files)

    def generate_pb_project(self, connection_name):
        # create a directory called profiles if doesn't exist
        if not os.path.exists("profiles"):
            os.makedirs("profiles")
        if not os.path.exists("profiles/models"):
            os.makedirs("profiles/models")
        entity_name = self.get_entity_name()
        id_types = self.get_id_types(entity_name)
        self.file_generator.create_pb_project(entity_name, id_types, connection_name)
        print("We have updated the pb_project.yaml file with this info. Please check it out.")
        return entity_name, id_types

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
        id_stitcher_model_name = f"{entity_name}_id_graph"
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
            |                 └── Material_{entity_name}_id_graph_<hash>_1.sql
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
        print("Fast mode is enabled. Normally, we print one line at a time, but for fast mode we stop only for user inputs.")
        bypass = True
    else:
        bypass = False
    main(bypass)