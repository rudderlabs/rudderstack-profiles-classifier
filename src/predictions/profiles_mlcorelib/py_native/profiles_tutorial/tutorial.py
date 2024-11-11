import pkg_resources
import logging
import re
import sys
import os
import warnings
import subprocess
import zipfile
from .input_handler import InputHandler, InputSteps
from .database_manager import DatabaseManager
from .file_generator import FileGenerator
import pandas as pd
from .config import (
    SAMPLE_DATA_DIR,
    TABLE_SUFFIX,
    ID_GRAPH_MODEL_SUFFIX,
)

from typing import Tuple
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger

# from profiles_rudderstack.reader import Reader

warnings.filterwarnings("ignore", category=UserWarning, module="snowflake.connector")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
logging.getLogger("snowflake.connector.network").setLevel(logging.ERROR)


class TutorialModel(BaseModelType):
    TypeName = "profiles_tutorial"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "fast_mode": {"type": "boolean"},
        },
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        self.fast_mode = build_spec.get("fast_mode", False)
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return TutorialRecipe(self.fast_mode)

    def validate(self) -> tuple[bool, str]:
        return super().validate()


class TutorialRecipe(PyNativeRecipe):
    def __init__(self, fast_mode: bool) -> None:
        super().__init__()
        self.profile_builder = ProfileBuilder(self.reader, fast_mode)
        self.logger = Logger("TutorialRecipe")

    def describe(self, this: WhtMaterial) -> Tuple[str, str]:
        return (
            "# Profiles Tutorial\nThis is a guided interactive tutorial on Rudderstack Profiles. This tutorial will walk through key concepts of profiles and how it works. As a part of this tutorial, we will also build a basic project with an ID Stitcher Model ultimately producing an ID Graph in your warehouse.",
            ".md",
        )

    def register_dependencies(self, this: WhtMaterial):
        pass

    def execute(self, this: WhtMaterial):
        if not os.path.exists("sample_data"):
            self.logger.info("unzipping sample data...")
            unzip_sample_data()
        self.profile_builder.run()


class ProfileBuilder:
    def __init__(self, reader, fast_mode: bool):
        self.config = {}
        self.db_manager = None
        self.input_handler = InputHandler(reader, fast_mode)
        self.file_generator = FileGenerator(fast_mode, self.input_handler)
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
        entity_name, id_types, id_graph_model = self.generate_pb_project(
            connection_name
        )
        # inputs.yaml
        self.map_tables_to_id_types(relevant_tables, id_types, entity_name)
        # profiles.yaml
        self.build_id_stitcher_model(relevant_tables, entity_name, id_graph_model)
        self.pb_runs(entity_name, id_graph_model)

    def pb_runs(self, entity_name, id_graph_model: str):
        about_pb_runs = """
            We have now defined a user entity, id types that belong to this entity, data sources from which to extract this data, and finally, a simple id stitcher model. 
            The next step is to run the project. 
            This is done by passing the `pb run` command within your CLI. Profiles will then compile the yaml code - creating a bunch of SQL files, and build a DAG based off of your configuration
            The DAG contains the sequence of SQL files that will then be executed in your warehouse.
            
            Before we perform a pb run, it is worth mentioning there is another command to produce the sql files only, without it actually running in the warehouse. This gives two advantages:
            1. If there are any errors in the yaml declaration, these can be caught here. 
            2. You can observe the generated SQL before it is run in your warehouse. 

            This optional command is `pb compile` Let's run this first to see what happens.
            Paste the `pb compile` command below.
            """
        self.input_handler.display_multiline_message(about_pb_runs)
        self.input_handler.get_user_input("Enter `pb compile`", options=["pb compile"])
        os.chdir("profiles")
        pb_compile_output = self._subprocess_run(["pb", "compile"])
        os.chdir("..")
        _ = self.explain_pb_compile_results(pb_compile_output, id_graph_model)
        about_pb_run = """
            The command to run profiles project is `pb run`. 
            You would normally do that in your terminal as a cli command.
            But for this guided demo, you can just enter the command and the tutorial will execute it for you."""
        self.input_handler.display_multiline_message(about_pb_run)
        seq_no, id_stitcher_table_name = self.prompt_to_do_pb_run(id_graph_model)
        distinct_main_ids = self.explain_pb_run_results(
            entity_name, id_stitcher_table_name
        )
        seq_no, updated_id_stitcher_table_name = self.second_run(
            distinct_main_ids, entity_name, id_graph_model, id_stitcher_table_name
        )
        self.third_run(
            entity_name,
            id_graph_model,
            id_stitcher_table_name,
            updated_id_stitcher_table_name,
            seq_no,
        )

    def explain_pb_compile_results(self, pb_compile_output, id_graph_model):
        seq_no, _ = self.parse_pb_output_text(pb_compile_output, id_graph_model)
        about_pb_compile = f"""
        The profiles project is compiled successfully. This would have created a new folder called `outputs` within the `profiles` folder. In that you should see following folder structure:
        ```
        .
        └── profiles
            ├── outputs
            │   └── prod
            │       └── seq_no
            │           └── {seq_no}
            │              └── compile
            │              
        ```
        You can check out the output directory and sql files produced. 
        This `pb compile` command actually did not run the profiles project. It only 'compiled' the project, i.e., created these sql files. Now let's move on to actually running the project.
        """
        self.input_handler.display_multiline_message(about_pb_compile)
        return seq_no

    def display_welcome_message(self):
        messages = """
        This is a guided interactive tutorial on Rudderstack Profiles. This tutorial will walk through key concepts of profiles and how it works. 
        As a part of this tutorial, we will also build a basic project with an ID Stitcher Model ultimately producing an ID Graph in your warehouse.
        This tutorial will have multiple interactive components. Most of the interaction will take place within your terminal or command line. 
        However, we will prompt you to interact with a yaml configuration directly.
        And of course, we are producing output tables and views based on a fictional business in your output schema in your warehouse that you can query directly in your DBMS console or preferred IDE.
        The goal of this tutorial is to familiarize you with the profiles product in the rudderstack platform.
        This includes details on the yaml configuration, how data unification works, what the outputs look like after a run, and how to troubleshoot and build a solid ID Graph around a user entity. 
        Our goal is that you can immediately put this knowledge to action with your own data by building your own profiles project and extending it further to unify your data around a defined entity, 
        building a c360 degree view of this entity, and putting that data into action for the benefit of your business!
        """
        print(messages)
        self.input_handler.display_multiline_message(
            "Please read through the text in detail and press Enter to continue to each next step. Press “Enter” now"
        )

        fitional_business_overview = """
        In a moment, we will seed your warehouse with fictional business data to run the profiles project on during this tutorial. (Press Enter to continue)
        The business in this tutorial is `Secure Solutions, LLC`. This fictional business sells security IOT devices as well as a security management subscription service. 
        They have a number of Shopify stores and a subscription management service, and one brick and mortar store where customers can buy security equipment and checkout at a Kiosk. 
        But their pre and post sale messaging to their current and prospective customers are limited because they do not have a great view of their customers and how they interact within their business ecosystem. 
        They also struggle to allocate marketing and campaign money across their ad platforms because they do not have a great view on the user journey and what marketing initiatives are truly successful, and what aren’t. 
        In order to improve their business messaging they have to have a solid 360 degree view of their customers so that they can send the right messages, at the right time, to the right people. 
        Additionally, in order to allocate marketing spend, they have to have solid campaign reporting that builds on this 360 view of the customer. 
        In order to do both of these, Secure Solutions has to build a solid ID Graph around their customer data.
        This tutorial will walk through how to setup a profiles project, bring in source data, and build a solid ID Graph around the customer. 
        Secure Solutions, LLC knows that they have around 319 customers. 171 of which represent known users and the remaining 148 are unknown.
        Meaning, they have not performed any sort of conversion yet."""
        self.input_handler.display_multiline_message(fitional_business_overview)

    def get_entity_name(self):
        about_entity_message = """
        We are now going to define an entity in the pb_project.yaml file around which we want to model the input data. In every company there are business artifacts that are tracked across systems for the purpose of creating a complete data picture of the artifact.
        In Profiles, this artifact is called an entity. 
        `Entities` can be as common as users, accounts, and households. But can expand to be anything that you track across systems and want to gain a complete picture of, such as campaigns, devices or accounts.
        Entities are the central concept that a profiles project is built around. The identity graphs and C360 tables will be built around an entity.
        """
        print(about_entity_message)
        return self.input_handler.get_user_input(
            "For the purpose of this tutorial, we will define a 'user' entity. Please enter `user`",
            default="user",
            options=["user"],
        )

    def get_id_types(self, entity_name):
        about_id_types = """
        You will be collecting different identifiers for this entity, such as email, phone number, user_id, anonymous_id etc.
        For your entity, some of these identifiers may represent anonymous activity while others represent known activity.

        These identifiers form digital footprints across the event tracking systems. An entity is identified by these identifiers. 
        An account may have a domain, account_id, organization_id etc. 
        A product may have sku, product_id, product_name etc.

        For our example, a user may have a few anonymous_ids, one email, and one user_id etc. Part of the profiles building process is qualifying these id values as `id_types`.
        And in a subsequent step, we will do a mapping exercise where we map specific columns from your input tables to these id_types connected to the entity defined above.

        Best practices for defining id_types for an entity is that the values need to be unique to a single instance of your entity. 
        When picking `id_types` consider the granularity of the `entity`. At the user grain, you will want to pick a unique `id_type` of the same grain.
        For higher level grains such as organization or account, you can include user level grain `id_type` as well as org level `id_type`.
        
        Sometimes, these ids may also be very transient, like a session id or an order_id. As long as they uniquely map to one entity (ex: user), you can use them.
        For example, an order_id is likely won't be shared by two users, so it can uniquely identify a user, even if we typically associate order_id with an order.
        This gets very useful when we join data from different systems such as payment systems, marketing campaign systems, order management systems etc.

        In this tutorial, we are going to define a pre-set list of id_types that belong to the customers (user entity) of Secure Solutions, LLC.
        
        We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested.
        """
        self.input_handler.display_multiline_message(about_id_types)
        id_types = self.input_handler.guide_id_type_input(entity_name)

        conclusion = """
        We have now defined an entity called "user" along with the associated id_types that exist across our different source systems. 
        Now, let's move onto bringing in our data sources in order to run the ID Stitcher model and output an ID Graph."""
        print(conclusion)
        return id_types

    def connect_to_snowflake(self):
        connection_messages = """
        Profiles is a SQL based tool that's setup using a yaml configuration. 
        We will walk you through a basic profiles yaml configuration in order to generate the SQL that will run in your warehouse. 
        But first, we must build a connection between your configuration and your warehouse.
        RudderStack Profiles writes all the output tables and views to a single schema and database. 
        But the inputs can come from multiple databases and schemas.
        This is a very common scenario, given that your warehouse data can come from different sources and systems. 
        Ex: you may have event stream data sitting in RudderStack database with schemas - mobile, web, server etc. Then Salesforce data may be in a different database called `SalesforceDB` and schema called `Salesforce`. 
        Profiles can ingest data from any number of different input databases and schemas as long as the user has access to them.
        For this demo though, we are ingesting the seed data into the same database and schema as the output. So you can create a sample schema in your warehouse and use that if you want to keep all your existing schemas clean.
        We recommend creating a new schema for this purpose - a helpful name would be `profiles_tutorial`. If you prefer creating a new schema, please do that now and come back."""
        self.input_handler.display_multiline_message(connection_messages)
        if self.fast_mode:
            # get password from user
            password = self.input_handler.get_user_input(
                "Enter password for profiles_demo user", password=True
            )
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
                "output_schema": inputs[InputSteps.OUTPUT_SCHEMA],
            }
        self.db_manager = DatabaseManager(
            self.config, self.input_handler, self.fast_mode
        )

    def upload_sample_data(self):
        logger.info(
            "Now, let's seed your warehouse with the sample data from Secure Solutions, LLC"
        )
        logger.info(
            f"We will add a suffix `{TABLE_SUFFIX}` to the table names to avoid conflicts and not overwrite any existing tables."
        )
        return self.db_manager.upload_sample_data(SAMPLE_DATA_DIR, TABLE_SUFFIX)

    def find_relevant_tables(self, new_table_names):
        logger.info(
            f"\n** Searching the `{self.config['input_schema']}` schema in `{self.config['input_database']}` database for uploaded sample tables, that will act as sources for profiles **\n"
        )
        try:
            relevant_tables = self.db_manager.find_relevant_tables(new_table_names)
            if not relevant_tables:
                logger.error(
                    "No relevant tables found. Please check your inputs and try again."
                )
                sys.exit(1)
            logger.info(
                f"Found {len(relevant_tables)} relevant tables: {relevant_tables}"
            )
            return relevant_tables
        except Exception as e:
            logger.exception(f"An error occurred while fetching tables: {e}")
            sys.exit(1)

    def map_tables_to_id_types(self, relevant_tables, id_types, entity_name):
        about_input_definitions = """
        Now, we need to bring in the warehouse tables that have the relevant data for us to build an id graph off of. For this part of the tutorial, we will be updating the inputs.yaml file we mentioned above.
        The beauty of profiles is that it can ingest any table in your warehouse, whether it's collected by Rudderstack or not. For the tutorial, we will keep it simple and define 3 event sources coming from Secure Solutions, LLC's Shopify stores.
        1. PAGES: this is an event table that tracks all behavior (including anonymous activity)  across their stores. 
        2. TRACKS: This event table tracks a specific event along with user information about that event. The event tracked in this table is an ORDER_COMPLETED event. 
        3. IDENTIFIES: Users can sign up for a newsletter from the business. This event captures that event along with the users anonymous id and email. Thereby linking their anonymous activity to a specific email address.
        Each table will have its own input definition and we will define those here. This is also where we do the mapping exercise mentioned above. 
        Within each definition, we will map specific columns to the user entity's ID Types defined in the pb_project.yaml 
        We want to tell profiles exactly what columns to select in each table and map those columns to the id_types that we defined on the entity level in the pb_project.yaml file. 
        Note, the column names of the input tables are irrelevant. 
        What is relevant is what the data in each column represents and that you map the specific columns to the associated id_types. 
        For example, you will notice that the anonymous_id in the input tables is called anonymous_id. 
        But the id type we defined in the pb project yaml is anon_id. So we need to connect those two for each identifier column in each input.
        """
        self.input_handler.display_multiline_message(about_input_definitions)

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
                    logger.info("You are already at the first table.")
            else:
                if table_mappings:
                    id_mappings[table] = {
                        "mappings": table_mappings,
                        "full_table_name": f"{self.config['input_database']}.{self.config['input_schema']}.{table}",
                    }
                table_index += 1
        self.file_generator.create_inputs_yaml(id_mappings)
        print(
            "Perfect! You can now examine your inputs.yaml file and see the input definitions."
        )

    def build_id_stitcher_model(self, table_names, entity_name, id_graph_model):
        id_stitcher_spec = """
        Now, let's define an ID Stitcher Model. This model type will generate SQL which will run in your data warehouse and ultimately output an ID Graph. 
        Let's first define a name for the model. This will also be the name of the output view within your warehouse after the run is completed."""
        self.input_handler.display_multiline_message(id_stitcher_spec)
        id_graph_model_name = self.input_handler.get_user_input(
            f"Enter a name for the model, Let's give the name `{id_graph_model}`",
            options=[id_graph_model],
            default=id_graph_model,
        )
        id_stitcher_spec2 = """
        The id stitcher model has a concept known as an “edge”. An Edge is simply a direct connection between two nodes. 
        In profiles, a single node is a combination of your id value, and id type. So a full edge record would have 2 nodes.
        One side will have 1 id value and its associated id type. The other side will have another id value and its associated id type. 
        The record itself is considered an edge. Which is the connection between the 2 nodes. Within the ID Stitcher Model, we want to define the edge sources, which are the data tables we will access in order to extract the edges to build your ID Graph. 
        The edge sources come from your defined input sources, which we define in inputs.yaml file. These point to a table in the warehouse, but they can be referred with a different name within profiles project. 
        The "input model name" is different from the warehouse table name. In our demo, we give a `rs` prefix to the model names. You can see that in the inputs.yaml file too.       
        Since we have already defined our input sources, we can refer to them directly in the model spec here. Let's add our edge sources:
        """
        edge_sources = []
        self.input_handler.display_multiline_message(id_stitcher_spec2)
        for table in table_names:
            table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
            edge_source = self.input_handler.get_user_input(
                f"Enter `inputs/{table_name}` as an edge source",
                options=[f"inputs/{table_name}"],
                default=f"inputs/{table_name}",
            )
            edge_sources.append(edge_source)
        self.file_generator.create_profiles_yaml(
            entity_name, edge_sources, id_graph_model_name
        )
        print(
            "Perfect! You can now examine your profiles.yaml file and see the model spec for the ID Stitcher."
        )

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
        A profiles project contains a few yaml files, of following structure:
        ```
        .
        └── <project_directory>
            ├── pb_project.yaml
            └── models
                └── inputs.yaml
                └── profiles.yaml
        
        Here's a brief description of what each file is:

        - `pb_project.yaml`: This file contains the project declaration. The name of the project, entity, and the entity's defined id types etc.
            It also includes the warehouse connection name - `{connection_name}`, which calls the connection config we created in the previous step (the same name as in siteconfig.yaml). 
        - `models/inputs.yaml`: This file will contain the input data sources - the tables and columns that map to the entity and their id types. We will explain this in more detail in the subsequent steps.
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
        id_graph_model = f"{entity_name}_{ID_GRAPH_MODEL_SUFFIX}"
        id_types = self.get_id_types(entity_name)
        self.file_generator.create_pb_project(
            entity_name, id_types, connection_name, id_graph_model
        )
        print(
            "We have updated the pb_project.yaml file with this info. Please check it out."
        )
        return entity_name, id_types, id_graph_model

    def _subprocess_run(self, args):
        response = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        assert (
            response.returncode == 0
        ), f"Command {args} failed with error: {response.stderr}"
        return response.stdout

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

    def prompt_to_do_pb_run(self, id_graph_name: str, command: str = "pb run"):
        self.input_handler.get_user_input(
            f"Enter `{command}` to continue", options=[command]
        )
        logger.info("Running the profiles project...(This will take a few minutes)")
        os.chdir("profiles")
        pb_run_output = self._subprocess_run(command.split())
        os.chdir("..")
        seq_no, id_graph_table_name = self.parse_pb_output_text(
            pb_run_output, id_graph_name
        )
        logger.info("Done")
        return seq_no, id_graph_table_name

    # def get_latest_id_stitcher_table_name(self, entity_name: str) -> str:
    #     id_stitchers = []
    #     id_stitcher_model_name = f"{entity_name}_id_graph"
    #     for folder in os.listdir("profiles/output/prod/seq_no/latest/run"):
    #         if id_stitcher_model_name in folder:
    #             id_stitchers.append(folder)
    #     if len(id_stitchers) == 1:
    #         return id_stitchers[0]
    #     else:
    #         creation_time = 0
    #         for id_stitcher in id_stitchers:
    #             t = os.path.getctime(f"profiles/output/prod/seq_no/latest/run/{id_stitcher}")
    #             if t > creation_time:
    #                 creation_time = t
    #                 id_stitcher_table_name = id_stitcher
    #         return id_stitcher_table_name

    def explain_pb_run_results(self, entity_name: str, id_stitcher_table_name: str):
        prompt = f"""
        Congrats! You have completed your first profiles run.
        This should have created multiple tables in your warehouse account, and a new seq_no folder in the profiles/output folder - just like the prev output of `pb compile`. 
        These sql files were actually executed on your warehouse now. That's the difference from the compile command. Every new `pb run` or `pb compile` creates a new seq_no folder.

        Lets observe the output ID Graph produced. The table that was created, which we will query now is {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name}.        
        There are three key columns in this table, along with some other timestamp meta-data columns we can ignore for now:
            - `{entity_name}_main_id` - This is an id that Profiles creates. It uniquely identifies an entity (ex: user) across systems.
            - `other_id` - These are the id value extracted from the edge sources stitched to the user_main_id
            - `other_id_type` - the type of id from the original table. These are all the id_types you had provided earlier.
        
        So in this table, we can expect multiple rows for a unique {entity_name}, all sharing the same {entity_name}_main_id. But other_id + other_id_type should be unique. 

        You can sample the data in this table by running following query in your warehouse:

        `SELECT * FROM {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name} LIMIT 10;`
        """
        self.input_handler.display_multiline_message(prompt)

        distinct_ids = self._explain_first_run(entity_name, id_stitcher_table_name)
        return distinct_ids

    def id_stitcher_queries(self, entity_name: str, id_stitcher_table_name: str) -> int:
        query_distinct_main_ids = f"select count(distinct {entity_name}_main_id) from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name}"
        # logger.info("You can check the total number of entities in your project by running the following query in your warehouse account:")
        print(query_distinct_main_ids)
        # run query
        result = self.db_manager.connector.run_query(query_distinct_main_ids)
        return result[0][0]

    def _explain_first_run(self, entity_name: str, id_stitcher_table_name: str):

        user_main_ids_query = f"""
        select {entity_name}_main_id, 
                count(*) as num_of_other_ids 
        from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name} 
        group by 1 order by 2 desc"""

        intro_text = f"""
        Now, let's perform some QA on the ID graph and verify the output. 
        As mentioned above, there is a column called `{entity_name}_main_id` which is supposed to represent a unique key for a unique user with multiple IDs.
        Let's get a count of the other ids per main id to see what we have. Remember, Secure Solutions, LLC is expecting roughly 319 distinct users. 
        171 of those being identified (or known) users who have either signed up for the newsletter or have completed an order.
        Those user_main_ids should have a unique email, user_id, and shopify_customer_id associated with it. Any {entity_name}_main_id that solely has anonymous_ids would represent unknown users.
        Here's the query we will run: 
        """
        self.input_handler.display_multiline_message(intro_text)
        print(user_main_ids_query)
        user_main_ids = self.db_manager.connector.run_query(
            user_main_ids_query, output_type="pandas"
        )
        distinct_ids = len(user_main_ids)
        assert distinct_ids == 3
        alerting_text = f"""
        Oh no!! Something looks off here. It appears there are only 3 {entity_name}_main_ids. Let's bring in the id_type column and aggregate again so we can get a count per id_type, per {entity_name}_main_id.
        """
        self.input_handler.display_multiline_message(alerting_text)

        query = f"""
        select {entity_name}_main_id, 
               other_id_type, count(*) as cnt 
        from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name} 
        group by 1,2 
        order by 1,3"""
        print(query)

        result = self.db_manager.connector.run_query(query, output_type="pandas")
        result.columns = [col.lower() for col in result.columns]
        logger.info(result)

        prompt = f"""
        Notice the count of user_ids and emails within each user_main_id where you can see that there are many users. That means, these users have been merged in error. 
        But interestingly, there's a single shopify_store_id per main_id
        This is an important clue. A shopify_store_id is not something that we associate at a {entity_name} level, but instead at a shop/store level.
        You may want to bring this into a user 360 view as a feature, but it should not be involved in the stitching process in any way since the values are not meant to be unique for a user.
        Adding this in the id stitcher has resulted in over-stitching of the {entity_name}s. 
        Now let's fix this. We need to make two changes to the project files. First in pb_project.yaml, and second in inputs.yaml.
        In `pb_project.yaml`, let's remove `shopify_store_id` as an id_type. This is in two places - 
            - under entities -> id_types
            - under id_types
        In our inputs.yaml we need to remove the id mappings we previously created for the shopify_store_id. You can remove the select, type, entity key/values 
        from the PAGES and IDENTIFIES input definitions underneath the `ids:` key in each definition. 
        """
        self.input_handler.display_multiline_message(prompt)
        self.input_handler.get_user_input(
            "Enter 'done' to continue once you have made the changes", options=["done"]
        )
        # Validate whether the new yaml files have shopify_store_id
        while not self.file_generator.validate_shopify_store_id_is_removed():
            logger.info(
                "Please make sure to remove shopify_store_id from pb_project.yaml, inputs.yaml and try again"
            )
            self.input_handler.get_user_input(
                "Enter 'done' to continue once you have made the changes",
                options=["done"],
            )
        return distinct_ids

    def cluster_size_analysis(self, entity_name: str, id_stitcher_table_name: str):
        query = f"""
                select {entity_name}_main_id, count(*) as num_of_other_ids 
                from {self.config['output_database']}.{self.config['output_schema']}.{id_stitcher_table_name} 
                group by 1 
                order by 2 desc 
                """
        logger.info(query)
        res = self.db_manager.connector.run_query(query, output_type="pandas")
        res.columns = [col.lower() for col in res.columns]
        return res

    def second_run(
        self,
        distinct_ids: int,
        entity_name: str,
        id_graph_model: str,
        id_stitcher_table_name: str,
    ):
        logger.info("Now let's run the updated profiles project")
        seq_no, updated_id_stitcher_table_name = self.prompt_to_do_pb_run(
            id_graph_model
        )
        edge_table = f"{self.config['output_database']}.{self.config['output_schema']}.{updated_id_stitcher_table_name}_internal_edges"
        # updated_id_stitcher_table_name = self.get_latest_id_stitcher_table_name(entity_name)
        second_run_prompt = f"""
        Now that the second full run is complete lets QA the outputs. 
        Optionally, you can inspect and see that profiles appended an additional seq_no directory in the outputs folder on this run. 
        This directory has the newly generated sql based on your edited configuration. Because we did not build any new models, the sql is very similar. 
        You will notice though, the hash number for the models are now different ({updated_id_stitcher_table_name} vs {id_stitcher_table_name}). 
        That is because we edited the source data for the id_stitcher model. If you add, delete, or update any of the configuraton for a model, pb will generate a new hash for that model and run it fresh.
        Representing the fact that, though it is the same model type running again, the actual SQL will be slightly different due to the updated configuration. 
        Since we are already familiar with the contents of the ID Graph, let's go ahead and aggregate this table and see how many user_main_ids we get this time."""
        self.input_handler.display_multiline_message(second_run_prompt)

        # prompt = f"""
        # You can see that the id stitcher table name has changed. It is now {updated_id_stitcher_table_name} (from earlier {id_stitcher_table_name}).
        # Notice how the hash has changed. This is because we removed shopify_store_id as an id_type, and hence the graph is different.
        # The hash is a fingerprint of the definition, so it changes if the profiles project changes.
        # What's the number of distinct {entity_name}_main_ids now?
        # """
        # self.input_handler.display_multiline_message(prompt)
        distinct_ids_upd = self.id_stitcher_queries(
            entity_name, updated_id_stitcher_table_name
        )
        # self.top_clusters_query = f"""
        #         select {entity_name}_main_id, count(*) as cluster_size
        #         from {self.config['output_database']}.{self.config['output_schema']}.{updated_id_stitcher_table_name}
        #         group by 1
        #         order by 2 desc
        #         """
        prompt = f"""
        Interesting, the number you should see is: {distinct_ids_upd}. This is much better than the earlier number {distinct_ids}!
        It's important to note how adding a bad id type resulted in, collapsing all users into just a handful of main ids
        But bad id types aren't the only reason for over stitching. Sometimes, a few bad data points can also cause this. 
        Let's see if we have any overly large clusters within this current output. 
        Run the following query:
        """

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
        self.input_handler.display_multiline_message(prompt)
        # logger.info(self.top_clusters_query)
        logger.info(
            "You can run this query in your warehouse account to check the size of all the clusters"
        )
        result = self.cluster_size_analysis(entity_name, updated_id_stitcher_table_name)
        # result = self.db_manager.connector.run_query(self.top_clusters_query, output_type="pandas")
        print(result.head(10))
        edge_table_intro = f"""
        This is a much more healthy id graph. But it does appear that we have 5 user_main_ids that have an unusually high ID count. All of the user_main_ids below the top 5 seem to be fine. 
        Lets see how we can troubleshoot and fix this newly discovered issue. 
        We already reviewed the id types we brought in. And with the removal of the shopify store id as an id type that was not unique to a user, we know the rest are fine. 
        This means that, within one or more of the id types that remain, there are specific values that have caused additional overstitching to happen in the last run. 
        But how do we find these bad values?
        Since the ID Graph is the finished output, we will have to back up from that and see how it was constructed in order to observe what could have possibly caused additional overstitching. 
        We can do that by accessing a pre stitched table. That table is the Edges Table. The table name is `{edge_table}`.
        This table is constructed directly off of your edge sources specified within your ID Stitcher Spec. 
        The records in this table are the edges from your input tables. 
        A single record will have 2 nodes, one on each side, where the record itself represents the edge (the connection between the two nodes) 
        The table name will always be your main id graph output table with `_internal_edges` appended to it.
        Run the query below in order to see the contents of the edge table
        `select * from {edge_table} where id1 != id2 limit 100;`
        Now, let's run a more advanced query in order to analyze what id values have possibly stitched multiple users together. 
        Here, we are analyzing the count of edges for each side of the nodes using the edge table as the source. 
        """
        self.input_handler.display_multiline_message(edge_table_intro)

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
        # self.input_handler.display_multiline_message(cluster_description)

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
        dense_edges = self.db_manager.connector.run_query(
            dense_edges_query, output_type="pandas"
        )
        dense_edges.columns = [col.lower() for col in dense_edges.columns]
        print(dense_edges.head(20))
        explain_bad_anon_ids = f"""
        Secure Solutions, LLC would expect an accurate user_main_id for a known active user who has purchased a number of IoT devices to have between 15 to 25 other ids.
        A healthy user_main_id would encompass a number of device ids, a number of anonymous_ids, but only one email, shopify_customer_id, and user_id. 
        This means, the count of edges between the different id values associated with a single user should be in the same ballpark.  
        However, after analyzing the output of the above query, we can see that 5 anonymous_ids have a count of edges well over 30. 
        This is a clue to the bad values. The true test is to see if these anonymous_ids are connected to more than one email, user_id, or shopify_customer_id. 
        We will extend this query to see.
        """
        self.input_handler.display_multiline_message(explain_bad_anon_ids)
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
        bad_anons = self.db_manager.connector.run_query(
            query_investigate_bad_anons, output_type="pandas"
        )
        bad_anons.columns = [col.lower() for col in bad_anons.columns]
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        print(bad_anons.head(20))
        explain_bad_anon_ids2 = """
        We sorted the query on an id type (email) where we know there should only be one per user_main_id. 
        And we can see that each of the 5 anonymous_ids seen in the previous query have 12 emails, user_ids, and shopify_customer_is linked to them. 
        You can also expand the array column to get a full list of the other ids that were directly linked to each single id value. 
        You can see for the first anonymous_id for example, that there are many users merged together. 
        These 5 anonymous_ids are the clear culprit as to what has caused some of these users to merge. 
        Recalling the beginning of this tutorial, Secure Solutions, LLC has 1 brick and mortar store where customers can purchase IoT security devices from where they can checkout from an in store Kiosk. 
        In a realistic scenario, we could go back into our source event tables (the same ones we defined as input sources within this project) and see that the IP Address between these 5 anonymous_ids are all the same. 
        Meaning, there is a single machine that is causing these users to merge. 
        So how do we fix this? We don't want to remove anonymous_id as an id type since Secure Solutions wants to track and build features off of anonymous activity as well as known. 
        Instead, we need to filter out these specific values from being involved in the stitching process to begin with. 
        We have public docs available on the different methods for filtering, as there are multiple ways. (ref: https://www.rudderstack.com/docs/profiles/cli-user-guide/structure/#filters)
        But to wrap up this tutorial, we will go ahead and add a regex filter on the anonymous_id type. 
        This is done back in the pb_project.yaml.
        """
        self.input_handler.display_multiline_message(explain_bad_anon_ids2)
        self.input_handler.display_multiline_message(
            "Click enter so we update your `pb_project.yaml` file with the regex filter to the anonymous_id type in the background"
        )
        self.file_generator.update_bad_anons_filter()
        print(
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
        # self.input_handler.display_multiline_message(explain_bad_anon_ids)
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
        # self.input_handler.get_user_input("Update your pb_project.yaml with above changes, and enter 'done' to continue once you have made the changes", options=["done"])
        # # check the above change is done or not
        # while not self.file_generator.validate_bad_anons_are_filtered():
        #     logger.info("That doesn't look right. Please make sure the above filters are added exactly as shown above, and try again")
        #     self.input_handler.get_user_input("Update your pb_project.yaml with above changes, and enter 'done' to continue once you have made the changes", options=["done"])

    def third_run(
        self,
        entity_name: str,
        id_graph_model: str,
        id_stitcher_table_1: str,
        id_stitcher_table_2: str,
        seq_no2: int,
    ):
        # Third run
        seq_no_intro = f"""
        Now that the anon ids are excluded in the project definition, let's re-run the profiles project. 
        But we will run with a parameter, `--seq_no`. Each pb run or pb compile creates a new seq_no as you saw.
        We can do a pb run with a seq_no param, so that profiles can reuse tables that already exist in the warehouse. If some model definition has changed, only those and the downstream queries are executed on the warehouse.
        This results in a faster pb run, along with some cost savings on the warehouse. The previous run had a seq number of {seq_no2}, apparent from the id graph table name {id_stitcher_table_2}.
        We will use that seq no in the pb run."""
        self.input_handler.display_multiline_message(seq_no_intro)
        seq_no3, id_stitcher_table_name_3 = self.prompt_to_do_pb_run(
            id_graph_model, command=f"pb run --seq_no {seq_no2}"
        )
        prompt = f"""
        You can see that the id stitcher table name has changed again. It is now {id_stitcher_table_name_3} (from earlier {id_stitcher_table_2} and {id_stitcher_table_1}).
        The filter on anonymous_id resulted in the changing of the hash. But as we reused prev seq_no, the suffix seq_no did not change.
        Let's check the number of distinct {entity_name}_main_ids now.
        """
        self.input_handler.display_multiline_message(prompt)
        distinct_ids_3 = self.id_stitcher_queries(entity_name, id_stitcher_table_name_3)
        prompt = f"""
        The number of distinct {entity_name}_main_ids is now {distinct_ids_3}. Great! Remember at the beginning we expected to have 319 {entity_name}_main_ids.
        Let's quickly look at the count of other ids per {entity_name}_main_id
        """
        self.input_handler.display_multiline_message(prompt)

        result = self.cluster_size_analysis(entity_name, id_stitcher_table_name_3)
        print(result.head(20))

        prompt = f"""
        Great! We can see that even the user_main_ids with the highest count are within the threshold that we would expect. 
        Now that Secure Solutions, LLC has a solid id graph, they can unlock future major value add projects for the business!! Like building an accurate 360 degree view of their customers, predicting accurate churn scores, model marketing attribution, etc.
        """
        self.input_handler.display_multiline_message(prompt)

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


def get_sample_data_path():
    """Returns the path to the data directory"""
    return pkg_resources.resource_filename(
        "profiles_mlcorelib", "py_native/profiles_tutorial/sample_data.zip"
    )


def unzip_sample_data():
    zip_file_path = get_sample_data_path()
    # Ensure the zip file exists
    if not os.path.exists(zip_file_path):
        raise Exception(f"Error: {zip_file_path} not found.")
    # Unzip the file
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                if not file_info.filename.startswith("__MACOSX"):
                    zip_ref.extract(file_info, ".")
        print(f"Successfully extracted {zip_file_path} to the current directory")
    except Exception as e:
        raise Exception(f"An error occurred while extracting: {str(e)}")


# def main(fast_mode: bool):
#     if not os.path.exists("sample_data"):
#         print("Unzipping sample data...")
#         unzip_sample_data()
#     profile_builder = ProfileBuilder(fast_mode)
#     profile_builder.run()

# if __name__ == "__main__":
#     # Add a 'fast' flag that sets bypass = True and also removes input() step in multi line prints
#     import argparse
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--fast", help="Run the tutorial in fast mode", type=str, default='n')
#     args = parser.parse_args()
#     if args.fast == 'y':
#         print("Fast mode is enabled. Normally, we print one line at a time, but for fast mode we stop only for user inputs. Disable by running `python tutorial.py --fast n`\n")
#         bypass = True
#     else:
#         bypass = False
#     main(bypass)
