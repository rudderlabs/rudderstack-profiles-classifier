import logging
import sys
import warnings
from input_handler import InputHandler, InputSteps
from database_manager import DatabaseManager
from file_generator import FileGenerator
from config import SAMPLE_DATA_DIR, TABLE_SUFFIX
warnings.filterwarnings("ignore", category=UserWarning, module="snowflake.connector")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
# Set higher logging level for snowflake connector
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
        
        self.file_generator.create_pb_project(entity_name, id_types, connection_name)
        self.file_generator.create_inputs_yaml(id_mappings)
        self.file_generator.create_profiles_yaml(entity_name, id_mappings.keys())

def main():
    profile_builder = ProfileBuilder()
    profile_builder.run()

if __name__ == "__main__":
    main()