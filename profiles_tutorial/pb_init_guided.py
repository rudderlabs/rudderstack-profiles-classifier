import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import re
from pathlib import Path
from ruamel.yaml import YAML
import getpass
from datetime import datetime
import pandas as pd

SAMPLE_DATA_DIR = "sample_data"
def connect_to_snowflake():
    common_steps = [
        "Account (ex: abc12345.us-east-1): ",
        "Username: ",
        "Password: ",
        "Role: ",
        "Warehouse: "
    ]

    input_steps = [
        "Input Database name (where the source data is - for this demo, we will upload sample data to this database/schema - so you can choose any existing database/schema): ",
        "Input Schema name (where the source data is): "
    ]

    output_steps = [
        "Output Database name (where the output profiles data will be stored): ",
        "Output Schema name (where the output profiles data will be stored): "
    ]

    inputs = {}
    current_step = 0
    total_steps = len(common_steps) + len(input_steps) + len(output_steps)

    # Collect common connection details
    while current_step < len(common_steps):
        prompt = common_steps[current_step]
        
        if "Password" in prompt:
            user_input = get_user_input(prompt, password=True)
        else:
            user_input = get_user_input(prompt)
        
        if user_input.lower() == 'back':
            if current_step > 0:
                current_step -= 1
            else:
                print("You are already at the first step.")
        else:
            inputs[current_step] = user_input
            current_step += 1

    # Collect input connection details
    while current_step < len(common_steps) + len(input_steps):
        prompt = input_steps[current_step - len(common_steps)]
        user_input = get_user_input(prompt)
        
        if user_input.lower() == 'back':
            if current_step > 0:
                current_step -= 1
            else:
                print("You are already at the first step.")
        else:
            inputs[current_step] = user_input
            current_step += 1

    # Collect output connection details
    while current_step < total_steps:
        prompt = output_steps[current_step - len(common_steps) - len(input_steps)]
        user_input = get_user_input(prompt)
        
        if user_input.lower() == 'back':
            if current_step > len(common_steps):
                current_step -= 1
            else:
                print("You are already at the first step.")
        else:
            inputs[current_step] = user_input
            current_step += 1

    print("\nPlease review the details you entered:")
    print("\n** Common Connection Details **")
    print(f"Account: {inputs[0]}")
    print(f"Username: {inputs[1]}")
    print(f"Role: {inputs[3]}")
    print(f"Warehouse: {inputs[4]}")

    print("\n** Input Connection Details (from where event-stream data will be read) **")
    print(f"Input Database: {inputs[5]}")
    print(f"Input Schema: {inputs[6]}")

    print("\n** Output Connection Details (where the profiles output data will be stored) **")
    print(f"Output Database: {inputs[7]}")
    print(f"Output Schema: {inputs[8]}")

    confirm = input("\nAre these details correct? (yes/no): ").strip().lower()
    if confirm == 'no':
        print("\nLet's try again.\n")
        return connect_to_snowflake()

    try:
        conn = snowflake.connector.connect(
            account=inputs[0],
            user=inputs[1],
            password=inputs[2],
            role=inputs[3],
            warehouse=inputs[4]
        )
    except Exception as e:
        print(f"Failed to establish connection: {e}")
        exit(1)

    config = {
        "input_database": inputs[5],
        "input_schema": inputs[6],
        "output_database": inputs[7],
        "output_schema": inputs[8],
    }

    return conn, config, inputs[2], inputs[0]

def upload_sample_data(conn, input_database, input_schema):
    new_table_names = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in os.listdir(SAMPLE_DATA_DIR):
        if filename.endswith(".csv"):
            base_name = os.path.splitext(filename)[0]
            table_name = f"{base_name}_{timestamp}"
            new_table_names[filename] = table_name
            df = pd.read_csv(os.path.join(SAMPLE_DATA_DIR, filename))

            success, _, _, _ = write_pandas(conn, 
                                                      df, 
                                                      table_name=table_name, 
                                                      database=input_database,
                                                      schema=input_schema,
                                                      quote_identifiers=False,
                                                      auto_create_table=True)
            if not success:
                print(f"Failed to upload {filename}")
                exit(1)
    return new_table_names


def find_relevant_tables(conn, input_database, input_schema, new_table_names):
    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES IN SCHEMA {input_database}.{input_schema}")
    tables = cursor.fetchall()
    relevant_tables = [table[1] for table in tables if table[1].lower() in new_table_names.values()]
    return relevant_tables

def get_columns(conn, input_database, input_schema, table):
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESCRIBE TABLE {input_database}.{input_schema}.{table}")
        columns = cursor.fetchall()
    finally:
        cursor.close()
    return [col[0] for col in columns]

def suggest_id_mappings(columns, id_types):
    suggestions = {id_type: [] for id_type in id_types}
    
    for column in columns:
        for id_type in id_types:
            # Special case for exact 'id' column
            if column.lower() == 'id':
                suggestions[id_type].append(column)
                continue
            # Split id_type and column by underscores
            id_type_parts = [part for part in id_type.lower().split('_') if part != 'id']
            column_parts = [part for part in column.lower().split('_') if part != 'id']
            
            # Check for matches
            if any(
                (id_part in col_part or col_part in id_part) and len(id_part) > 2 and len(col_part) > 2
                for id_part in id_type_parts
                for col_part in column_parts
            ):
                suggestions[id_type].append(column)
    
    return suggestions



def get_user_input(prompt, options=None, password=False):
    while True:
        full_prompt = f"{prompt}\n> "
        if password:
            user_input = getpass.getpass(full_prompt).strip()
        else:
            user_input = input(full_prompt).strip()
        if options is None or user_input in options:
            return user_input
        print("Invalid input. Please try again.")

def create_siteconfig(conn, output_db, output_schema,  password, account):
    print("""We need to save the warehouse credentials in a file called `siteconfig.yaml`. 
          This file is stored in a folder called `.pb` in your home directory. 
          This helps profile builder to connect to your warehouse account and run the project automatically.""")
    
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)

    home_dir = str(Path.home())
    pb_dir = os.path.join(home_dir, ".pb")
    os.makedirs(pb_dir, exist_ok=True)
    siteconfig_path = os.path.join(pb_dir, "siteconfig.yaml")

    existing_siteconfig = {}
    if os.path.exists(siteconfig_path):
        print(f"Found existing siteconfig.yaml file at {siteconfig_path}. We will append the credentials to the existing file.")
        with open(siteconfig_path, "r") as f:
            existing_siteconfig = yaml.load(f) or {} # yaml.safe_load(f) or {}

    connection_name = "test"
    while connection_name in existing_siteconfig.get("connections", {}):
        replace = get_user_input(f"Connection '{connection_name}' already exists. Should we replace it? (yes/no): ", ["yes", "no"])
        if replace.lower() == "yes":
            break
        connection_name = get_user_input("Enter a new connection name: ")

    new_connection = {
        connection_name: {
            "target": "prod",
            "outputs": {
                "prod": {
                    "account": account,
                    "dbname": output_db,
                    "password": password,  
                    "role": conn.role,
                    "schema": output_schema,
                    "type": "snowflake",
                    "user": conn.user,
                    "warehouse": conn.warehouse
                }
            }
        }
    }

    if "connections" not in existing_siteconfig:
        existing_siteconfig["connections"] = {}
    existing_siteconfig["connections"][connection_name] = new_connection[connection_name]

    with open(siteconfig_path, "w") as f:
        yaml.dump(existing_siteconfig, f)

    return connection_name  

def create_pb_project(entity_name, id_types, connection_name):
    pb_project = {
        "name": "demo_project",
        "schema_version": 72,
        "connection": connection_name,
        "model_folders": ["models"],
        "entities": [
            {
                "name": entity_name,
                "id_stitcher": f"models/{entity_name}_id_stitcher",
                "id_types": id_types,
                "feature_views": {
                    "using_ids": [
                        {"id": id_type, "name": f"with_{id_type}"} for id_type in id_types
                    ]
                }
            }
        ],
        "id_types": [{"name": id_type} for id_type in id_types]
    }
    
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 4096  # Prevent line wrapping
    
    with open("profiles/pb_project.yaml", "w") as f:
        yaml.dump(pb_project, f)

def create_inputs_yaml(id_mappings, entity_name):
    inputs = {
        "inputs": [
            {
                "name": f"rs{table.capitalize()}",
                "app_defaults": {
                    "table": mapping["full_table_name"],
                    "occurred_at_col": "timestamp",
                    "ids": [
                        {
                            "select": column,
                            "type": id_type,
                            "entity": entity_name
                        }
                        for id_type, columns in mapping["mappings"].items()
                        for column in columns
                    ]
                }
            }
            for table, mapping in id_mappings.items()
        ]
    }
    
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 4096  # Prevent line wrapping
    
    with open("profiles/models/inputs.yaml", "w") as f:
        yaml.dump(inputs, f)

def create_profiles_yaml(entity_name, tables):
    profiles = {
        "models": [
            {
                "name": f"{entity_name}_id_stitcher",
                "model_type": "id_stitcher",
                "model_spec": {
                    "validity_time": "24h",
                    "entity_key": entity_name,
                    "edge_sources": [
                        {"from": f"inputs/rs{table.capitalize()}"}
                        for table in tables
                    ]
                }
            }
        ]
    }
    
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 4096  # Prevent line wrapping
    
    with open("profiles/models/profiles.yaml", "w") as f:
        yaml.dump(profiles, f)

def main():
    print("""\n
          This is a guided tutorial on RudderStack Profiles. 
          This involves details on key concepts of Profiles. You will also build a basic profiles project as you go through this tutorial.
          Please read through the text in detail and press Enter to continue to the next step. It asks you to enter a few details in between.
          At any point, you can type "back" to go back and modify your previous input. (Press Enter to continue)""")
    input("")
    
    about_profiles = """A profiles project is about building a unified view of an entity. This involves primarily two steps. (Press Enter to continue)
    1. Creating an entity (Press Enter to continue)
    2. Creating different features for the entity (Press Enter to continue.. Do this for each step going forward)
    An `entity` is a distinct object of interest for you. It cound be a user, an organisation, account, or even product. 
    So, to get started, we need to know a bit about the entity you are trying to build profiles for.
    """
    # print one line at a time, next line prints after user presses enter
    for line in about_profiles.split("\n"):
        print(line)
        input("")

    entity_name = get_user_input("What do you like to call your primary entity? (ex - user, customer, account etc)")

    about_id_types = """You would be collecting different identifiers for this entity, such as email, phone number, user_id, anonymous_id etc.
    These identifiers form digital footprints across your event tracking systems. An entity is identified by these identifiers. 
    For example, a user may have an anonymous_id, email, phone_number, user_id etc. Each of these are called id_types.
    An account may have a domain, account_id, organisation_id etc. 
    A product may have sku, product_id, product_name etc.
    """
    for line in about_id_types.split("\n"):
        print(line)
        input("")
    
    print("You can add more id_types later, but for now, let's get started with the most important ones - typically seen in .")
    id_types = []

    while True:
        id_type = get_user_input(f"Enter an id type associated with {entity_name} (or 'done' if finished): ")
        if id_type.lower() == 'done':
            break
        if id_type:
            id_types.append(id_type)

    print(f"\nGreat. So, the id types associated with entity {entity_name} are: {', '.join(id_types)}")
    about_id_types_advanced = """Some times, these ids may also be very transient, like a session id or an order_id. As long as they uniquely map to one entity (ex: user), you can use them.
    For example, an order_id is likely won't be shared by two users, so it can uniquely identify a user, even if we typically associate order_id with an order.
    This gets very useful when we join data from different systems such as payment systems, marketing campaign systems, order management systems etc.
    But for this guided demo, we will stick to the most important id_types.
    """
    for line in about_id_types_advanced.split("\n"):
        print(line)
        input("")

    print("We will now search your warehouse account for tables and columns that are relevant for building profiles for this entity.")
    print("Please provide the necessary details for connecting to your warehouse.")

    conn, config, password, account = connect_to_snowflake()
    input_db = config["input_database"]
    input_schema = config["input_schema"]
    output_db = config["output_database"]
    output_schema = config["output_schema"]
    print("\nUploading sample data to your warehouse account. We will add a suffix to the table names to avoid conflicts and not overwrite any existing tables.")
    new_table_names = upload_sample_data(conn, input_db, input_schema)
    print("\nSample data has been uploaded to the following tables:")
    for original_name, new_name in new_table_names.items():
        print(f"\t{original_name} -> {new_name}")

    print(f"\n** Searching the `{input_schema}` schema in `{input_db}` database for uploaded sample tables, that will act as sources for profiles **\n")

    try:    
        relevant_tables = find_relevant_tables(conn, input_db, input_schema, new_table_names)
    except Exception as e:
        print(f"Error fetching tables: {e}")
        exit(1)
    if not relevant_tables:
        print("No relevant tables found. Please check your inputs and try again.")
        exit(1)

    print("Found following tables:")
    for table in relevant_tables:
        print(f"\t{table}")
    
    id_mappings = {}
    for table in relevant_tables:
        try:
            columns = get_columns(conn, input_db, input_schema, table)
        except Exception as e:
            print(f"Error fetching columns for {table}: {e}")
            continue

        suggestions = suggest_id_mappings(columns, id_types)
        
        print(f"\nLet's map columns in the `{table}` table to our id types:")
        table_mappings = {}
        id_is_included = False
        for id_type in id_types:
            prompt = f"\nEnter columns for {id_type} (comma-separated, or press Enter if none): "
            if suggestions[id_type]:
                if not id_is_included and 'id' in suggestions[id_type]:
                    # remove id from suggestions[id_type]
                    suggestions[id_type].remove('id')
                prompt += f"\nSuggestions: {', '.join(suggestions[id_type])}\n"
            columns = get_user_input(prompt).split(',')
            columns = [col.strip() for col in columns if col.strip()]
            if columns:
                table_mappings[id_type] = columns
                if 'id' in columns:
                    id_is_included = True
        
        id_mappings[table] = {"mappings": table_mappings,
                              "full_table_name": f"{input_db}.{input_schema}.{table}"}
    
    # Create necessary files
    connection_name = create_siteconfig(conn, output_db, output_schema, password, account)        
    
    about_files = f"""We will now discuss a couple of files that are needed to build profiles for this entity.
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
    for line in about_files.split("\n"):
        print(line)
        input("")
    
    create_pb_project(entity_name, id_types, connection_name)
    create_inputs_yaml(id_mappings, entity_name)
    create_profiles_yaml(entity_name, relevant_tables)
    
    print("\nProfile Builder project files have been created successfully!")

if __name__ == "__main__":
    main()