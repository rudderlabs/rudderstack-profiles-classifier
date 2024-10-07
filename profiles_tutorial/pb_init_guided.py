import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import re
from pathlib import Path
from ruamel.yaml import YAML
import getpass
import pandas as pd
import sys
from llm_core.wh.utils import SnowparkConnector

SAMPLE_DATA_DIR = "sample_data"
TABLE_SUFFIX = '68C7CF88'

def connect_to_snowflake(inputs=None):
    common_steps = [
        "Account (ex: ina31471.us-east-1): ",
        "Username: ",
        "Password: ",
        "Role: ",
        "Warehouse: "
    ]

    input_steps = [
        f"Input Database name (where the source data is - for this demo, we will upload sample data to this database/schema (with a suffix `{TABLE_SUFFIX}` to avoid overwrites) - so you can choose any existing database/schema): ",
        "Input Schema name (where the source data is): "
    ]

    output_steps = [
        "Output Database name (where the output profiles data will be stored): ",
        "Output Schema name (where the output profiles data will be stored): "
    ]
    if not inputs:
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

    config = {
        "account": inputs[0],
        "user": inputs[1],
        "password": inputs[2],
        "role": inputs[3],
        "warehouse": inputs[4],
        "input_database": inputs[5],
        "input_schema": inputs[6],
        "output_database": inputs[7],
        "output_schema": inputs[8]
    }

    print("\nPlease review the details you entered:")
    print("\n** Common Connection Details **")
    print(f"Account: {config['account']}")
    print(f"Username: {config['user']}")
    print(f"Role: {config['role']}")
    print(f"Warehouse: {config['warehouse']}")

    print("\n** Input Connection Details (from where event-stream data will be read) **")
    print(f"Input Database: {config['input_database']}")
    print(f"Input Schema: {config['input_schema']}")

    print("\n** Output Connection Details (where the profiles output data will be stored) **")
    print(f"Output Database: {config['output_database']}")
    print(f"Output Schema: {config['output_schema']}")

    confirm = input("\nAre these details correct? (yes/no): ").strip().lower()
    if confirm == 'no':
        print("\nLet's try again.\n")
        return connect_to_snowflake()

    try:
        snowpark = SnowparkConnector({
            "account": config["account"],
            "dbname": config["output_database"],
            "schema": config["output_schema"],
            "type": "snowflake",
            "user": config["user"],
            "password": config["password"],
            "role": config["role"],
            "warehouse": config["warehouse"],
        })
    except Exception as e:
        print(f"Failed to establish connection: {e}")
        exit(1)
    return config, snowpark

def upload_sample_data(conn: SnowparkConnector, input_database: str, input_schema: str) -> dict:
    new_table_names = {}
    to_upload=True
    res = conn.run_query(f"SHOW TABLES IN SCHEMA {input_database}.{input_schema}")
    existing_tables = [row[1].lower() for row in res] 
    for filename in os.listdir(SAMPLE_DATA_DIR):
        if not filename.endswith(".csv"):
            continue

        base_name = os.path.splitext(filename)[0]
        table_name = f"{base_name}_{TABLE_SUFFIX}"
        new_table_names[filename] = table_name
        if not to_upload:
            continue
        
        if table_name.lower() in existing_tables:
            print(f"Table {table_name} already exists.")
            action = get_user_input("Do you want to skip uploading again, so we can reuse the tables? (yes/no) (yes - skips upload, no - uploads again): ", 
                                    options=["yes", "no"])
            if action.lower() == "yes":
                print("Skipping upload of all csv files.")
                to_upload=False
                continue

            _ = conn.run_query(f"DROP TABLE {input_database}.{input_schema}.{table_name}")

        df = pd.read_csv(os.path.join(SAMPLE_DATA_DIR, filename))
        conn.session.write_pandas(df, 
                                    table_name, 
                                    database=input_database.upper(), 
                                    schema=input_schema.upper(), 
                                    auto_create_table=True, 
                                    overwrite=True)
    return new_table_names


def find_relevant_tables(conn: SnowparkConnector, input_database: str, input_schema: str, new_table_names: dict):
    res = conn.run_query(f"SHOW TABLES IN SCHEMA {input_database}.{input_schema}")
    tables = [row[1] for row in res] 
    relevant_tables = [table for table in tables if table in new_table_names.values()]
    return relevant_tables

def guide_id_type_input(entity_name):
    predefined_id_types = ['device_id',
                            'email',
                            'user_id',
                            'shopify_customer_id',
                            'anonymous_id',
                            'shopify_store_id']
    selected_id_types = []
    print("We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested")
    for expected_id_type in predefined_id_types:
        while True:
            user_input = get_user_input(f"\nLet's add '{expected_id_type}' as an id type for {entity_name}: ")
            if user_input.lower() == expected_id_type.lower():
                selected_id_types.append(expected_id_type)
                break
            else:
                print(f"Please enter the exact name '{expected_id_type}'. Let's try again.")
    print(f"\nGreat. So, the id types associated with entity {entity_name} are:")
    for id_type in selected_id_types:
        print(f"\t - {id_type}")
    return selected_id_types

def get_columns(conn: SnowparkConnector, input_database: str, input_schema: str, table: str):
    try:
        query = f"DESCRIBE TABLE {input_database}.{input_schema}.{table}"
        result = conn.run_query(query, output_type="list")
        columns = [row["name"] for row in result]
    except Exception as e:
        raise Exception(f"Error fetching columns for {table}: {e}")
    return columns

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


def get_user_input(prompt, options=None, password=False, default=None):
    while True:
        if default:
            full_prompt = f"{prompt} (default: {default})\n>"
        else:
            full_prompt = f"{prompt}\n> "        
        if password:
            user_input = getpass.getpass(full_prompt).strip()
        else:
            user_input = input(full_prompt).strip()
        if not user_input and default:
            user_input = default
        if options:
            if user_input.lower() in [opt.lower() for opt in options]:
                return user_input
            print("Invalid input. Please try again.")
        else:
            return user_input

def create_siteconfig(config: dict):
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
                    "account": config["account"],
                    "dbname": config["output_database"],
                    "password": config["password"],  
                    "role": config["role"],
                    "schema": config["output_schema"],
                    "type": "snowflake",
                    "user": config["user"],
                    "warehouse": config["warehouse"]
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

def create_inputs_yaml(id_mappings):
    inputs = {"inputs": []}
    for table, mapping in id_mappings.items():
        table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
        input_entry = {"name": table_name,
                       "app_defaults": 
                       {
                           "table": mapping["full_table_name"],
                           "occurred_at_col": "timestamp",
                           "ids": []
                       }
                    }
        for id_info in mapping["mappings"]:
            input_entry["app_defaults"]["ids"].append(id_info)
        inputs["inputs"].append(input_entry)

    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 4096  # Prevent line wrapping
    
    with open("profiles/models/inputs.yaml", "w") as f:
        yaml.dump(inputs, f)

def create_profiles_yaml(entity_name, tables):
    edge_sources = []
    for table in tables:
        table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
        edge_sources.append({"from": f"inputs/{table_name}"})
    profiles = {
        "models": [
            {
                "name": f"{entity_name}_id_stitcher",
                "model_type": "id_stitcher",
                "model_spec": {
                    "validity_time": "24h",
                    "entity_key": entity_name,
                    "edge_sources": edge_sources
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

def get_sample_data(conn: SnowparkConnector, database, schema, table, column, num_samples=5):
    """
    Retrieve a specified number of sample data from a column in a table.
    """
    try:
        query = f"SELECT {column} FROM {database}.{schema}.{table} where {column} is not null LIMIT {num_samples}"
        df: pd.DataFrame = conn.run_query(query, output_type="pandas")
        if df.empty:
            return []        
        samples = df.iloc[:,0].dropna().astype(str).tolist()
        return samples[:num_samples]
    except Exception as e:
        print(f"Error fetching sample data for column '{column}' in table '{table}': {e}")
        return []
    
def map_columns_to_id_types(conn: SnowparkConnector, 
                            input_db: str, 
                            input_schema: str, 
                            table: str, 
                            id_types: list, 
                            entity_name: str):
    try:
        columns = get_columns(conn, input_db, input_schema, table)
    except Exception as e:
        print(f"Error fetching columns for {table}: {e}")
        return None, "back"
        
    # Shortlist columns based on regex matches with id_types
    shortlisted_columns = {}
    for id_type in id_types:
        pattern = re.compile(rf".*{id_type}.*", re.IGNORECASE)
        matched_columns = [col for col in columns if pattern.match(col)]
        if matched_columns:
            shortlisted_columns[id_type] = matched_columns
    # Display table context
    print(f"\n{'-'*80}\n")
    print(f"The table `{table}` has the following columns, which look like id types:\n")

    # Display shortlisted columns with sample data
    for id_type, cols in shortlisted_columns.items():
        for col in cols:
            sample_data = get_sample_data(conn, input_db, input_schema, table, col)
            print(f"id_type: {id_type}")
            print(f"column: {col} (sample data: {sample_data})\n")

    # Display all available id_types
    print(f"Following are all the id types defined earlier: \n\t{','.join(id_types)}")

    applicable_id_types_input = get_user_input(f"Enter the comma-separated list of id_types applicable to the `{table}` table: \n>")
    applicable_id_types = [id_type.strip() for id_type in applicable_id_types_input.split(",") if id_type.strip() in [id_type_.lower() for id_type_ in id_types]]

    applicable_id_types = [it for it in id_types if it.lower() in [ait.lower() for ait in applicable_id_types]]

    if not applicable_id_types:
        print(f"No valid id_types selected for `{table}` table. Skipping this table (it won't be part of id stitcher)")
        return {}, "next"
    
    print(f"\nNow let's map different id_types in table `{table}` to a column (you can also use SQL string operations on these columns: ex: LOWER(EMAIL_ID)):\n")

    table_mappings = []
    for id_type in applicable_id_types:
        while True:
            print(f"\nid type: {id_type}")
            # Suggest columns based on regex matches
            suggested_cols = shortlisted_columns.get(id_type, [])
            if suggested_cols:
                print("Suggestions based on column names:")
                for col in suggested_cols:
                    sample_data = get_sample_data(conn, input_db, input_schema, table, col)
                    print(f" - {col} (sample data: {sample_data})")
            user_input = get_user_input(f"Enter the column(s) for id_type '{id_type}' in table `{table}` (comma-separated if there are multiple columns in the same table mapping to {id_type}), or 'skip' to skip:\n> ")
            if user_input.lower() == 'skip':
                print(f"Skipping id_type '{id_type}' for table `{table}`")
                break

            selected_columns = [col.strip() for col in user_input.split(",")]
            if not selected_columns:
                print("No valid columns selected. Please try again.\n")
                continue
            # Display selected columns with sample data for confirmation
            print(f"Selected columns for id_type '{id_type}':")
            for col in selected_columns:
                sample_data = get_sample_data(conn, input_db, input_schema, table, col)
                print(f"- {col} (sample data: {sample_data})")

            confirm = get_user_input("Is this correct? (yes/no): ", options=["yes", "no"])
            if confirm.lower() == 'yes':
                for col in selected_columns:
                    mapping = {"select": col, 
                               "type": id_type,
                               "entity": entity_name}
                    table_mappings.append(mapping)
                break
            else:
                print("Let's try mapping again.\n")
    if table_mappings:
        print("Following is the summary of id types selected: \n")
        summary = {"table": table, 
                   "ids": table_mappings}
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.preserve_quotes = True
        yaml.width = 4096  # Prevent line wrapping
        yaml.dump(summary, sys.stdout)
        print("\n")
        input(f"The above is the inputs yaml for table `{table}`")
    else:
        input("No id_type mappings were selected for this table.\n")
    return table_mappings, "next"
                    

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
    id_types = guide_id_type_input(entity_name)

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

    config, snowpark = connect_to_snowflake()
    input_db = config["input_database"]
    input_schema = config["input_schema"]
    print(f"\nUploading sample data to your warehouse account. We will add a suffix `{TABLE_SUFFIX}` to the table names to avoid conflicts and not overwrite any existing tables.")

    new_table_names = upload_sample_data(snowpark, input_db, input_schema)
    for original_name, new_name in new_table_names.items():
        print(f"\t{original_name} -> {new_name}")

    print(f"\n** Searching the `{input_schema}` schema in `{input_db}` database for uploaded sample tables, that will act as sources for profiles **\n")

    try:    
        relevant_tables = find_relevant_tables(snowpark, input_db, input_schema, new_table_names)
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
    table_index = 0
    while table_index < len(relevant_tables):
        table = relevant_tables[table_index]
        table_mappings, action = map_columns_to_id_types(snowpark, input_db, input_schema, table, id_types, entity_name)
        if action == "back":
            if table_index > 0:
                table_index -= 1
                if relevant_tables[table_index] in id_mappings:
                    _ = id_mappings.pop(relevant_tables[table_index])
            else:
                print("You are already at the first table.")
        else:
            if table_mappings:
                id_mappings[table] = {"mappings": table_mappings, 
                                      "full_table_name": f"{input_db}.{input_schema}.{table}"}
            table_index +=1
    
    # Create necessary files
    connection_name = create_siteconfig(config)
    
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
    create_inputs_yaml(id_mappings)
    create_profiles_yaml(entity_name, id_mappings.keys())
    
    print("\nProfile Builder project files have been created successfully!")

if __name__ == "__main__":
    main()