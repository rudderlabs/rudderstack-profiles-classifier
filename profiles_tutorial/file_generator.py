import os
from pathlib import Path
from ruamel.yaml import YAML
import logging
from config import TABLE_SUFFIX, CONFIG_FILE_PATH, INPUTS_FILE_PATH, PROFILES_FILE_PATH

logger = logging.getLogger(__name__)

class FileGenerator:
    def __init__(self):
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.width = 4096  # Prevent line wrapping

    def create_siteconfig(self, config: dict) -> str:
        logger.info("""We need to save the warehouse credentials in a file called `siteconfig.yaml`. 
              This file is stored in a folder called `.pb` in your home directory. 
              This helps profile builder to connect to your warehouse account and run the project automatically.""")

        home_dir = str(Path.home())
        pb_dir = os.path.join(home_dir, ".pb")
        os.makedirs(pb_dir, exist_ok=True)
        siteconfig_path = os.path.join(pb_dir, "siteconfig.yaml")

        existing_siteconfig = {}
        if os.path.exists(siteconfig_path):
            logger.info(f"Found existing siteconfig.yaml file at {siteconfig_path}. We will append the credentials to the existing file.")
            with open(siteconfig_path, "r") as f:
                existing_siteconfig = self.yaml.load(f) or {}

        connection_name = "test"
        while connection_name in existing_siteconfig.get("connections", {}):
            replace = input(f"Connection '{connection_name}' already exists. Should we replace it? (yes/no): ").lower()
            if replace == "yes":
                break
            connection_name = input("Enter a new connection name: ")

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
            self.yaml.dump(existing_siteconfig, f)

        return connection_name

    def create_pb_project(self, entity_name, id_types, connection_name):
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
        
        with open(CONFIG_FILE_PATH, "w") as f:
            self.yaml.dump(pb_project, f)

    def create_inputs_yaml(self, id_mappings):
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
        
        with open(INPUTS_FILE_PATH, "w") as f:
            self.yaml.dump(inputs, f)

    def create_profiles_yaml(self, entity_name, tables):
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
        
        with open(PROFILES_FILE_PATH, "w") as f:
            self.yaml.dump(profiles, f)