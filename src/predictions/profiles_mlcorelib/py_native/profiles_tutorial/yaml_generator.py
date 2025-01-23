import os
from ruamel.yaml import YAML
import logging
import yaml

from .io_handler import IOHandler
from .config import (
    TABLE_SUFFIX,
    CONFIG_FILE_PATH,
    INPUTS_FILE_PATH,
    PROFILES_FILE_PATH,
    PREDEFINED_ID_TYPES,
)

logger = logging.getLogger(__name__)


class YamlGenerator:
    def __init__(self, io: IOHandler):
        self.io = io
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.width = 4096  # Prevent line wrapping

    def create_pb_project(self, entity_name, id_types, connection_name, id_graph_model):
        pb_project = {
            "name": "demo_project",
            "schema_version": 72,
            "connection": connection_name,
            "model_folders": ["models"],
            "entities": [
                {
                    "name": entity_name,
                    "id_stitcher": f"models/{id_graph_model}",
                    "id_types": id_types,
                }
            ],
            "id_types": [{"name": id_type} for id_type in id_types],
        }

        with open(CONFIG_FILE_PATH, "w") as f:
            self.yaml.dump(pb_project, f)

    def create_inputs_yaml(self, id_mappings):
        inputs = {"inputs": []}
        for table, mapping in id_mappings.items():
            table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
            input_entry = {
                "name": table_name,
                "app_defaults": {
                    "table": mapping["full_table_name"],
                    "occurred_at_col": "event_timestamp",
                    "ids": [],
                },
            }
            for id_info in mapping["mappings"]:
                input_entry["app_defaults"]["ids"].append(id_info)
            inputs["inputs"].append(input_entry)

        with open(INPUTS_FILE_PATH, "w") as f:
            self.yaml.dump(inputs, f)

    def create_profiles_yaml(self, entity_name, tables, model_name):
        edge_sources = []
        for table in tables:
            # table_name = "rs" + table.replace(f"_{TABLE_SUFFIX}", "").capitalize()
            # edge_sources.append({"from": f"inputs/{table_name}"})
            edge_sources.append({"from": table})
        profiles = {
            "models": [
                {
                    "name": model_name,
                    "model_type": "id_stitcher",
                    "model_spec": {
                        "validity_time": "24h",
                        "entity_key": entity_name,
                        "edge_sources": edge_sources,
                    },
                }
            ]
        }

        with open(PROFILES_FILE_PATH, "w") as f:
            self.yaml.dump(profiles, f)

    def validate_shopify_store_id_is_removed(self):
        with open(CONFIG_FILE_PATH, "r") as file:
            pb_project = yaml.safe_load(file)
        with open(INPUTS_FILE_PATH, "r") as file:
            inputs = yaml.safe_load(file)
        if "shopify_store_id" in pb_project["entities"][0][
            "id_types"
        ] or "shopify_store_id" in [
            id_type["name"] for id_type in pb_project["id_types"]
        ]:
            logger.error("shopify_store_id still exists in pb_project.yaml")
            return False
        for input_table in inputs["inputs"]:
            for id_info in input_table["app_defaults"]["ids"]:
                if id_info["type"] == "shopify_store_id":
                    logger.error("shopify_store_id still exists in inputs.yaml")
                    return False
        return True

    def remove_shopify_store_id(self):
        with open(CONFIG_FILE_PATH, "r") as file:
            pb_project = yaml.safe_load(file)
        with open(INPUTS_FILE_PATH, "r") as file:
            inputs = yaml.safe_load(file)
        for id_type in pb_project["id_types"]:
            if id_type["name"] == "shopify_store_id":
                pb_project["id_types"].remove(id_type)
        for entity in pb_project["entities"]:
            for id_type in entity["id_types"]:
                if id_type == "shopify_store_id":
                    entity["id_types"].remove(id_type)
        for input_table in inputs["inputs"]:
            for id_info in input_table["app_defaults"]["ids"]:
                if id_info["type"] == "shopify_store_id":
                    input_table["app_defaults"]["ids"].remove(id_info)
        with open(CONFIG_FILE_PATH, "w") as file:
            yaml.dump(pb_project, file)
        with open(INPUTS_FILE_PATH, "w") as file:
            yaml.dump(inputs, file)

    def update_bad_anons_filter(self):
        regex_pattern = "(c8bc33a0-7cb7-47f9-b24f-73e077346142|f0ed91a9-e1a9-46a5-9257-d590f45612fe|cbe0ea73-4878-4892-ac82-b9ad42797000|f4690568-e9e7-4182-abc6-6ea2791daba3|b369d6f5-c17a-457c-ab86-5649c1b53883)"
        with open(CONFIG_FILE_PATH, "r") as file:
            pb_project = yaml.safe_load(file)
        for id_type in pb_project["id_types"]:
            if id_type["name"] == "anon_id":
                id_type["filters"] = [{"type": "exclude", "regex": regex_pattern}]
        with open(CONFIG_FILE_PATH, "w") as file:
            yaml.dump(pb_project, file)

    def add_macros(self, macros: list[dict]):
        with open(PROFILES_FILE_PATH, "r") as file:
            profiles = yaml.safe_load(file)

        profiles["macros"] = macros
        with open(PROFILES_FILE_PATH, "w") as file:
            yaml.dump(profiles, file)

    def add_features(self, entity_name: str, features: list[dict]):
        vars = []
        for feature in features:
            entity_var = {
                "name": feature["name"],
                "select": feature["select"],
            }
            if "from" in feature:
                entity_var["from"] = feature["from"]
            if "description" in feature:
                entity_var["description"] = feature["description"]
            if "window" in feature:
                entity_var["window"] = feature["window"]
            if "where" in feature:
                entity_var["where"] = feature["where"]

            vars.append({"entity_var": entity_var})

        with open(PROFILES_FILE_PATH, "r") as file:
            profiles = yaml.safe_load(file)

        profiles["var_groups"] = [
            {"name": f"{entity_name}_features", "entity_key": entity_name, "vars": vars}
        ]
        with open(PROFILES_FILE_PATH, "w") as file:
            yaml.dump(profiles, file)

    def add_feature_views(self, entity_name: str, using_ids: list[dict]):
        with open(CONFIG_FILE_PATH, "r") as file:
            pb_project = yaml.safe_load(file)

        entities = pb_project["entities"]
        for entity in entities:
            if entity["name"] == entity_name:
                entity["feature_views"] = {"using_ids": using_ids}

        with open(CONFIG_FILE_PATH, "w") as file:
            yaml.dump(pb_project, file)

    def guide_id_type_input(self, entity_name) -> list[str]:
        about_id_types = {
            "anon_id": """
RudderStack creates a cookie for each user when they visit your site.
This cookie is used to identify the user across different sessions anonymously, prior to the users identifying themselves.
Every single event from event stream will have an anonymous_id.""",
            "email": """
If a customer signs up using their email, this can be used to identify customers across devices, or even when they clear cookies.""",
            "user_id": """
Most apps/websites define their own user_id for signed in users. 
These are the "identified" users, and typically the most common id type for computing various metrics such as daily active users, monthly active users, etc.""",
            "device_id": """
This is a more specialized id type specific to Secure Solutions, LLC. Since they sell IoT devices, they want to link specific device ids back to specific users.
The ID stitcher helps connect these events to the user_id/email of the user as long as there's even a single event linking them (ex: a 'register your device' event).""",
            "shopify_customer_id": """
An Ecommerce platform like Shopify will auto generate a unique identifier for each unique user who completes any sort of checkout conversion and thereby become an official customer. 
This unique identifier is called, “shopify_customer_id”. We will want to bring this ID Type in in order to enhance the user ID Graph.""",
            "shopify_store_id": """
When you have a payment service provider like Shopify, you may have different stores or apps - one for a website, one for an android app etc. 
These different apps or stores may have their own unique identifiers. It is important to note here, this id type is of a higher level grain than the user identifiers. 
One store id may be linked to many users.  And so for the user entity, we should not bring it in. 
But for the purposes of this tutorial, we will go ahead and bring it in to show you what happens and how to troubleshoot when a resolved profile id has merged multiple users together.""",
        }
        selected_id_types = []
        for n, expected_id_type in enumerate(PREDEFINED_ID_TYPES):
            if n == 0:
                self.io.display_message(f"The first id type is {expected_id_type}.")
            else:
                self.io.display_message(f"Now, the next id: {expected_id_type}.")
            self.io.display_multiline_message(about_id_types[expected_id_type])

            user_input = self.io.get_user_input(
                f"\nLet's add '{expected_id_type}' as an id type for {entity_name}: ",
                options=[expected_id_type],
            )
            selected_id_types.append(user_input)

        self.io.display_message(
            f"\nGreat. So, the id types associated with entity {entity_name} are:"
        )
        for id_type in selected_id_types:
            self.io.display_message(f"\t - {id_type}")
        return selected_id_types
