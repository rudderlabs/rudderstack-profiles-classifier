from ruamel.yaml import YAML
import logging
import yaml

from .config import TABLE_SUFFIX, CONFIG_FILE_PATH, INPUTS_FILE_PATH, PROFILES_FILE_PATH

logger = logging.getLogger(__name__)


class YamlGenerator:
    def __init__(self, fast_mode: bool):
        self.fast_mode = fast_mode
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
                    "occurred_at_col": "timestamp",
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

    def update_bad_anons_filter(self):
        regex_pattern = "(c8bc33a0-7cb7-47f9-b24f-73e077346142|f0ed91a9-e1a9-46a5-9257-d590f45612fe|cbe0ea73-4878-4892-ac82-b9ad42797000|f4690568-e9e7-4182-abc6-6ea2791daba3|b369d6f5-c17a-457c-ab86-5649c1b53883)"
        with open(CONFIG_FILE_PATH, "r") as file:
            pb_project = yaml.safe_load(file)
        for id_type in pb_project["id_types"]:
            if id_type["name"] == "anon_id":
                id_type["filters"] = [{"type": "exclude", "regex": regex_pattern}]
        with open(CONFIG_FILE_PATH, "w") as file:
            yaml.dump(pb_project, file)
