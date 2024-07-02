from typing import List, Dict, Tuple, Union
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
)
from profiles_rudderstack.logger import Logger
import re


class AttributionModel(BaseModelType):
    TypeName = "attribution_report"  # the name of the model type

    # json schema for the build spec
    BuildSpecSchema = {
        "type": "object",
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            "report_granularity": {"type": ["string", "null"]},
            "spend_inputs": {"type": "array", "items": {"type": "string"}},
            "user_journeys": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "from": {"type": "string"},
                        "timestamp": {"type": "string"},
                        "touch": {
                            "type": "object",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "utm_source": {"type": "string"},
                                    "utm_campaign": {"type": "string"},
                                },
                                "oneOf": [
                                    {"required": ["utm_source", "utm_campaign"]},
                                    {"required": ["utm_source"]},
                                    {"required": ["utm_campaign"]},
                                ],
                            },
                        },
                    },
                    "required": ["from", "timestamp", "touch"],
                },
            },
            "conversions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "timestamp": {"type": "string"},
                        "value": {"type": "string"},
                    },
                    "required": ["name", "timestamp"],
                },
            },
        },
        "required": EntityKeyBuildSpecSchema["required"]
        + ["spend_inputs", "user_journeys", "conversions"],
        "additionalProperties": False,
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return AttributionModelRecipe(self.build_spec)

    def validate(self) -> Tuple[bool, str]:
        return True, "Validated successfully"


class AttributionModelRecipe(PyNativeRecipe):
    def __init__(self, config: Dict) -> None:
        self.logger = Logger("attribution_model")

        self.config = config
        self.spend_inputs = self.config["spend_inputs"]
        self.user_journeys = self.config["user_journeys"]
        self.conversions = self.config["conversions"]

        self.inputs = {
            "var_table": f'{self.config["entity_key"]}/all/var_table',
        }
        for spend_input in self.spend_inputs:
            self.inputs[spend_input] = f"{spend_input}"
        for obj in self.config["user_journeys"]:
            self.inputs[f'{obj["from"]}'] = obj["from"]
        for obj in self.config["conversions"]:
            for key, value in obj.items():
                self.inputs[value] = f'entity/{self.config["entity_key"]}/{value}'

    def describe(self, this: WhtMaterial):
        description = """You can see the output table in the warehouse where each touchpoint has an attribution score."""
        return description, ".txt"

    def register_dependencies(self, this: WhtMaterial):
        for key in self.inputs:
            this.de_ref(self.inputs[key])

    def execute(self, this: WhtMaterial):
        pass
