from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.contract import build_contract
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from typing import List
import pandas as pd
from pathlib import Path

from .src.train import train
from .src.predict import predict

class ChurnModel(BaseModelType):
    TypeName = "churn_prediction"

    BuildSpecSchema = {
            "type": "object",
            "properties": {
                "inputs": { "type": "array", "items": { "type": "string" } },
                "model_info": {
                    "type": "object",
                    "properties": {
                        "file_extension": { "type": "string" },
                        "file_validity": { "type": "string" },
                        "config": { "type": ["object"] } },
                        # "config": { "type": ["object", "null"], "additionalProperties": True } },
                    "required": ["file_extension", "file_validity", "config"] }
            },
            "required": ["inputs", "model_info"],
            "additionalProperties": False
            }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self)-> PyNativeRecipe:
        return ChurnRecipe(self.build_spec["inputs"], self.build_spec["model_info"])

    def validate(self):
        # Model Validate
        if self.build_spec.get("inputs") is None or len(self.build_spec["inputs"]) == 0:
            return False, "inputs are required"
        
        return super().validate()


class ChurnRecipe(PyNativeRecipe):
    def __init__(self, inputs: List[str], model_info: dict) -> None:
        self.inputs = inputs
        self.model_info = model_info
        self.logger = Logger("ChurnRecipe")

    def describe(self, this: WhtMaterial):
        materialName = this.name()
        return f"""Material - {materialName}\nInputs: {self.inputs}""", ".txt"

    def prepare(self, this: WhtMaterial):
        for inModel in self.inputs:
            # contract = build_contract('{ "is_event_stream": true, "with_columns":[{"name":"num"}] }')
            this.de_ref(inModel)

    def execute(self, this: WhtMaterial):
        self.logger.info("Executing ChurnRecipe ================ Start")
        snowpark_session = this.wht_ctx.snowpark_session
        wh_type = this.wht_ctx.client.wh_type   ## warehouse type from configs to differ the snowpark session with redshift connection 
        model_name = this.model.name()
        output_folder = this.get_output_folder()
        training_json_file = f"{output_folder}/{model_name}_training_file.json"
        aws_config=None
        inputs = None

        self.logger.info(f"Training script initiation ================")
        train(snowpark_session, inputs, training_json_file, self.model_info.get('config'), wh_type, this)
        self.logger.info(f"Predictions script initiation ================")
        predict(snowpark_session, aws_config, training_json_file, None, self.model_info.get('config'), this)

        self.logger.info("Executing ChurnRecipe ================ End")