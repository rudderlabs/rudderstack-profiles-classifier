from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.contract import build_contract
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger
from typing import List
import pandas as pd
from pathlib import Path

from .train import train
from .predict import predict

class ChurnModel(BaseModelType):
    TypeName = "churn_prediction"

    BuildSpecSchema = {
            "type": "object",
            "properties": {
                "inputs": { "type": "array", "items": { "type": "string" } },
                "train": {
                    "type": "object",
                    "properties": {
                        "file_extension": { "type": "string" },
                        "file_validity": { "type": "string" },
                        "config": { "type": ["object"] } },
                    "required": ["file_extension", "file_validity"] },
                "predict": {
                    "type": "object",
                    "properties": {
                        "config": { "type": ["object"] } } }
            },
            "required": ["inputs", "train", "predict"],
            "additionalProperties": False
            }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self)-> PyNativeRecipe:
        return ChurnRecipe(self.build_spec["inputs"], self.build_spec["train"], self.build_spec["predict"])

    def validate(self):
        # Model Validate
        if self.build_spec.get("inputs") is None or len(self.build_spec["inputs"]) == 0:
            return False, "inputs are required"
        
        return super().validate()


class ChurnRecipe(PyNativeRecipe):
    def __init__(self, inputs: List[str], train_config: dict, predict_config: dict) -> None:
        self.inputs = inputs
        self.train_info = train_config
        self.predict_info = predict_config
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
        model_name = this.model.name()
        output_folder = this.get_output_folder()
        training_json_file = f"{output_folder}/{model_name}_training_file.json"
        aws_config=None
        inputs = None

        # wh_type = this.wht_ctx.client.__wh_type     ## warehouse type from configs to differ the snowpark session with redshift connection

        self.logger.info(f"Training script initiation ================")
        train(snowpark_session, inputs, training_json_file, self.train_info.get('config'), this)
        self.logger.info(f"Predictions script initiation ================")
        predict(snowpark_session, aws_config, training_json_file, None, self.predict_info.get('config'), this)

        self.logger.info("Executing ChurnRecipe ================ End")