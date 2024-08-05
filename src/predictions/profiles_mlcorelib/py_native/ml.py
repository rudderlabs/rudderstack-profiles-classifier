from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe, NoOpRecipe
from profiles_rudderstack.material import WhtFolder
from typing import Tuple
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
    FeatureDetailsBuildSpecSchema,
)


class MlModel(BaseModelType):
    TypeName = "ml_model"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "occurred_at_col": {"type": "string"},
            **EntityKeyBuildSpecSchema["properties"],
            "inputs": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "training_spec": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "data": {
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "label_column": {"type": "string"},
                            "prediction_horizon_days": {"type": "integer"},
                        },
                        "required": ["label_column", "prediction_horizon_days"],
                    },
                    "preprocessing": {
                        "type": "object",
                    },
                    "training_file_lookup_path": {"type": "string"},
                    "validity_time": {
                        "type": "string",
                        "enum": ["day", "week", "month"],
                    },
                    "name": {
                        "type": "string",
                    },
                },
                "required": ["data", "name"],
            },
            "prediction_spec": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "column_names": {
                        "type": "object",
                    },
                    "name": {
                        "type": "string",
                    },
                    **FeatureDetailsBuildSpecSchema["properties"],
                },
                "required": ["column_names", "name"],
            },
        },
        "required": ["training_spec", "prediction_spec", "inputs"]
        + EntityKeyBuildSpecSchema["required"],
    }

    def __init__(
        self,
        build_spec: dict,
        schema_version: int,
        pb_version: str,
        parent_folder: WhtFolder,
    ) -> None:
        # ephemeral materialization so that pb doesn't try to validate the output of the model
        build_spec["materialization"] = {"output_type": "ephemeral"}
        super().__init__(build_spec, schema_version, pb_version)
        training_model_name = build_spec["training_spec"]["name"]
        training_build_spec = {
            "occurred_at_col": build_spec["occurred_at_col"],
            "entity_key": build_spec["entity_key"],
            "materialization": build_spec.get("materialization", {}),
            "inputs": build_spec["inputs"],
            "training_file_lookup_path": build_spec["training_spec"].get(
                "training_file_lookup_path", None
            ),
            "validity_time": build_spec["training_spec"].get("validity_time", None),
            "ml_config": {
                "data": build_spec["training_spec"]["data"],
            },
        }
        parent_folder.add_child_specs(
            training_model_name, "training_model", training_build_spec
        )
        training_model_ref = parent_folder.folder_ref() + training_model_name
        prediction_build_spec = {
            "occurred_at_col": build_spec["occurred_at_col"],
            "entity_key": build_spec["entity_key"],
            "training_model": training_model_ref,
            "inputs": build_spec["inputs"],
            "ml_config": {
                "data": build_spec["training_spec"]["data"],
                "outputs": {
                    "column_names": build_spec["prediction_spec"]["column_names"],
                },
            },
            "features": build_spec["prediction_spec"]["features"],
        }
        parent_folder.add_child_specs(
            self.build_spec["prediction_spec"]["name"],
            "prediction_model",
            prediction_build_spec,
        )

    def get_material_recipe(self) -> PyNativeRecipe:
        return NoOpRecipe()

    def validate(self) -> Tuple[bool, str]:
        return super().validate()
