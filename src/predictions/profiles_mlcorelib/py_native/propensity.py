from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe, NoOpRecipe
from profiles_rudderstack.material import WhtFolder
from typing import Tuple
from profiles_rudderstack.schema import (
    EntityKeyBuildSpecSchema,
    EntityIdsBuildSpecSchema,
)

PredictionColumnSpecSchema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "description": {"type": "string"},
        "is_feature": {"type": "boolean"},
    },
    "required": ["name"],
}


class PropensityModel(BaseModelType):
    TypeName = "propensity"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            **EntityKeyBuildSpecSchema["properties"],
            **EntityIdsBuildSpecSchema["properties"],
            "inputs": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "training": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "predict_var": {"type": "string"},
                    "predict_window_days": {"type": "integer"},
                    "max_row_count": {"type": "integer"},
                    "eligible_users": {"type": "string"},
                    "label_value": {"type": "number"},
                    "recall_to_precision_importance": {"type": "number"},
                    "new_materialisations_config": {"type": "object"},
                    "top_k_array_categories": {"type": "integer"},
                    "timestamp_columns": {"type": "array", "items": {"type": "string"}},
                    "arraytype_columns": {"type": "array", "items": {"type": "string"}},
                    "booleantype_columns": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "ignore_features": {"type": "array", "items": {"type": "string"}},
                    "numeric_features": {"type": "array", "items": {"type": "string"}},
                    "categorical_features": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "type": {
                        "type": "string",
                        "enum": ["classification", "regression"],
                    },
                    "validity": {
                        "type": "string",
                        "enum": ["day", "week", "month"],
                    },
                    "file_lookup_path": {"type": "string"},
                },
                "required": ["predict_var", "predict_window_days"],
            },
            "prediction": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "output_columns": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "percentile": PredictionColumnSpecSchema,
                            "score": PredictionColumnSpecSchema,
                        },
                        "required": ["percentile", "score"],
                    },
                    "eligible_users": {"type": "string"},
                },
                "required": ["output_columns"],
            },
        },
        "required": ["training", "prediction", "inputs"]
        + EntityKeyBuildSpecSchema["required"],
    }

    def __init__(
        self,
        build_spec: dict,
        schema_version: int,
        pb_version: str,
        parent_folder: WhtFolder,
        model_name: str,
    ) -> None:
        # ephemeral materialization so that pb doesn't try to validate the output of the model
        build_spec["materialization"] = {"output_type": "ephemeral"}
        super().__init__(build_spec, schema_version, pb_version)
        training_model_name = model_name + "_training"
        training_spec = self._get_training_spec()
        parent_folder.add_child_specs(
            training_model_name, "training_model", training_spec
        )
        training_model_ref = parent_folder.folder_ref() + training_model_name
        prediction_spec = self._get_prediction_spec(training_model_ref, training_spec)
        parent_folder.add_child_specs(
            model_name + "_prediction",
            "prediction_model",
            prediction_spec,
        )

    def _get_training_spec(self) -> dict:
        data = {}
        data["label_column"] = self.build_spec["training"]["predict_var"]
        data["prediction_horizon_days"] = self.build_spec["training"][
            "predict_window_days"
        ]
        training_params = self.build_spec.get("training", {})
        data_keys = [
            "type",
            "label_value",
            "eligible_users",
            "max_row_count",
            "recall_to_precision_importance",
            "new_materialisations_config",
        ]
        for key in data_keys:
            data[key] = training_params.get(key, None)

        preprocessing = {}
        preprocessing_keys = [
            "top_k_array_categories",
            "timestamp_columns",
            "arraytype_columns",
            "booleantype_columns",
            "ignore_features",
            "numeric_features",
            "categorical_features",
        ]
        for key in preprocessing_keys:
            preprocessing[key] = training_params.get(key, None)
        return {
            "entity_key": self.build_spec["entity_key"],
            "materialization": self.build_spec.get("materialization", {}),
            "inputs": self.build_spec["inputs"],
            "training_file_lookup_path": self.build_spec["training"].get(
                "file_lookup_path", None
            ),
            "validity_time": self.build_spec["training"].get("validity", None),
            "ml_config": {"data": data, "preprocessing": preprocessing},
        }

    def _get_prediction_spec(self, training_model_ref: str, training_spec) -> dict:
        data = training_spec["ml_config"]["data"]
        output_columns = self.build_spec["prediction"]["output_columns"]
        features = []
        columns = ["percentile", "score"]
        for column in columns:
            if output_columns[column].get("is_feature", True):
                features.append(
                    {
                        "name": output_columns[column]["name"],
                        "description": output_columns[column].get("description", None),
                    }
                )
        if self.build_spec["prediction"].get("eligible_users", None) is not None:
            data["eligible_users"] = self.build_spec["prediction"]["eligible_users"]
        spec = {
            "entity_key": self.build_spec["entity_key"],
            "training_model": training_model_ref,
            "inputs": self.build_spec["inputs"],
            "ml_config": {
                "data": data,
                "outputs": {
                    "column_names": {
                        "percentile": output_columns["percentile"]["name"],
                        "score": output_columns["score"]["name"],
                    },
                },
            },
            "features": features,
        }
        if "ids" in self.build_spec:
            spec["ids"] = self.build_spec["ids"]
        return spec

    def get_material_recipe(self) -> PyNativeRecipe:
        return NoOpRecipe()

    def validate(self) -> Tuple[bool, str]:
        return super().validate()
