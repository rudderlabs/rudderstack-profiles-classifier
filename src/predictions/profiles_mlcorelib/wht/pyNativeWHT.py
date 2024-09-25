import os
from datetime import datetime, timedelta
from typing import List, Tuple

from ..utils import utils

from ..utils.constants import TrainTablesInfo
from ..connectors.Connector import Connector
from .pythonWHT import PythonWHT
from profiles_rudderstack.material import WhtMaterial


class PyNativeWHT:
    def __init__(
        self, whtMaterial: WhtMaterial, site_config_path: str, project_folder_path: str
    ) -> None:
        self.pythonWHT = PythonWHT(site_config_path, project_folder_path)
        self.whtMaterial = whtMaterial

    def set_connector(
        self,
        connector: Connector,
    ) -> None:
        self.pythonWHT.set_connector(connector)

    def get_date_range(
        self, creation_ts: datetime, prediction_horizon_days: int
    ) -> Tuple:
        start_date, end_date = self.whtMaterial.wht_ctx.time_info()
        if end_date is None:
            start_date, end_date = self.pythonWHT.get_date_range(
                creation_ts, prediction_horizon_days
            )
        else:
            if (start_date is None) or (
                start_date is not None
                and (end_date - start_date)
                >= timedelta(days=2 * prediction_horizon_days)
            ):
                start_date, end_date = self.pythonWHT.get_date_range(
                    end_date, prediction_horizon_days
                )
            elif start_date is not None and (end_date - start_date) < timedelta(
                days=2 * prediction_horizon_days
            ):
                raise Exception(
                    f"begin_time and end_time needs to be atleast {2*prediction_horizon_days} days apart for the predictive feature {self.whtMaterial.model.name()} with prediction_horizon_days: {prediction_horizon_days}"
                )

        return str(start_date), str(end_date)

    def get_latest_entity_var_table(self, entity_key: str) -> Tuple[str, str, str]:
        model_ref = f"entity/{entity_key}/var_table"
        material = self.whtMaterial.de_ref(model_ref)
        if material is None:
            raise Exception(f"Material not found for model ref: {model_ref}")
        material_split = self.pythonWHT.split_material_name(material.name())
        creation_ts = self.pythonWHT.get_model_creation_ts(
            material_split["model_hash"],
            entity_key,
        )
        return material_split["model_hash"], material_split["model_name"], creation_ts

    def get_column_name(self, model_ref):
        column_name = self.whtMaterial.de_ref(model_ref).model.db_object_name_prefix()
        return column_name

    def update_config_info(self, merged_config):
        entity = self.whtMaterial.model.entity()
        merged_config["data"]["entity_key"] = entity["Name"]
        merged_config["data"]["entity_column"] = (
            self.pythonWHT.connector.get_entity_column_case_corrected(
                entity["IdColumnName"]
            )
        )
        merged_config["data"][
            "output_profiles_ml_model"
        ] = self.whtMaterial.model.name()
        self.pythonWHT.connector.feature_table_name = (
            f"{self.whtMaterial.name()}_feature_table"
        )
        merged_config["data"]["label_column"] = self.get_column_name(
            merged_config["data"]["label_column"]
        )
        return merged_config

    def get_end_ts(self) -> str:
        _, end_ts = self.whtMaterial.wht_ctx.time_info()
        return str(end_ts)

    def get_material_names(
        self,
        start_date: str,
        end_date: str,
        prediction_horizon_days: int,
        inputs: List[dict],
        input_columns: List[str],
        entity_column: str,
        feature_data_min_date_diff: int,
        return_partial_pairs: bool = False,
    ) -> List[TrainTablesInfo]:
        return self.pythonWHT.get_material_names(
            start_date,
            end_date,
            prediction_horizon_days,
            inputs,
            input_columns,
            entity_column,
            False,
            feature_data_min_date_diff,
        )

    def run(self, feature_package_path: str, date: str):
        self.pythonWHT.run(feature_package_path, date)

    def compute_material_name(
        self, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        return self.pythonWHT.compute_material_name(model_name, model_hash, seq_no)

    def get_registry_table_name(self) -> str:
        return self.pythonWHT.get_registry_table_name()

    def get_latest_seq_no(self, inputs: List[dict]) -> int:
        return self.pythonWHT.get_latest_seq_no(inputs)

    def get_inputs(self, input_model_refs: List[str]) -> List[dict]:
        inputs = []
        for input in input_model_refs:
            material = self.whtMaterial.de_ref(input)
            # if material.model.model_type() == "sql_template":
            #     material = self.whtMaterial.de_ref(input + "/var_table")
            #     id_column_name = self.whtMaterial.model.entity()["IdColumnName"]
            #     self.whtMaterial.de_ref(input + f"/var_table/{id_column_name}")
            column_name = None
            if material.model.materialization()["output_type"] == "column":
                column_name = material.model.db_object_name_prefix()
                table_material_ref = material.model.encapsulating_model().model_ref()
                table_material = self.whtMaterial.de_ref(table_material_ref)
            else:
                table_material = material
            material_name_dict = self.pythonWHT.split_material_name(material.name())
            inputs.append(
                {
                    "table_name": table_material.name(),
                    "model_ref": material.model.model_ref(),
                    "model_type": material.model.model_type(),
                    "selector_sql": material.get_selector_sql(),
                    "column_name": column_name,
                    "model_name": material_name_dict["model_name"],
                    "model_hash": material_name_dict["model_hash"],
                }
            )
        return inputs

    def validate_sql_table(self, inputs, entity_column) -> None:
        for input in inputs:
            if input["model_type"] == "sql_template":
                self.pythonWHT.connector.validate_sql_table(
                    input["table_name"], entity_column
                )

    def get_credentials(self, project_path: str, site_config_path: str) -> str:
        connection_name = utils.load_yaml(
            os.path.join(project_path, "pb_project.yaml")
        )["connection"]
        connection = utils.load_yaml(site_config_path)["connections"][connection_name]
        return connection["outputs"][connection["target"]]
