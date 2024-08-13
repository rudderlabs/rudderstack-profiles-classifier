import os
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

from ..utils import utils

from ..utils.constants import TrainTablesInfo
from ..connectors.Connector import Connector
from .pythonWHT import PythonWHT
from profiles_rudderstack.material import WhtMaterial


class PyNativeWHT:
    def __init__(self, whtMaterial: WhtMaterial) -> None:
        self.pythonWHT = PythonWHT()
        self.whtMaterial = whtMaterial

    def init(
        self,
        connector: Connector = None,
        site_config_path: str = None,
        project_folder_path: str = None,
    ) -> None:
        self.pythonWHT.init(connector, site_config_path, project_folder_path)

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

    def extract_conditions(self, condition_string):
        pattern = r"user\.Var\('(\w+)'\)([<=!]+[0-9]+)"
        return re.findall(pattern, condition_string)

    def get_column_names(self, var_names):
        return [f"{{{{{var}.Model.DbObjectNamePrefix()}}}}" for var in var_names]

    def update_conditions(self, condition_string):
        conditions = self.extract_conditions(condition_string)
        var_names = [var[0] for var in conditions]
        column_names = self.get_column_names(var_names)

        updated_condition = condition_string
        for i, (var_name, condition) in enumerate(conditions):
            var_placeholder = f"user.Var('{var_name}'){condition}"
            updated_condition = updated_condition.replace(
                var_placeholder, f"{column_names[i]}{condition}", 1
            )

        return updated_condition

    def update_config_info(self, merged_config):
        entity = self.whtMaterial.model.entity()
        merged_config["data"]["entity_key"] = entity["Name"]
        merged_config["data"][
            "entity_column"
        ] = self.pythonWHT.connector.get_entity_column_case_corrected(
            entity["IdColumnName"]
        )
        merged_config["data"][
            "output_profiles_ml_model"
        ] = self.whtMaterial.model.name()

        merged_config["data"]["eligible_users"] = self.update_conditions(
            merged_config["data"]["eligible_users"]
        )
        # merged_config["data"]["label_column"] = self.update_conditions(
        #     merged_config["data"]["label_column"]
        # )
        return merged_config

    def get_material_names(
        self,
        start_date: str,
        end_date: str,
        entity_var_model_name: str,
        model_hash: str,
        prediction_horizon_days: int,
        input_models: List[str],
        input_material_or_selector_sql: List[str],
        feature_data_min_date_diff: int,
    ) -> List[TrainTablesInfo]:
        return self.pythonWHT.get_material_names(
            start_date,
            end_date,
            entity_var_model_name,
            model_hash,
            prediction_horizon_days,
            input_models,
            input_material_or_selector_sql,
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

    def get_input_models(
        self, input_material: List[str], entity_var_table: str
    ) -> Dict[str, Dict[str, str]]:
        return self.pythonWHT.get_input_models(input_material, entity_var_table)

    def get_credentials(self, project_path: str, site_config_path: str) -> str:
        connection_name = utils.load_yaml(
            os.path.join(project_path, "pb_project.yaml")
        )["connection"]
        connection = utils.load_yaml(site_config_path)["connections"][connection_name]
        return connection["outputs"][connection["target"]]
