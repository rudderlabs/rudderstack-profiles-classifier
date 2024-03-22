from typing import List, Tuple

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
        connector: Connector,
        session,
        site_config_path: str,
        project_folder_path: str,
    ) -> None:
        self.pythonWHT.init(connector, session, site_config_path, project_folder_path)

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

    def get_material_names(
        self,
        start_date: str,
        end_date: str,
        features_model_name: str,
        model_hash: str,
        prediction_horizon_days: int,
        input_models: List[str],
        inputs: List[str],
    ) -> List[TrainTablesInfo]:
        return self.pythonWHT.get_material_names(
            start_date,
            end_date,
            features_model_name,
            model_hash,
            prediction_horizon_days,
            input_models,
            inputs,
        )

    def run(self, feature_package_path: str, date: str):
        self.pythonWHT.run(feature_package_path, date)

    def compute_material_name(
        self, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        return self.pythonWHT.compute_material_name(model_name, model_hash, seq_no)
    
    def get_registry_table_name(self) -> str:
        return self.pythonWHT.get_registry_table_name()
