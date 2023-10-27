import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union

import utils


class Connector(ABC):
    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format for connection to warehouses

        Args:
            credentials (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            dict: Data warehouse creadentials remapped in format that is required to create a connection to warehouse
        """
        new_creds = {k if k != 'dbname' else 'database': v for k, v in credentials.items() if k != 'type'}
        return new_creds

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def join_file_path(self, file_name: str) -> str:
        pass

    @abstractmethod
    def run_query(self, session, query: str) -> Any:
        pass
    
    @abstractmethod
    def get_table(self, session, table_name: str, **kwargs):
        pass
    
    @abstractmethod
    def get_table_as_dataframe(self, session, table_name: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def write_pandas(self, df: pd.DataFrame, table_name: str, auto_create_table: bool, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def label_table(self, session,
                    label_table_name: str, label_column: str, entity_column: str, index_timestamp: str,
                    label_value: Union[str,int,float], label_ts_col: str):
        pass

    @abstractmethod
    def save_file(self, session, file_name: str, stage_name: str, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def call_procedure(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def get_non_stringtype_features(self, feature_table, label_column: str, entity_column: str, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(self, feature_table, label_column: str, entity_column: str, **kwargs)-> List[str]:
        pass

    @abstractmethod
    def get_arraytype_features(self, session, table_name: str)-> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(self, session, table_name: str, index_timestamp: str)-> List[str]:
        pass

    @abstractmethod
    def get_material_names(self, session, material_table: str, start_date: str, end_date: str, 
                        package_name: str, model_name: str, model_hash: str, material_table_prefix: str, prediction_horizon_days: int, 
                        output_filename: str, site_config_path: str, project_folder: str)-> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        pass

    @abstractmethod
    def get_material_registry_name(self, session, table_prefix: str) -> str:
        pass

    @abstractmethod
    def get_latest_material_hash(self, session, material_table: str, model_name:str) -> Tuple:
        pass

    @abstractmethod
    def fetch_staged_file(self, session, stage_name: str, file_name: str, target_folder: str)-> None:
        pass

    @abstractmethod
    def drop_cols(self, table, col_list: list):
        pass

    @abstractmethod
    def filter_feature_table(self, feature_table, entity_column: str, index_timestamp: str, max_row_count: int):
        pass

    @abstractmethod
    def add_days_diff(self, table, new_col, time_col_1, time_col_2):
        pass

    @abstractmethod
    def join_feature_table_label_table(self, feature_table, label_table, entity_column: str):
        pass

    @abstractmethod
    def get_distinct_values_in_column(self, table, column_name: str) -> List:
        pass