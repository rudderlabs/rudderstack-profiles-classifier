import pandas as pd


from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class Connector(ABC):
    @abstractmethod
    def build_session(self, creds):
        pass

    @abstractmethod
    def run_query(self, session, query: str) -> None:
        pass

    @abstractmethod
    def join_file_path(self, file_name: str):
        pass

    @abstractmethod
    def get_table(self, table_name: str):
        pass

    @abstractmethod
    def get_table_as_dataframe(self, session: Any, table_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str) -> None:
        pass

    @abstractmethod
    def label_table(self, session, label_table_name, label_column, entity_column, index_timestamp, label_value, label_ts_col):
        pass

    @abstractmethod
    def save_file(self, file_name: str, stage_name: str, overwrite: bool) -> None:
        pass

    @abstractmethod
    def write_pandas(self, df: pd.DataFrame, table_name: str, auto_create_table: bool, overwrite: bool) -> None:
        pass

    @abstractmethod
    def call_procedure(self, train_procedure, remote_table_name: str, figure_names: dict, merged_config: dict):
        pass

    @abstractmethod
    def get_non_stringtype_features(self, session, feature_table_name, label_column: str, entity_column: str) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(self, session, feature_table_name: str, label_column: str, entity_column: str)-> List[str]:
        pass

    @abstractmethod
    def get_arraytype_features(self, session, table_name: str)-> list:
        pass

    @abstractmethod
    def get_timestamp_columns(self, session, table_name: str, index_timestamp)-> list:
        pass

    @abstractmethod
    def get_material_names(self, session, material_table: str, start_date: str, end_date: str,
                        package_name: str, model_name: str, model_hash: str, material_table_prefix: str, prediction_horizon_days: int,
                        output_filename: str)-> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        pass

    @abstractmethod
    def get_latest_material_hash(self, session, material_table: str, model_name:str) -> Tuple:
        pass

    @abstractmethod
    def get_arraytype_features(self, session, feature_table)->list:
        pass

    @abstractmethod
    def filter_columns(self, table, column_element):
        pass

    @abstractmethod
    def drop_cols(self, table, col_list):
        pass

    @abstractmethod
    def add_days_diff(self, table, new_col, time_col_1, time_col_2):
        pass

    @abstractmethod
    def join_feature_table_label_table(self, feature_table, label_table, entity_column):
        pass