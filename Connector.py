import pandas as pd
import snowflake.snowpark

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union


class Connector(ABC):
    @abstractmethod
    def build_session(self, creds: dict):
        pass

    @abstractmethod
    def run_query(self, session, query: str) -> Any:
        pass

    @abstractmethod
    def join_file_path(self, file_name: str) -> str:
        pass
    
    @abstractmethod
    def get_table(self, session, table_name: str) -> Union[snowflake.snowpark.Table, pd.DataFrame]:
        pass
    
    @abstractmethod
    def get_table_as_dataframe(self, session, table_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_table(self, table: Union[snowflake.snowpark.Table, pd.DataFrame], table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def label_table(self, session,
                    label_table_name: str, label_column: str, entity_column: str, index_timestamp: str,
                    label_value: Union[str,int,float], label_ts_col: str) -> Union[snowflake.snowpark.Table, pd.DataFrame]:
        pass

    @abstractmethod
    def save_file(self, session, file_name: str, stage_name: str, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def write_pandas(self, df: pd.DataFrame, table_name: str, auto_create_table: bool, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def call_procedure(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def get_material_registry_name(self, session, table_prefix: str) -> str:
        pass

    @abstractmethod
    def get_non_stringtype_features(self, feature_table: Union[str, pd.DataFrame], label_column: str, entity_column: str, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(self, feature_table: Union[str, pd.DataFrame], label_column: str, entity_column: str, **kwargs)-> List[str]:
        pass

    @abstractmethod
    def get_arraytype_features(self, session, table_name: str)-> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(self, session, table_name: str, index_timestamp: str)-> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns_from_table(self, session, table_name: str, index_timestamp: str)-> List[str]:
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
    def filter_columns(self, table: Union[snowflake.snowpark.Table, pd.DataFrame], column_element: str):
        pass
    
    @abstractmethod
    def drop_cols(self, table: Union[snowflake.snowpark.Table, pd.DataFrame], col_list: List):
        pass

    @abstractmethod
    def add_days_diff(self, table: Union[snowflake.snowpark.Table, pd.DataFrame], new_col: str, time_col_1: str, time_col_2: str):
        pass

    @abstractmethod
    def join_feature_table_label_table(self, feature_table: Union[snowflake.snowpark.Table, pd.DataFrame], label_table: Union[snowflake.snowpark.Table, pd.DataFrame], entity_column: str):
        pass
    
    @abstractmethod
    def call_prediction_procedure(self, predict_data: Union[snowflake.snowpark.Table, pd.DataFrame], prediction_procedure: Any, entity_column: str, index_timestamp: str,
                                  score_column_name: str, percentile_column_name: str, output_label_column: str, train_model_id: str,
                                  prob_th: float, input: Union[snowflake.snowpark.Table, pd.DataFrame]):
        pass