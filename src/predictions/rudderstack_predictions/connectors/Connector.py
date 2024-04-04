import pandas as pd
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Union, Sequence, Optional, Dict

from ..utils import utils


class Connector(ABC):
    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format for connection to warehouses"""
        new_creds = {
            k if k != "dbname" else "database": v
            for k, v in credentials.items()
            if k != "type"
        }
        return new_creds

    def get_input_column_types(
        self,
        session,
        trainer_obj,
        table_name: str,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        """Returns a dictionary containing the input column types with keys (numeric, categorical, arraytype, timestamp) for a given table."""
        schema_fields = self.fetch_table_metadata(session, table_name)

        numeric_columns = utils.merge_lists_to_unique(
            self.get_numeric_features(schema_fields, label_column, entity_column),
            trainer_obj.prep.numeric_pipeline["numeric_columns"],
        )
        categorical_columns = utils.merge_lists_to_unique(
            self.get_stringtype_features(schema_fields, label_column, entity_column),
            trainer_obj.prep.categorical_pipeline["categorical_columns"],
        )
        arraytype_columns = self.get_arraytype_columns(
            schema_fields, label_column, entity_column
        )
        timestamp_columns = (
            self.get_timestamp_columns(schema_fields, label_column, entity_column)
            if len(trainer_obj.prep.timestamp_columns) == 0
            else trainer_obj.prep.timestamp_columns
        )
        return {
            "numeric": numeric_columns,
            "categorical": categorical_columns,
            "arraytype": arraytype_columns,
            "timestamp": timestamp_columns,
        }

    @abstractmethod
    def fetch_filtered_table(
        self,
        df,
        features_profiles_model,
        model_hash,
        start_time,
        end_time,
        columns,
    ):
        pass

    @abstractmethod
    def transform_arraytype_features(self, feature_table, input_column_types):
        pass

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def join_file_path(self, file_name: str) -> str:
        pass

    @abstractmethod
    def run_query(self, session, query: str, response: bool) -> Optional[Sequence]:
        pass

    @abstractmethod
    def call_procedure(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_merged_table(self, feature_table, feature_table_instance):
        pass

    @abstractmethod
    def fetch_processor_mode(
        self, user_preference_order_infra: List[str], is_rudder_backend: bool
    ) -> str:
        pass

    @abstractmethod
    def get_table(self, session, table_name: str, **kwargs):
        pass

    @abstractmethod
    def get_table_as_dataframe(
        self, session, table_name: str, **kwargs
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def send_table_to_train_env(self, table, **kwargs) -> Any:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def is_valid_table(self, session, table_name) -> bool:
        pass

    @abstractmethod
    def check_table_entry_in_material_registry(
        self, session, registry_table_name: str, material: dict
    ) -> bool:
        pass

    @abstractmethod
    def write_pandas(
        self,
        df: pd.DataFrame,
        table_name: str,
        auto_create_table: bool,
        overwrite: bool,
    ) -> Any:
        pass

    @abstractmethod
    def label_table(
        self,
        session,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ):
        pass

    @abstractmethod
    def save_file(
        self, session, file_name: str, stage_name: str, overwrite: bool
    ) -> Any:
        pass

    @abstractmethod
    def fetch_table_metadata(self, session, table_name: str) -> List:
        pass

    @abstractmethod
    def get_numeric_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_arraytype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_high_cardinal_features(
        self,
        feature_table,
        categorical_columns,
        label_column,
        entity_column,
        cardinal_feature_threshold,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_default_label_value(
        self, session, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        pass

    @abstractmethod
    def get_tables_by_prefix(self, session, prefix: str) -> str:
        pass

    @abstractmethod
    def get_creation_ts(
        self,
        session,
        material_table: str,
        model_hash: str,
        entity_key: str,
    ):
        pass

    @abstractmethod
    def get_latest_seq_no_from_registry(
        self, session, material_table: str, model_hash: str, model_name: str
    ) -> int:
        pass

    @abstractmethod
    def get_end_ts(
        self,
        session,
        material_table: str,
        model_name: str,
        model_hash: str,
        seq_no: int,
    ):
        pass

    @abstractmethod
    def add_index_timestamp_colum_for_predict_data(
        self, predict_data, index_timestamp: str, end_ts: str
    ):
        pass

    @abstractmethod
    def fetch_staged_file(
        self, session, stage_name: str, file_name: str, target_folder: str
    ) -> None:
        pass

    @abstractmethod
    def drop_cols(self, table, col_list: list):
        pass

    @abstractmethod
    def filter_feature_table(
        self,
        feature_table,
        entity_column: str,
        max_row_count: int,
        min_sample_for_training: int,
    ):
        pass

    @abstractmethod
    def check_for_classification_data_requirement(
        self, session, meterials, label_column, label_value
    ) -> bool:
        pass

    @abstractmethod
    def check_for_regression_data_requirement(self, session, meterials) -> bool:
        pass

    @abstractmethod
    def validate_columns_are_present(self, feature_table, label_column):
        pass

    @abstractmethod
    def validate_class_proportions(self, feature_table, label_column):
        pass

    @abstractmethod
    def validate_label_distinct_values(self, feature_table, label_column):
        pass

    @abstractmethod
    def add_days_diff(self, table, new_col, time_col, end_ts):
        pass

    @abstractmethod
    def join_feature_table_label_table(
        self, feature_table, label_table, entity_column: str
    ):
        pass

    @abstractmethod
    def get_distinct_values_in_column(self, table, column_name: str) -> List:
        pass

    @abstractmethod
    def get_material_registry_table(
        self, session: Any, material_registry_table_name: str
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def generate_type_hint(self, df: Any):
        pass

    @abstractmethod
    def call_prediction_udf(
        self,
        predict_data: Any,
        prediction_udf: Any,
        entity_column: str,
        index_timestamp: str,
        score_column_name: str,
        percentile_column_name: str,
        output_label_column: str,
        train_model_id: str,
        input: Any,
        pred_df_config: Dict,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def select_relevant_columns(self, table, training_features_columns):
        pass

    @abstractmethod
    def compute_udf_name(self, model_path: str) -> None:
        pass

    @abstractmethod
    def pre_job_cleanup(self, session) -> None:
        pass

    @abstractmethod
    def post_job_cleanup(self, session) -> None:
        pass

    @abstractmethod
    def join_feature_label_tables(
        self,
        session,
        registry_table_name: str,
        entity_var_model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        prediction_horizon_days: int,
    ) -> Iterable:
        pass
