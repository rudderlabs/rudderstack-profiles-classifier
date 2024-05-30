import pandas as pd
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Union, Sequence, Optional, Dict

from ..utils import utils


class Connector(ABC):
    def __init__(self, creds: dict) -> None:
        self.session = self.build_session(creds)

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
        trainer_obj,
        table_name: str,
        label_column: str,
        entity_column: str,
        ignore_features: List[str],
    ) -> Dict:
        """Returns a dictionary containing the input column types with keys (numeric, categorical, arraytype, timestamp, booleantype) for a given table."""
        lowercase_list = lambda features: [feature.lower() for feature in features]
        schema_fields = self.fetch_table_metadata(table_name)

        config_numeric_features = trainer_obj.prep.numeric_pipeline["numeric_columns"]
        config_categorical_features = trainer_obj.prep.categorical_pipeline[
            "categorical_columns"
        ]
        config_arraytype_features = trainer_obj.prep.arraytype_columns
        config_timestamp_features = trainer_obj.prep.timestamp_columns
        config_booleantype_features = trainer_obj.prep.booleantype_columns
        config_agg_columns = set(
            config_numeric_features
            + config_categorical_features
            + config_arraytype_features
            + config_timestamp_features
            + config_booleantype_features
        )

        # The get_all_columns_of_a_type is used to get all the columns of a particular type. Set has been used so that the config_agg_columns can be removed from the inferred columns so that there wont be any duplicates. Finally its converted back to list as we have to return a list of columns.
        def get_all_columns_of_a_type(get_features, columns):
            agg_columns = utils.merge_lists_to_unique(
                list(
                    set(get_features(schema_fields, label_column, entity_column))
                    - config_agg_columns
                ),
                columns,
            )
            return agg_columns

        numeric_columns = get_all_columns_of_a_type(
            self.get_numeric_features, config_numeric_features
        )
        categorical_columns = get_all_columns_of_a_type(
            self.get_stringtype_features, config_categorical_features
        )
        arraytype_columns = get_all_columns_of_a_type(
            self.get_arraytype_columns, config_arraytype_features
        )
        timestamp_columns = get_all_columns_of_a_type(
            self.get_timestamp_columns, config_timestamp_features
        )
        booleantype_columns = get_all_columns_of_a_type(
            self.get_booleantype_columns, config_booleantype_features
        )

        input_column_types = {
            "numeric": numeric_columns,
            "categorical": categorical_columns,
            "arraytype": arraytype_columns,
            "timestamp": timestamp_columns,
            "booleantype": booleantype_columns,
        }

        if ignore_features is None:
            ignore_features = []

        for column_type, columns in input_column_types.items():
            input_column_types[column_type] = [
                column
                for column in columns
                if column.lower() not in lowercase_list(ignore_features)
            ]

        return input_column_types

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
    def transform_booleantype_features(self, feature_table, input_column_types):
        pass

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def join_file_path(self, file_name: str) -> str:
        pass

    @abstractmethod
    def run_query(self, query: str, response: bool) -> Optional[Sequence]:
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
    def get_table(self, table_name: str, **kwargs):
        pass

    @abstractmethod
    def get_table_as_dataframe(
        self,
        # session is being passed as argument since "self.session" is not available in Snowpark stored procedure
        session,
        table_name: str,
        **kwargs
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def send_table_to_train_env(self, table, **kwargs) -> Any:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def is_valid_table(self, table_name) -> bool:
        pass

    @abstractmethod
    def check_table_entry_in_material_registry(
        self, registry_table_name: str, material: dict
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
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ):
        pass

    @abstractmethod
    def save_file(
        self,
        # session is being passed as argument since "self.session" is not available in Snowpark stored procedure
        session,
        file_name: str,
        stage_name: str,
        overwrite: bool,
    ) -> Any:
        pass

    @abstractmethod
    def fetch_table_metadata(self, table_name: str) -> List:
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
    def get_booleantype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_default_label_value(
        self, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        pass

    @abstractmethod
    def get_tables_by_prefix(self, prefix: str) -> str:
        pass

    @abstractmethod
    def get_creation_ts(
        self,
        material_table: str,
        model_hash: str,
        entity_key: str,
    ):
        pass

    @abstractmethod
    def get_latest_seq_no_from_registry(
        self, material_table: str, model_hash: str, model_name: str
    ) -> int:
        pass

    @abstractmethod
    def get_model_hash_from_registry(
        self, material_table: str, model_name: str, seq_no: int
    ) -> str:
        pass

    @abstractmethod
    def get_end_ts(
        self,
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
        self, stage_name: str, file_name: str, target_folder: str
    ) -> None:
        pass

    @abstractmethod
    def drop_cols(self, table, col_list: list):
        pass

    @abstractmethod
    def filter_feature_table(
        self,
        feature_table,
        max_row_count: int,
        min_sample_for_training: int,
    ):
        pass

    @abstractmethod
    def check_for_classification_data_requirement(
        self,
        materials,
        label_column,
        label_value,
        entity_column,
        filter_condition,
    ) -> bool:
        pass

    @abstractmethod
    def check_for_regression_data_requirement(
        self, materials, filter_condition
    ) -> bool:
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
        self, material_registry_table_name: str
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
        prob_th: float,
        input: Any,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def select_relevant_columns(self, table, training_features_columns):
        pass

    @abstractmethod
    def compute_udf_name(self, model_path: str) -> None:
        pass

    @abstractmethod
    def pre_job_cleanup(self) -> None:
        pass

    @abstractmethod
    def post_job_cleanup(self) -> None:
        pass

    @abstractmethod
    def join_feature_label_tables(
        self,
        registry_table_name: str,
        entity_var_model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        prediction_horizon_days: int,
    ) -> Iterable:
        pass

    @abstractmethod
    def get_old_prediction_table(
        self,
        lookahead_days: int,
        current_date: str,
        model_id: str,
        material_registry: str,
    ):
        pass

    @abstractmethod
    def get_previous_predictions_info(
        self, prev_pred_ground_truth_table, score_column, label_column
    ):
        pass
