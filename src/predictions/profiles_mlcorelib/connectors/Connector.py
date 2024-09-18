import pandas as pd
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any, Iterable, List, Tuple, Union, Sequence, Optional, Dict, Set

from ..utils.logger import logger


class Connector(ABC):
    def __init__(self, creds: dict) -> None:
        self.schema = None
        self.feature_table_name = None
        self.session = self.build_session(creds)

    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format for connection to warehouses"""
        new_creds = {
            k if k != "dbname" else "database": v
            for k, v in credentials.items()
            if k != "type"
        }
        return new_creds

    def validate_sql_table(self, table_name: str, entity_column: str) -> None:
        total_rows_count_query = f"select count(*) from {self.schema}.{table_name};"
        distinct_rows_count_query = (
            f"select count(distinct {entity_column}) from {self.schema}.{table_name};"
        )

        total_rows = self.run_query(total_rows_count_query)[0][0]
        distinct_rows = self.run_query(distinct_rows_count_query)[0][0]

        if total_rows != distinct_rows:
            logger.get().error(
                f"SQL model table {table_name} has duplicate values in entity column {entity_column}. Please make sure that the column {entity_column} in all SQL model has unique values only."
            )
            raise Exception(
                f"SQL model table {table_name} has duplicate values in entity column {entity_column}. Please make sure that the column {entity_column} in all SQL model has unique values only."
            )

    def _validate_common_columns(
        self,
        columns_per_input: List[Set[str]],
    ) -> None:
        all_columns = set.union(*columns_per_input)

        for column in all_columns:
            if (
                sum(
                    column in ind_input_column for ind_input_column in columns_per_input
                )
                > 1
            ):
                logger.get().error(
                    f"Common column {column} is present in 2 or more inputs. Please correct the inputs in config."
                )
                raise Exception(
                    f"Common columns are present in 2 or more inputs. Please correct the inputs in config."
                )

    def get_input_columns(self, trainer_obj, inputs):
        columns_per_input = list()

        for input_ in inputs:
            query = input_["selector_sql"] + " LIMIT 1"
            ind_input_columns = set(self.run_query(query)[0]._fields)
            ind_input_columns.difference_update(
                {
                    trainer_obj.index_timestamp,
                    trainer_obj.index_timestamp.upper(),
                    trainer_obj.index_timestamp.lower(),
                    trainer_obj.entity_column,
                    trainer_obj.entity_column.upper(),
                    trainer_obj.entity_column.lower(),
                }
            )
            columns_per_input.append(ind_input_columns)

        self._validate_common_columns(columns_per_input)
        input_columns = set.union(*columns_per_input)
        return list(input_columns)

    def _get_table_info(self, inputs):
        tables = OrderedDict()
        for input_ in inputs:
            table_name = input_["table_name"]
            if table_name not in tables:
                tables[table_name] = {
                    "column_name": [],
                }
            if input_["column_name"]:
                tables[table_name]["column_name"].append(input_["column_name"])

        return tables

    def _construct_join_query(self, entity_column, input_columns, tables):
        select_col_str = ", ".join(input_columns)
        query_parts = []
        for i, (table_name, info) in enumerate(tables.items(), start=1):
            if len(info["column_name"]) > 0:
                columns = ", ".join([entity_column] + info["column_name"])
                subquery = f"(SELECT {columns} FROM {self.schema}.{table_name})"
            else:
                subquery = f"(SELECT * FROM {self.schema}.{table_name})"

            if i == 1:
                query_parts.append(f"{subquery} t{i}")
            else:
                query_parts.append(
                    f"INNER JOIN {subquery} t{i} ON t1.{entity_column} = t{i}.{entity_column}"
                )

        query = f"""SELECT t1.{entity_column} AS {entity_column}, {select_col_str}
            FROM
                """ + "\n    ".join(
            query_parts
        )
        return query

    def join_input_tables(
        self,
        inputs: List[Dict],
        input_columns: List[str],
        entity_column: str,
        temp_joined_input_table_name: str,
    ) -> None:
        tables = self._get_table_info(inputs)
        query = self._construct_join_query(entity_column, input_columns, tables)
        self.write_joined_input_table(query, temp_joined_input_table_name)

    def drop_joined_tables(self, table_list: List[str]) -> None:
        for table in table_list:
            self.run_query(
                f"drop table if exists {self.schema}.{table};", response=False
            )

    def get_input_column_types(
        self,
        trainer_obj,
        input_columns: List[str],
        inputs: List[dict],
        table_name: str,
    ) -> Tuple:
        """Returns a dictionary containing the input column types with keys (numeric, categorical, arraytype, timestamp, booleantype) for a given table."""
        entity_column = trainer_obj.entity_column
        label_column = trainer_obj.label_column
        ignore_features = trainer_obj.prep.ignore_features

        lowercase_list = lambda features: [feature.lower() for feature in features]
        schema_fields = self.fetch_table_metadata(table_name)

        config_numeric_features = trainer_obj.prep.numeric_features
        config_categorical_features = trainer_obj.prep.categorical_features
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

        """The get_columns_of_given_datatype function retrieves all columns of a specified datatype from a dataset. 
           A set is utilized to eliminate duplicates that may arise from including config_agg_columns in the inferred columns. 
           Once duplicates are removed, the result is converted back to a list."""

        def get_all_columns_of_a_type(
            get_datatype_features_fn, config_datatype_columns
        ):
            datatype_features_dict = get_datatype_features_fn(
                schema_fields, label_column, entity_column
            )

            given_datatype_columns = {}

            for col, datatype in datatype_features_dict.items():
                if col not in config_agg_columns:
                    given_datatype_columns[col] = datatype

            for col in config_datatype_columns:
                given_datatype_columns[col] = next(
                    iter(self.data_type_mapping["numeric"].values())
                )

            return given_datatype_columns

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

        updated_input_column_types = dict()
        for column_type, columns in input_column_types.items():
            updated_input_column_types[column_type] = {
                key: value
                for key, value in columns.items()
                if key.lower() in lowercase_list(input_columns)
            }

        if ignore_features is None:
            ignore_features = []

        try:
            self.check_arraytype_conflicts(updated_input_column_types, inputs)
            ignore_features.extend(updated_input_column_types["arraytype"])
        except Exception as e:
            raise Exception(str(e))

        # Since ignore_features are applied later to arraytype_conflict check,
        # that means even if someone adds a feature in ignore_features which is arraytype,
        # it will still throw the exception if it is mentioned in inputs(which is important as well)
        # and those in feature_table_model will be ignored.
        for column_type, columns in updated_input_column_types.items():
            updated_input_column_types[column_type] = {
                key: value
                for key, value in columns.items()
                if key.lower() not in lowercase_list(ignore_features)
            }

        trainer_obj.prep.ignore_features = ignore_features
        return updated_input_column_types

    def check_arraytype_conflicts(self, updated_input_column_types, inputs):
        arraytype_columns = updated_input_column_types.get("arraytype", [])

        for column in arraytype_columns:
            column_lower = column.lower()

            for input in inputs:
                if (
                    input["column_name"] is not None
                    and column_lower == input["column_name"].lower()
                ):
                    raise Exception(
                        f"Array type features are not supported. Please remove '{column_lower}' and any other array type features from inputs."
                    )

    def get_entity_column_case_corrected(self, entity_column: str) -> str:
        """Returns the entity column. In case of redshift, even if entity_column is case-sensitive but still
        while fetching data from table in form of pandas dataframe, entity_column is converted to lowercase.
        But this is not the case in Bigquery.
        ex: given entity_column: 'Entity_Main_Id'
            Redshift pandas df will have column 'entity_main_id'
            while in Bigquery pandas df will have column 'Entity_Main_Id'
            and snowflake snowpark handles it though .select() method which is case-insensitive.
        """
        return entity_column

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
    def transform_arraytype_features(
        self, feature_table, input_column_types, top_k_array_categoriesm, **kwargs
    ):
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
    def get_entity_var_table_ref(self, table_name: str) -> str:
        """
        This fn is being used to create entity_var_table_ref for generating the input selector sql.
        for ex:
        in case of snowflake: SELECT * FROM "MATERIAL_ENTITY_VAR_MODE_HASH_SEQ"
        in case of redshift: SELECT * FROM "schema_name"."material_entity_var_mode_hash_seq"
        in case of bigquery: SELECT * FROM `schema_name`.`Material_entity_var_mode_hash_seq`
        """
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
    def write_joined_input_table(self, query: str, table_name: str) -> None:
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
        **kwargs,
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
    ) -> Dict:
        pass

    @abstractmethod
    def get_stringtype_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        pass

    @abstractmethod
    def get_arraytype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
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
    ) -> Dict:
        pass

    @abstractmethod
    def get_booleantype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
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
        input: Any,
        pred_output_df_columns: Dict,
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
        model_name: str,
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
        model_name: str,
        material_registry: str,
    ):
        pass

    @abstractmethod
    def get_previous_predictions_info(
        self, prev_pred_ground_truth_table, score_column, label_column
    ):
        pass
