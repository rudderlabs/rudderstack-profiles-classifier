import ast
from functools import reduce
import os
import json
import shutil
import numpy as np
import pandas as pd
from pathlib import Path
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Iterable, List, Tuple, Any, Union, Optional, Sequence, Dict

from ..utils import utils
from ..utils import constants
from ..utils.logger import logger
from ..wht.rudderPB import MATERIAL_PREFIX
from .Connector import Connector
from .wh.profiles_connector import ProfilesConnector

local_folder = constants.LOCAL_STORAGE_DIR


class CommonWarehouseConnector(Connector):
    def __init__(self, creds: dict, folder_path: str, data_type_mapping: dict) -> None:
        super().__init__(creds)
        self.local_dir = os.path.join(folder_path, local_folder)
        path = Path(self.local_dir)
        path.mkdir(parents=True, exist_ok=True)
        self.data_type_mapping = data_type_mapping
        return

    def get_local_dir(self) -> str:
        return self.local_dir

    def join_file_path(self, file_name: str) -> str:
        """Joins the given file name to the local data folder path."""
        return os.path.join(self.local_dir, file_name)

    def call_procedure(self, *args, **kwargs):
        args = list(args)
        train_function = args.pop(0)
        return train_function(*args, **kwargs)

    def write_joined_input_table(self, query, table_name):
        table_path = f"{self.schema}.{table_name}"
        drop_table_query = f"DROP TABLE IF EXISTS {table_path};"
        create_temp_table_query = f"""
                                    CREATE TABLE {table_path} AS
                                    {query} ;
                                """

        for query in [drop_table_query, create_temp_table_query]:
            self.run_query(query, response=False)

    def transform_arraytype_features(
        self,
        feature_df: pd.DataFrame,
        arraytype_features: List[str],
        top_k_array_categories,
        **kwargs,
    ) -> Union[List[str], pd.DataFrame]:
        """Transforms arraytype features in a pandas DataFrame by expanding the arraytype features
        as {feature_name}_{unique_value} columns and perform numeric encoding based on their count in those cols.
        """

        predict_arraytype_features = kwargs.get("predict_arraytype_features", {})

        transformed_dfs = []
        transformed_feature_df = feature_df.copy()
        transformed_array_col_names = []

        # Group by columns excluding arraytype features
        group_by_cols = [
            col for col in feature_df.columns if col not in arraytype_features
        ]

        for array_col_name in arraytype_features:
            feature_df[array_col_name] = feature_df[array_col_name].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) else x
            )
            feature_df[array_col_name] = feature_df[array_col_name].apply(
                lambda arr: [x.lower() for x in arr] if isinstance(arr, list) else arr
            )

            # Get rows with empty or null arrays
            empty_list_rows = feature_df[
                feature_df[array_col_name].apply(
                    lambda x: x is None or (isinstance(x, list) and len(x) == 0)
                )
            ]

            # Explode arraytype column
            exploded_df = (
                feature_df[[*group_by_cols, array_col_name]]
                .explode(array_col_name)
                .rename(columns={array_col_name: "ARRAY_VALUE"})
            )

            # Group by and count occurrences
            grouped_df = (
                exploded_df.groupby(group_by_cols + ["ARRAY_VALUE"])
                .size()
                .reset_index(name="COUNT")
            )

            unique_values = grouped_df["ARRAY_VALUE"].dropna().unique()
            top_k_array_categories = min(top_k_array_categories, len(unique_values))

            # Select top k most frequent values
            top_values = (
                grouped_df.groupby("ARRAY_VALUE")["COUNT"]
                .sum()
                .nlargest(top_k_array_categories)
                .index
            )

            predict_top_values = predict_arraytype_features.get(array_col_name, [])
            if len(predict_top_values) != 0:
                top_values = [
                    item[len(array_col_name) :].strip("_").lower()
                    for item in predict_top_values
                    if "OTHERS" not in item
                ]

                unique_values = list(set(unique_values) | set(top_values))

            other_values = set(grouped_df["ARRAY_VALUE"]) - set(top_values)

            new_array_column_names = [
                f"{array_col_name}_{value}".upper().strip() for value in unique_values
            ]
            other_column_name = f"{array_col_name}_OTHERS".upper()

            # Pivot the DataFrame to create new columns for each unique value
            pivoted_df = pd.pivot_table(
                grouped_df,
                index=group_by_cols,
                columns="ARRAY_VALUE",
                values="COUNT",
                fill_value=0,
            ).reset_index()

            for value in top_values:
                if value not in pivoted_df.columns:
                    pivoted_df[value] = 0

            # Join with rows having empty or null arrays, and fill NaN values with 0
            joined_df = empty_list_rows.merge(pivoted_df, on=group_by_cols, how="outer")
            for value in top_values:
                joined_df[value] = joined_df[value].fillna(0)

            joined_df.drop(columns=arraytype_features, inplace=True)

            rename_dict = {
                old_name: new_name
                for old_name, new_name in zip(unique_values, new_array_column_names)
            }
            joined_df = joined_df.rename(columns=rename_dict)
            joined_df[other_column_name] = sum(
                joined_df[f"{array_col_name}_{col}".upper()] for col in other_values
            )

            for old_name in unique_values:
                if old_name not in other_values:
                    transformed_array_col_names.append(rename_dict[old_name])
            transformed_array_col_names.append(other_column_name)

            joined_df.drop(
                columns=[f"{array_col_name}_{col}".upper() for col in other_values],
                inplace=True,
            )

            transformed_dfs.append(joined_df)

        if transformed_dfs:
            transformed_feature_df = reduce(
                lambda left, right: pd.merge(left, right, on=group_by_cols),
                transformed_dfs,
            )

        return transformed_array_col_names, transformed_feature_df

    def transform_booleantype_features(
        self, feature_df: pd.DataFrame, booleantype_features: List[str]
    ) -> pd.DataFrame:
        for boolean_column in booleantype_features:
            feature_df[boolean_column] = feature_df[boolean_column].astype(float)

        return feature_df

    def get_merged_table(self, base_table, incoming_table):
        return pd.concat([base_table, incoming_table], axis=0, ignore_index=True)

    def fetch_processor_mode(
        self, user_preference_order_infra: List[str], is_rudder_backend: bool
    ) -> str:
        # mode = (
        #     constants.RUDDERSTACK_MODE
        #     if is_rudder_backend
        #     else user_preference_order_infra[0]
        # )
        return constants.LOCAL_MODE

    def compute_udf_name(self, model_path: str) -> None:
        return

    def is_valid_table(self, table_name: str) -> bool:
        try:
            self.run_query(f"select * from {table_name} limit 1")
            return True
        except:
            return False

    def check_table_entry_in_material_registry(
        self, registry_table_name: str, material: dict
    ) -> bool:
        """
        Checks wether an entry is there in the material registry for the given
        material table name and wether its sucessfully materialised or not as well
        """
        material_registry_table = self.get_material_registry_table(registry_table_name)
        result = material_registry_table.loc[
            (
                material_registry_table["model_name"].str.lower()
                == material["model_name"].lower()
            )
            & (
                material_registry_table["model_hash"].str.lower()
                == material["model_hash"].lower()
            )
            & (material_registry_table["seq_no"] == material["seq_no"])
        ]
        row_count = result.shape[0]
        return row_count != 0

    def get_table(self, table_name: str, **kwargs) -> pd.DataFrame:
        """Fetches the table with the given name from the schema as a pandas Dataframe object."""
        return self.get_table_as_dataframe(self.session, table_name, **kwargs)

    def _create_get_table_query(self, table_name, **kwargs):
        filter_condition = kwargs.get("filter_condition", "")
        query = f"SELECT * FROM {table_name}"
        if filter_condition and filter_condition != "*":
            query += f" WHERE {filter_condition}"
        query += ";"
        return query

    def load_and_delete_json(self, json_file_name: str) -> dict:
        file_path = os.path.join(self.local_dir, json_file_name)
        with open(file_path, "r") as file:
            json_data = json.load(file)
        utils.delete_file(file_path)
        return json_data

    def send_table_to_train_env(self, table, **kwargs) -> Any:
        """Sends the given snowpark table to the training env(ie. local env) with the name as given.
        Therefore, no usecase for this function in case of Redshift/BigQuery."""
        pass

    def write_table(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:
        """Writes the given pandas dataframe to the warehouse schema with the given name.
        Also, giving 'local' as False (default value is True) will not write the table locally.
        """
        if kwargs.pop("local", True):
            self.write_table_locally(df, table_name)
        self.write_pandas(df, table_name, **kwargs)

    def write_pandas(self, df: pd.DataFrame, table_name_remote: str, **kwargs) -> None:
        rs_conn = ProfilesConnector(self.creds, **kwargs)
        if_exists = kwargs.get("if_exists", "append")
        rs_conn.write_to_table(
            df, table_name_remote, schema=self.schema, if_exists=if_exists
        )

    def label_table(
        self,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ) -> pd.DataFrame:
        """Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively."""

        def _replace_na(value):
            return np.nan if pd.isna(value) else value

        feature_table = self.get_table(label_table_name)
        if label_value is not None:
            feature_table[label_column] = feature_table[label_column].apply(_replace_na)
            feature_table[label_column] = np.where(
                feature_table[label_column] == label_value, 1, 0
            )
        label_table = feature_table[[entity_column, label_column]]
        return label_table

    def save_file(self, *args, **kwargs):
        """Function needed only for Snowflake Connector, hence an empty function here."""
        pass

    def fetch_given_data_type_columns(
        self,
        schema_fields: List,
        required_data_types: Tuple,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        """Fetches the column names from the given schema_fields based on the required data types (exclude label and entity columns)"""
        return {
            field.name: field.field_type
            for field in schema_fields
            if any(
                data_type in field.field_type
                for data_type in required_data_types.keys()
            )
            and field.name.lower() not in (label_column.lower(), entity_column.lower())
        }

    def get_numeric_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["numeric"],
            label_column,
            entity_column,
        )

    def get_stringtype_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["categorical"],
            label_column,
            entity_column,
        )

    def get_arraytype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["arraytype"],
            label_column,
            entity_column,
        )

    def get_timestamp_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["timestamp"],
            label_column,
            entity_column,
        )

    def get_booleantype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["booleantype"],
            label_column,
            entity_column,
        )

    def get_high_cardinal_features(
        self,
        table: pd.DataFrame,
        categorical_columns: List[str],
        label_column,
        entity_column,
        cardinal_feature_threshold,
    ) -> List[str]:
        high_cardinal_features = list()
        lower_categorical_features = [col.lower() for col in categorical_columns]
        for field in table.columns:
            if (field.lower() in lower_categorical_features) and (
                field.lower() not in (label_column.lower(), entity_column.lower())
            ):
                feature_data = table[field]
                total_rows = len(feature_data)
                top_10_freq_sum = sum(feature_data.value_counts().head(10))
                if top_10_freq_sum < cardinal_feature_threshold * total_rows:
                    high_cardinal_features.append(field)
        return high_cardinal_features

    def get_default_label_value(
        self, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        label_value = list()
        table = self.get_table(table_name)
        distinct_labels = table[label_column].unique()

        if len(distinct_labels) != 2:
            raise Exception("The feature to be predicted should be boolean")
        for e in distinct_labels:
            if e in positive_boolean_flags:
                label_value.append(e)

        if len(label_value) == 0:
            raise Exception(
                f"Label column {label_column} doesn't have any positive flags. Please provide custom label_value from label_column to bypass the error."
            )
        elif len(label_value) > 1:
            raise Exception(
                f"Label column {label_column} has multiple positive flags. Please provide custom label_value out of {label_value} to bypass the error."
            )
        return label_value[0]

    def fetch_filtered_table(
        self,
        df,
        model_name,
        model_hash,
        start_time,
        end_time,
        columns,
    ):
        filtered_df = (
            df.loc[
                (df["model_name"].str.lower() == model_name.lower())
                & (df["model_hash"].str.lower() == model_hash.lower())
                & (df["end_ts"].dt.date >= pd.to_datetime(start_time).date())
                & (df["end_ts"].dt.date <= pd.to_datetime(end_time).date()),
                columns.keys(),
            ]
            .drop_duplicates()
            .rename(columns=columns)
        )
        return filtered_df

    def join_feature_label_tables(
        self,
        registry_table_name: str,
        model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        prediction_horizon_days: int,
    ) -> Iterable:
        df = self.get_material_registry_table(registry_table_name)
        feature_df = self.fetch_filtered_table(
            df,
            model_name,
            model_hash,
            start_time,
            end_time,
            columns={"seq_no": "FEATURE_SEQ_NO", "end_ts": "FEATURE_END_TS"},
        )
        required_feature_cols = feature_df.columns.to_list()
        feature_df["TEMP_LABEL_END_TS"] = feature_df["FEATURE_END_TS"] + timedelta(
            days=prediction_horizon_days
        )

        label_start_time = datetime.strptime(
            start_time, constants.MATERIAL_DATE_FORMAT
        ) + timedelta(days=prediction_horizon_days)
        label_end_time = datetime.strptime(
            end_time, constants.MATERIAL_DATE_FORMAT
        ) + timedelta(days=prediction_horizon_days)
        label_df = self.fetch_filtered_table(
            df,
            model_name,
            model_hash,
            label_start_time,
            label_end_time,
            columns={"seq_no": "LABEL_SEQ_NO", "end_ts": "LABEL_END_TS"},
        )
        required_label_cols = label_df.columns.to_list()

        feature_label_df = pd.merge(
            feature_df,
            label_df,
            left_on=feature_df["TEMP_LABEL_END_TS"].dt.date,
            right_on=label_df["LABEL_END_TS"].dt.date,
            how="outer",
        ).replace({np.nan: None})
        feature_label_df_merged = feature_label_df[
            utils.merge_lists_to_unique(required_feature_cols, required_label_cols)
        ].iterrows()
        result = []
        for _, row in feature_label_df_merged:
            result.append(row)
        return result

    def get_old_prediction_table(
        self,
        lookahead_days: int,
        current_date: str,
        model_name: str,
        material_registry: str,
    ):
        past_predictions_end_date = utils.date_add(current_date, -lookahead_days)
        df = self.get_material_registry_table(material_registry)

        try:
            past_predictions_info = (
                df[
                    (df["model_name"] == model_name)
                    & (df["model_type"] == "python_model")
                    & (
                        df["end_ts"].dt.date
                        == pd.to_datetime(past_predictions_end_date).date()
                    )
                ]
                .sort_values(by="creation_ts", ascending=False)
                .iloc[0]
            )
        except IndexError:
            raise Exception(
                f"No past predictions found for model {model_name} before {past_predictions_end_date}"
            )

        predictions_table_name = (
            f"{MATERIAL_PREFIX}{model_name}"
            + "_"
            + f'{past_predictions_info["model_hash"]}'
            + "_"
            + f'{past_predictions_info["seq_no"]}'
        )
        return predictions_table_name

    def get_previous_predictions_info(
        self, prev_pred_ground_truth_table, score_column, label_column
    ):
        single_row = prev_pred_ground_truth_table.iloc[0]
        model_id = single_row["model_id"]
        valid_at = single_row["valid_at"]
        score_and_ground_truth_df = prev_pred_ground_truth_table[
            [score_column, label_column]
        ]
        return score_and_ground_truth_df, model_id, valid_at

    def get_creation_ts(
        self,
        material_table: str,
        model_hash: str,
        entity_key: str,
    ):
        """Retrieves the latest creation timestamp for a specific model hash, and entity key."""
        redshift_df = self.get_material_registry_table(material_table)
        try:
            temp_hash_vector = (
                redshift_df.query(f'model_hash.str.lower() == "{model_hash.lower()}"')
                .query(f'entity_key.str.lower() == "{entity_key.lower()}"')
                .sort_values(by="creation_ts", ascending=False)
                .reset_index(drop=True)[["creation_ts"]]
                .iloc[0]
            )

            creation_ts = temp_hash_vector["creation_ts"]
        except:
            raise Exception(
                f"Project is never materialzied with model hash {model_hash}."
            )
        return creation_ts.tz_localize(None)

    def get_latest_seq_no_from_registry(
        self, material_table: str, model_hash: str, model_name: str
    ) -> int:
        redshift_df = self.get_material_registry_table(material_table)
        try:
            temp_hash_vector = (
                redshift_df.query(f'model_hash.str.lower() == "{model_hash.lower()}"')
                .query(f'model_name.str.lower() == "{model_name.lower()}"')
                .sort_values(by="creation_ts", ascending=False)
                .reset_index(drop=True)[["seq_no"]]
                .iloc[0]
            )
            seq_no = temp_hash_vector["seq_no"]
        except:
            raise Exception(
                f"Error occured while fetching latest seq_no from registry table. Project is never materialzied with model hash {model_hash}."
            )
        return int(seq_no)

    def get_model_hash_from_registry(
        self, material_table: str, model_name: str, seq_no: int
    ) -> str:
        material_registry_df = self.get_material_registry_table(material_table)
        try:
            temp_hash_vector = (
                material_registry_df.query(
                    f'model_name.str.lower() == "{model_name.lower()}"'
                )
                .query(f"seq_no == {seq_no}")
                .sort_values(by="creation_ts", ascending=False)
                .reset_index(drop=True)[["model_hash"]]
                .iloc[0]
            )
            model_hash = temp_hash_vector["model_hash"]
        except:
            raise Exception(
                f"Error occurred while fetching model hash from registry table. \
                    No material found with name {model_name} and seq no {seq_no}"
            )
        return model_hash

    def add_index_timestamp_colum_for_predict_data(
        self, predict_data: pd.DataFrame, index_timestamp: str, end_ts: str
    ) -> pd.DataFrame:
        predict_data[index_timestamp] = pd.to_datetime(end_ts)
        return predict_data

    def fetch_staged_file(
        self,
        stage_name: str,
        file_name: str,
        target_folder: str,
    ) -> None:
        source_path = self.join_file_path(file_name)
        target_path = os.path.join(target_folder, file_name)
        shutil.move(source_path, target_path)

    def drop_cols(self, table: pd.DataFrame, col_list: list) -> pd.DataFrame:
        ignore_features_upper = [col.upper() for col in col_list]
        ignore_features_lower = [col.lower() for col in col_list]
        ignore_features_ = [
            col
            for col in table.columns
            if col in ignore_features_upper or col in ignore_features_lower
        ]
        return table.drop(columns=ignore_features_)

    def filter_feature_table(
        self,
        feature_table: pd.DataFrame,
        max_row_count: int,
    ) -> pd.DataFrame:
        if len(feature_table) <= max_row_count:
            return feature_table
        else:
            return feature_table.sample(n=max_row_count)

    def check_for_classification_data_requirement(
        self,
        materials: List[constants.TrainTablesInfo],
        label_column: str,
        label_value: str,
        entity_key: str,
        filter_condition: str = None,
    ) -> bool:
        total_negative_samples = 0
        total_samples = 0
        where_condition = ""
        if filter_condition and filter_condition != "*":
            where_condition = f" WHERE {filter_condition}"

        for m in materials:
            query_str = f"""SELECT COUNT(*) FROM (SELECT {entity_key}
                FROM {m.feature_table_name}{where_condition}) a
                INNER JOIN (SELECT * FROM {m.label_table_name}{where_condition}) b ON a.{entity_key} = b.{entity_key}
                WHERE b.{label_column} != {label_value}"""

            result = self.run_query(query_str, response=True)

            if len(result) != 0:
                total_negative_samples += result[0][0]

            query_str = f"""SELECT COUNT(*) as count
                FROM {m.label_table_name}{where_condition}"""
            result = self.run_query(query_str, response=True)

            if len(result) != 0:
                total_samples += result[0][0]

        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES
        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        min_negative_label_count = min_label_proportion * total_samples

        if (
            total_samples < min_no_of_samples
            or total_negative_samples < min_negative_label_count
        ):
            logger.get().debug(
                "Total number of samples or number of negative samples are "
                "not meeting the minimum training requirement, "
                f"total samples - {total_samples}, minimum samples required - {min_no_of_samples}, "
                f"total negative samples - {total_negative_samples}, "
                f"minimum negative samples portion required - {min_label_proportion}"
            )
            return False
        return True

    def check_for_regression_data_requirement(
        self,
        materials: List[constants.TrainTablesInfo],
        filter_condition: str = None,
    ) -> bool:
        total_samples = 0
        where_condition = ""
        if filter_condition and filter_condition != "*":
            where_condition = f" WHERE {filter_condition}"

        for material in materials:
            feature_material = material.feature_table_name
            query_str = f"""SELECT COUNT(*) as count
                FROM {feature_material}{where_condition}"""
            result = self.run_query(query_str, response=True)

            if len(result) != 0:
                total_samples += result[0][0]

        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES

        if total_samples < min_no_of_samples:
            logger.get().debug(
                "Number training samples are not meeting the minimum requirement, "
                f"total samples - {total_samples}, minimum samples required - {min_no_of_samples}"
            )
            return False

        return True

    def validate_columns_are_present(
        self,
        feature_table: pd.DataFrame,
        label_column: str,
    ) -> bool:
        # Check if label_column is present in feature_table
        if label_column not in feature_table.columns:
            raise Exception(
                f"Label column {label_column} is not present in the feature table."
            )

        if feature_table.shape[1] < 3:
            raise Exception(
                f"Feature table must have at least one column apart from the label column {label_column} and entity_column"
            )
        return True

    def validate_row_count(
        self,
        feature_table: pd.DataFrame,
        min_sample_for_training: int,
        train_table_pairs,
    ) -> bool:
        if len(feature_table) < min_sample_for_training:
            self.write_table(
                feature_table, self.feature_table_name, write_mode="overwrite"
            )

            log_message = (
                "Following are the table pairs used for creating the training data:\n"
                " Feature table name, label table name:\n"
            )
            log_message += "\n".join(
                f" {pair.feature_table_name}, {pair.label_table_name}"
                for pair in train_table_pairs
            )
            log_message += (
                f"\nThe table {self.feature_table_name} is built by joining the pairs using the entity-id, "
                "concatenating them, and applying eligible users flag. You can try different eligible users conditions to rerun the model to solve the data validation errors."
            )

            raise Exception(
                f"Insufficient data for training. Only {len(feature_table)} user records found, "
                f"while a minimum of {min_sample_for_training} user records is required.\n"
                f"For further information, you can check the table in the warehouse with the name: {self.feature_table_name}.\n"
                f"{log_message}"
            )

        return True

    def validate_class_proportions(
        self, feature_table: pd.DataFrame, label_column: str, train_table_pairs
    ) -> bool:
        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        max_label_proportion = constants.CLASSIFIER_MAX_LABEL_PROPORTION
        label_proportion = feature_table[label_column].value_counts(normalize=True)
        found_invalid_rows = (
            (label_proportion < min_label_proportion)
            | (label_proportion > max_label_proportion)
        ).any()

        if found_invalid_rows:
            self.write_table(
                feature_table, self.feature_table_name, write_mode="overwrite"
            )
            error_msg = ""
            for row in label_proportion.reset_index().values:
                error_msg += f"\tLabel: {row[0]:.0f} - users :({100 * row[1]:.2f}%)\n"

            log_message = (
                "Following are the table pairs used for creating the training data:\n"
            )
            log_message += " Feature table name, label table name:\n"
            log_message += "\n".join(
                f" {pair.feature_table_name}, {pair.label_table_name}"
                for pair in train_table_pairs
            )
            log_message += (
                f"\nThe table {self.feature_table_name} is built by joining the pairs using the entity-id, "
                "concatenating them, and applying eligible users flag. You can try different eligible users conditions to rerun the model to solve the data validation errors."
            )

            raise Exception(
                f"Label column {label_column} exhibits significant class imbalance.\n"
                f"The model cannot be trained on such a highly imbalanced dataset.\n"
                f"You can select a subset of users where the class imbalance is not as severe, such as by excluding inactive users, etc.\n"
                f"Current class proportions are as follows:\n{error_msg}"
                f"You can look for the table {self.feature_table_name} in your warehouse where the eligible users data is stored, and this imbalance is found. You can try different combinations of eligible users to see how the imbalance changes."
                f"{log_message}"
            )

        return True

    def validate_label_distinct_values(
        self,
        feature_table: pd.DataFrame,
        label_column: str,
        train_table_pairs,
    ) -> bool:
        distinct_values_count_list = feature_table[label_column].value_counts()
        num_distinct_values = len(distinct_values_count_list)
        req_distinct_values = constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES

        if num_distinct_values < req_distinct_values:
            self.write_table(
                feature_table, self.feature_table_name, write_mode="overwrite"
            )

            log_message = (
                "Following are the table pairs used for creating the training data:\n"
            )
            log_message += " Feature table name, label table name:\n"
            log_message += "\n".join(
                f" {pair.feature_table_name}, {pair.label_table_name}"
                for pair in train_table_pairs
            )
            log_message += (
                f"\nThe table {self.feature_table_name} is built by joining the pairs using the entity-id, "
                "concatenating them, and applying eligible users flag. You can try different eligible users conditions to rerun the model to solve the data validation errors."
            )

            raise Exception(
                f"Label column {label_column} has {num_distinct_values} distinct values while we expect a minimum of {req_distinct_values} values for a regression problem."
                f" Please check your label column and consider modifying the task in your Python model to 'classification' if that's a better fit."
                f"You can look for the table {self.feature_table_name} in your warehouse where the eligible users data is stored, for the distinct label count. You can try different combinations of eligible users to see how the label counts change."
                f"{log_message}"
            )

        return True

    def add_days_diff(
        self, table: pd.DataFrame, new_col: str, time_col: str, end_ts: str
    ) -> pd.DataFrame:
        """Adds a new column to the given table containing the difference in days between the given timestamp columns."""
        try:
            table["temp_1"] = pd.to_datetime(table[time_col]).dt.tz_localize(None)
        except pd.errors.OutOfBoundsDatetime as e:
            min_ts = pd.Timestamp.min
            max_ts = pd.Timestamp.max
            invalid_rows_count = (
                (table[time_col] < min_ts) | (table[time_col] > max_ts)
            ).sum()

            raise ValueError(
                f"{time_col} entity var has timestamp values outside the acceptable range of {min_ts} and {max_ts}.  \
                              This may be due to some data corruption in source tables or an error in the entity var definition.  \
                              You can exclude these users for training using the eligible_users flag.  \
                              Total rows with invalid {time_col} values: {invalid_rows_count}"
            )
        table["temp_2"] = pd.to_datetime(end_ts)
        table[new_col] = (table["temp_2"] - table["temp_1"]).dt.days
        return table.drop(columns=["temp_1", "temp_2"])

    def join_feature_table_label_table(
        self,
        feature_table: pd.DataFrame,
        label_table: pd.DataFrame,
        entity_column: str,
        join_type: str = "inner",
    ) -> pd.DataFrame:
        """
        Joins the given feature table and label table based on the given entity column.

        Args:
            feature_table (pd.DataFrame): The feature table to be joined.
            label_table (pd.DataFrame): The label table to be joined.
            entity_column (str): The name of the entity column to be used for joining.
            join_type (str): How to join the tables | Defaults to 'inner'.

        Returns:
            The table after the join action as a Pandas DataFrame object.
        """
        return feature_table.merge(label_table, on=[entity_column], how=join_type)

    def get_distinct_values_in_column(
        self, table: pd.DataFrame, column_name: str
    ) -> List:
        return table[column_name].unique()

    def get_tables_by_prefix(self, prefix: str):
        tables = list()
        registry_df = self.get_tablenames_from_schema()

        registry_df = registry_df[
            registry_df["tablename"].str.lower().str.startswith(f"{prefix.lower()}")
        ]

        for _, row in registry_df.iterrows():
            tables.append(row["tablename"])
        return tables

    def get_material_registry_table(
        self,
        material_registry_table_name: str,
    ) -> pd.DataFrame:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.
        """
        material_registry_table = self.get_table(material_registry_table_name)
        material_registry_table = material_registry_table.sort_values(
            by="creation_ts", ascending=False
        )

        def safe_parse_json(entry):
            try:
                if isinstance(
                    entry, str
                ):  # If the entry is a string(for redshift), parse it as JSON
                    entry_dict = eval(entry)
                elif isinstance(
                    entry, dict
                ):  # If the entry is already a dictionary(for bigquery), use it directly
                    entry_dict = entry

                return entry_dict.get("complete", {}).get("status")
            except:
                return None

        material_registry_table["status"] = material_registry_table["metadata"].apply(
            safe_parse_json
        )
        return material_registry_table[material_registry_table["status"] == 2]

    def generate_type_hint(
        self,
        df: pd.DataFrame,
    ):
        return None

    def call_prediction_udf(
        self,
        predict_data: pd.DataFrame,
        prediction_udf: Any,
        entity_column: str,
        index_timestamp: str,
        score_column_name: str,
        percentile_column_name: str,
        output_label_column: str,
        train_model_id: str,
        input: pd.DataFrame,
        pred_output_df_columns: Dict,
    ) -> pd.DataFrame:
        """Calls the given function for prediction and returns results of the predict function."""
        preds = predict_data[[entity_column, index_timestamp]]
        prediction_df = prediction_udf(input)

        preds[score_column_name] = prediction_df[pred_output_df_columns["score"]]

        if "label" in pred_output_df_columns:
            preds[output_label_column] = prediction_df[pred_output_df_columns["label"]]

        preds["model_id"] = train_model_id

        preds[percentile_column_name] = preds[score_column_name].rank(pct=True) * 100
        return preds

    """ The following functions are only specific to Redshift Connector and BigQuery Connector and not used by any other connector."""

    def write_table_locally(self, df: pd.DataFrame, table_name: str) -> None:
        """Writes the given pandas dataframe to the local storage with the given name."""
        table_path = os.path.join(self.local_dir, f"{table_name}.parquet.gzip")
        df.to_parquet(table_path, compression="gzip")

    def fetch_feature_df_path(self, feature_table_name: str) -> str:
        """This function will return the feature_df_path"""
        feature_df_path = os.path.join(
            self.local_dir, f"{feature_table_name}.parquet.gzip"
        )
        return feature_df_path

    def select_relevant_columns(
        self, table: pd.DataFrame, training_features_columns: Sequence[str]
    ) -> pd.DataFrame:
        # table can have columns in upper case or lower case. We need to handle both
        uppercase_list = lambda features: [feature.upper() for feature in features]
        training_features_columns = uppercase_list(training_features_columns)

        matching_columns = []
        for col in list(table):
            if col.upper() in training_features_columns:
                matching_columns.append(col)
        # Assert all columns in training_features_columns are part of matching_columns handing case sensitivity
        matching_columns_upper = [col.upper() for col in matching_columns]
        assert len(matching_columns_upper) == len(
            training_features_columns
        ), f"Expected columns {training_features_columns} not found in table {matching_columns_upper}"
        return table.filter(matching_columns)

    def make_local_dir(self) -> None:
        Path(self.local_dir).mkdir(parents=True, exist_ok=True)

    def delete_local_data_folder(self) -> None:
        try:
            shutil.rmtree(self.local_dir)
            logger.get().info("Local directory removed successfully")
        except OSError as o:
            logger.get().info("Local directory not present")
            pass

    def pre_job_cleanup(self) -> None:
        pass

    def post_job_cleanup(self) -> None:
        self.session.close()

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def run_query(self, query: str, response: bool) -> Optional[Sequence]:
        pass

    @abstractmethod
    def get_table_as_dataframe(self, _, table_name: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_tablenames_from_schema(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_create_metrics_table_query(self, metrics_df, table_name: str):
        pass
