import os
import json
import shutil
import numpy as np
import pandas as pd
from pathlib import Path
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Iterable, List, Any, Union, Optional, Sequence, Dict

from ..utils import utils
from ..utils import constants
from ..utils.logger import logger
from .Connector import Connector
from .wh.profiles_connector import ProfilesConnector

local_folder = constants.LOCAL_STORAGE_DIR


class CommonWarehouseConnector(Connector):
    def __init__(self, folder_path: str) -> None:
        self.local_dir = os.path.join(folder_path, local_folder)
        path = Path(self.local_dir)
        path.mkdir(parents=True, exist_ok=True)
        self.array_time_features = {}
        return

    def get_local_dir(self) -> str:
        return self.local_dir

    def join_file_path(self, file_name: str) -> str:
        """
        Joins the given file name to the local data folder path.

        Args:
            file_name (str): The name of the file to be joined to the path.

        Returns:
            The joined file path as a string.
        """
        return os.path.join(self.local_dir, file_name)

    def call_procedure(self, *args, **kwargs):
        """Calls the given function for training

        Args:
            session : connection session for warehouse access
            args (list): List of arguments to be passed to the training function
            kwargs (dict): Dictionary of keyword arguments to be passed to the training function

        Returns:
            Results of the training function
        """
        args = list(args)
        snowflake_relevent_feature_table_name = args.pop(
            2
        )  # feature_table_name of snowflake table store on warehouse. Thus, irrelevant for Redshift/BigQuery.
        del snowflake_relevent_feature_table_name

        train_function = args.pop(0)
        return train_function(*args, **kwargs)

    def get_merged_table(self, base_table, incoming_table):
        """Returns the merged table.

        Args:
            base_table (pd.DataFrame): 1st DataFrame
            incoming_table (pd.DataFrame): 2nd DataFrame

        Returns:
            pd.DataFrame: Merged table
        """
        return pd.concat([base_table, incoming_table], axis=0, ignore_index=True)

    def fetch_processor_mode(
        self, user_preference_order_infra: List[str], is_rudder_backend: bool
    ) -> str:
        mode = (
            constants.RUDDERSTACK_MODE
            if is_rudder_backend
            else user_preference_order_infra[0]
        )
        return mode

    def get_udf_name(self, model_path: str) -> str:
        """Returns the udf name using info from the model_path

        Args:
            model_path (str): Path of the model

        Returns:
            str: UDF name
        """
        return None

    def is_valid_table(self, session, table_name: str) -> bool:
        """
        Checks whether a table exists in the data warehouse.

        Args:
            session : A session for data warehouse access.
            table_name (str): The name of the table to be checked.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            self.run_query(session, f"select * from {table_name} limit 1")
            return True
        except:
            return False

    def check_table_entry_in_material_registry(
        self, session, registry_table_name: str, material: dict
    ) -> bool:
        """
        Checks wether an entry is there in the material registry for the given
        material table name and wether its sucessfully materialised or not as well
        """
        material_registry_table = self.get_material_registry_table(
            session, registry_table_name
        )
        result = material_registry_table.loc[
            (material_registry_table["model_name"] == material["model_name"])
            & (material_registry_table["model_hash"] == material["model_hash"])
            & (material_registry_table["seq_no"] == material["seq_no"])
        ]
        row_count = result.shape[0]
        return row_count != 0

    def get_table(self, session, table_name: str, **kwargs) -> pd.DataFrame:
        """Fetches the table with the given name from the Redshift schema as a pandas Dataframe object

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the table to be fetched from the schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        return self.get_table_as_dataframe(session, table_name, **kwargs)

    def _create_get_table_query(self, table_name, **kwargs):
        filter_condition = kwargs.get("filter_condition", "")
        query = f"SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ";"
        return query

    def load_and_delete_json(self, json_file_name: str) -> dict:
        file_path = os.path.join(self.local_dir, json_file_name)
        with open(file_path, "r") as file:
            json_data = json.load(file)
        utils.delete_file(file_path)
        return json_data

    def send_to_train_env(self, table, table_name_remote: str, **kwargs) -> Any:
        """Sends the given snowpark table to the training env(ie. local env) with the name as given.
        Therefore, no usecase for this function in case of Redshift/BigQuery."""
        pass

    def write_table(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:
        """Writes the given pandas dataframe to the warehouse schema with the given name.

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the warehouse schema
            table_name (str): Name with which the dataframe is to be written
        Returns:
            Nothing
        """
        if kwargs.pop("local", True):
            self.write_table_locally(df, table_name)
        self.write_pandas(df, table_name, **kwargs)

    def write_pandas(self, df: pd.DataFrame, table_name_remote: str, **kwargs) -> None:
        """Writes the given pandas dataframe to the Warehouse schema with the given name

        Args:
            df (pd.DataFrame): Pandas dataframe to be written
            table_name (str): Name with which the dataframe is to be written
            [From kwargs]
                if_exists (str): How to write the dataframe to the warehouse schema | Defaults to "append"
        Returns:
            Nothing
        """
        rs_conn = ProfilesConnector(self.creds, **kwargs)
        if_exists = kwargs.get("if_exists", "append")
        rs_conn.write_to_table(
            df, table_name_remote, schema=self.schema, if_exists=if_exists
        )

    def label_table(
        self,
        session,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ) -> pd.DataFrame:
        """
        Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively.

        Args:
            session : connection session for warehouse access
            label_table_name (str): Name of the table to be labelled
            label_column (str): Name of the column to be labelled
            entity_column (str): Name of the entity column
            label_value (Union[str,int,float]): Value to be labelled as '1'

        Returns:
            label_table (pd.DataFrame): The labelled table as a pandas Dataframe object
        """
        feature_table = self.get_table(session, label_table_name)
        if label_value is not None:
            feature_table[label_column] = np.where(
                feature_table[label_column] == label_value, 1, 0
            )
        label_table = feature_table[[entity_column, label_column]]
        return label_table

    def save_file(self, *args, **kwargs):
        """Function needed only for Snowflake Connector, hence an empty function here."""
        pass

    def get_arraytype_columns_from_table(self, table: pd.DataFrame, **kwargs) -> list:
        """Returns the list of features to be ignored from the feature table.
        Args:
            table (pd.DataFrame): warehouse table.
        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        self.get_array_time_features_from_file(**kwargs)
        arraytype_columns = self.array_time_features["arraytype_columns"]
        return arraytype_columns

    def get_high_cardinal_features(
        self,
        table: pd.DataFrame,
        label_column,
        entity_column,
        cardinal_feature_threshold,
    ) -> List[str]:
        high_cardinal_features = list()
        for field in table.columns:
            if (
                table[field].dtype not in ("int64", "float64", "Int64", "Float64")
            ) and (field.lower() not in (label_column.lower(), entity_column.lower())):
                feature_data = table[field]
                total_rows = len(feature_data)
                top_10_freq_sum = sum(feature_data.value_counts().head(10))
                if top_10_freq_sum < cardinal_feature_threshold * total_rows:
                    high_cardinal_features.append(field)
        return high_cardinal_features

    def get_timestamp_columns_from_table(
        self, table: pd.DataFrame, **kwargs
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        kwargs.get("features_path", None)
        timestamp_columns = self.array_time_features["timestamp_columns"]
        return timestamp_columns

    def get_default_label_value(
        self, session, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        label_value = list()
        table = self.get_table(session, table_name)
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
        features_profiles_model,
        model_hash,
        start_time,
        end_time,
        columns,
    ):
        filtered_df = (
            df.loc[
                (df["model_name"] == features_profiles_model)
                & (df["model_hash"] == model_hash)
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
        session,
        registry_table_name: str,
        features_model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        prediction_horizon_days: int,
    ) -> Iterable:
        df = self.get_material_registry_table(session, registry_table_name)
        feature_df = self.fetch_filtered_table(
            df,
            features_model_name,
            model_hash,
            start_time,
            end_time,
            columns={"seq_no": "FEATURE_SEQ_NO", "end_ts": "FEATURE_END_TS"},
        )
        required_feature_cols = feature_df.columns.to_list()
        feature_df["TEMP_LABEL_END_TS"] = feature_df["FEATURE_END_TS"] + timedelta(
            days=prediction_horizon_days
        )

        time_format = "%Y-%m-%d"
        label_start_time = datetime.strptime(start_time, time_format) + timedelta(
            days=prediction_horizon_days
        )
        label_end_time = datetime.strptime(end_time, time_format) + timedelta(
            days=prediction_horizon_days
        )
        label_df = self.fetch_filtered_table(
            df,
            features_model_name,
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

    def get_creation_ts(
        self,
        session,
        material_table: str,
        model_hash: str,
        entity_key: str,
    ):
        """This function will return the model hash that is latest for given model name in material table

        Args:
            session : connection session for warehouse access
            material_table (str): name of material registry table
            model_hash (str): latest model hash
            entity_key (str): entity key

        Returns:
            (): it's latest creation timestamp
        """
        redshift_df = self.get_material_registry_table(session, material_table)
        try:
            temp_hash_vector = (
                redshift_df.query(f'model_hash == "{model_hash}"')
                .query(f'entity_key == "{entity_key}"')
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

    def get_end_ts(
        self, session, material_table, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        """This function will return the end_ts with given model hash and model name

        Args:
            session : connection session for warehouse access
            material_table (str): name of material registry table
            model_name (str): model_name to be searched in material registry table
            model_hash (str): latest model hash
            seq_no (int): latest seq_no

        Returns:
            str: end_ts for given model hash and model name
        """
        df = self.get_material_registry_table(session, material_table)

        try:
            feature_table_info_df = (
                df[
                    (df["model_name"] == model_name)
                    & (df["model_hash"] == model_hash)
                    & (df["seq_no"] == seq_no)
                ]
                .reset_index(drop=True)[["end_ts"]]
                .iloc[0]
            )

            end_ts = feature_table_info_df["end_ts"]
        except Exception as e:
            raise Exception(
                f"Project is never materialzied with model hash {model_hash}. Error message: {e}"
            )

        return end_ts.tz_localize(None)

    def add_index_timestamp_colum_for_predict_data(
        self, predict_data: pd.DataFrame, index_timestamp: str, end_ts: str
    ) -> pd.DataFrame:
        """This function will add index timestamp column to predict data

        Args:
            predict_data (pd.DataFrame): Dataframe to be predicted
            index_timestamp (str): Name of the index timestamp column
            end_ts (str): end timestamp value to calculate the difference.

        Returns:
            pd.DataFrame: Dataframe with index timestamp column
        """
        predict_data[index_timestamp] = pd.to_datetime(end_ts)
        return predict_data

    def fetch_staged_file(
        self,
        session,
        stage_name: str,
        file_name: str,
        target_folder: str,
    ) -> None:
        """Fetches the given file from the given stage and saves it to the given target folder.

        Args:
            session : connection session for warehouse access
            stage_name (str): Name of the stage from which to fetch the file.
            file_name (str): Name of the file to be fetched.
            target_folder (str): Path to the folder where the fetched file is to be saved.

        Returns:
            Nothing
        """
        source_path = self.join_file_path(file_name)
        target_path = os.path.join(target_folder, file_name)
        shutil.move(source_path, target_path)

    def drop_cols(self, table: pd.DataFrame, col_list: list) -> pd.DataFrame:
        """
        Drops the columns in the given list from the given table.

        Args:
            table (pd.DataFrame): The table to be filtered.
            col_list (list): The list of columns to be dropped.

        Returns:
            The table after the columns have been dropped as a Pandas DataFrame object.
        """
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
        entity_column: str,
        max_row_count: int,
        min_sample_for_training: int,
    ) -> pd.DataFrame:
        """
        Sorts the given feature table based on the given entity column and index timestamp.

        Args:
            feature_table (pd.DataFrame): The table to be filtered.
            entity_column (str): The name of the entity column to be used for sorting.

        Returns:
            The sorted feature table as a Pandas DataFrame object.
        """
        feature_table["row_num"] = feature_table.groupby(entity_column).cumcount() + 1
        feature_table = feature_table[feature_table["row_num"] == 1]
        feature_table = feature_table.sort_values(
            by=[entity_column], ascending=[True]
        ).drop(columns=["row_num"])
        feature_table_filtered = feature_table.head(max_row_count)
        if len(feature_table_filtered) < min_sample_for_training:
            raise Exception(
                f"Insufficient data for training. Only {len(feature_table_filtered)} user records found. Required minimum {min_sample_for_training} user records."
            )
        return feature_table_filtered

    def check_for_classification_data_requirement(
        self,
        session,
        materials: List[constants.TrainTablesInfo],
        label_column: str,
        label_value: str,
    ) -> bool:
        total_negative_samples = 0
        total_samples = 0
        for material in materials:
            label_material = material.label_table_name
            query_str = f"""SELECT COUNT(*) as count
                FROM {label_material}
                WHERE {label_column} != {label_value}"""

            result = self.run_query(session, query_str, response=True)

            if len(result) != 0:
                total_negative_samples += result[0][0]

            query_str = f"""SELECT COUNT(*) as count
                FROM {label_material}"""
            result = self.run_query(session, query_str, response=True)

            if len(result) != 0:
                total_samples += result[0][0]

        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES
        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        min_negative_label_count = min_label_proportion * total_samples

        if (
            total_samples < min_no_of_samples
            or total_negative_samples < min_negative_label_count
        ):
            logger.debug(
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
        session,
        materials: List[constants.TrainTablesInfo],
    ) -> bool:
        total_samples = 0
        for material in materials:
            feature_material = material.feature_table_name
            query_str = f"""SELECT COUNT(*) as count
                FROM {feature_material}"""
            result = self.run_query(session, query_str, response=True)

            if len(result) != 0:
                total_samples += result[0][0]

        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES

        if total_samples < min_no_of_samples:
            logger.debug(
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

    def validate_class_proportions(
        self,
        feature_table: pd.DataFrame,
        label_column: str,
    ) -> bool:
        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        max_label_proportion = constants.CLASSIFIER_MAX_LABEL_PROPORTION
        label_proportion = feature_table[label_column].value_counts(normalize=True)
        found_invalid_rows = (
            (label_proportion < min_label_proportion)
            | (label_proportion > max_label_proportion)
        ).any()
        if found_invalid_rows:
            raise Exception(
                f"Label column {label_column} has invalid proportions. \
                        Please check if the label column has valid labels."
            )
        return True

    def validate_label_distinct_values(
        self,
        feature_table: pd.DataFrame,
        label_column: str,
    ) -> bool:
        distinct_values_count_list = feature_table[label_column].value_counts()
        if (
            len(distinct_values_count_list)
            < constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES
        ):
            raise Exception(
                f"Label column {label_column} has invalid number of distinct values. \
                    Please check if the label column has valid labels."
            )
        return True

    def add_days_diff(
        self, table: pd.DataFrame, new_col: str, time_col: str, end_ts: str
    ) -> pd.DataFrame:
        """
        Adds a new column to the given table containing the difference in days between the given timestamp columns.

        Args:
            table (pd.DataFrame): The table to be filtered.
            new_col (str): The name of the new column to be added.
            time_col (str): The name of the first timestamp column.
            end_ts (str): end timestamp value to calculate the difference.

        Returns:
            The table with the new column added as a Pandas DataFrame object.
        """
        table["temp_1"] = pd.to_datetime(table[time_col]).dt.tz_localize(None)
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
        """Returns the distinct values in the given column of the given table.

        Args:
            table (pd.DataFrame): The dataframe from which the distinct values are to be extracted.
            column_name (str): The name of the column from which the distinct values are to be extracted.

        Returns:
            List: The list of distinct values in the given column of the given table.
        """
        return table[column_name].unique()

    def get_tables_by_prefix(self, session, prefix: str):
        tables = list()
        registry_df = self.get_tablenames_from_schema(session)

        registry_df = registry_df[
            registry_df["tablename"].str.lower().str.startswith(f"{prefix.lower()}")
        ]

        for _, row in registry_df.iterrows():
            tables.append(row["tablename"])
        return tables

    def get_material_registry_table(
        self,
        session,
        material_registry_table_name: str,
    ) -> pd.DataFrame:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.

        Args:
            session : connection session for warehouse access
            material_registry_table_name (str): The material registry table name.

        Returns:
            pd.DataFrame: The filtered material registry table containing only the successfully materialized data.
        """
        material_registry_table = self.get_table(session, material_registry_table_name)

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

    # TODO: checked this fn. Should be correct. Will make sure after BigQuery run.
    def generate_type_hint(self, df: pd.DataFrame, column_types: Dict[str, List[str]]):
        types = []
        cat_columns = [col.lower() for col in column_types["categorical_columns"]]
        numeric_columns = [col.lower() for col in column_types["numeric_columns"]]
        for col in df.columns:
            if col.lower() in cat_columns:
                types.append(str)
            elif col.lower() in numeric_columns:
                types.append(float)
            else:
                raise Exception(
                    f"Column {col} not found in the training data config either as categorical or numeric column"
                )
        return types

    # TODO: checked this fn. Should be correct. Will make sure after BigQuery run.
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
        prob_th: Optional[float],
        input: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calls the given function for prediction

        Args:
            predict_data (pd.DataFrame): Dataframe to be predicted
            prediction_udf (Any): Function for prediction
            entity_column (str): Name of the entity column
            index_timestamp (str): Name of the index timestamp column
            score_column_name (str): Name of the score column
            percentile_column_name (str): Name of the percentile column
            output_label_column (str): Name of the output label column
            train_model_id (str): Model id
            prob_th (float): Probability threshold
            input (pd.DataFrame): Input dataframe
        Returns:
            Results of the predict function
        """
        preds = predict_data[[entity_column, index_timestamp]]
        preds[score_column_name] = prediction_udf(input)
        preds["model_id"] = train_model_id
        if prob_th:
            preds[output_label_column] = preds[score_column_name].apply(
                lambda x: True if x >= prob_th else False
            )
        preds[percentile_column_name] = preds[score_column_name].rank(pct=True) * 100
        return preds

    """ The following functions are only specific to Redshift Connector and BigQuery Connector and not used by any other connector."""

    def write_table_locally(self, df: pd.DataFrame, table_name: str) -> None:
        """Writes the given pandas dataframe to the local storage with the given name.

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the local storage
            table_name (str): Name with which the dataframe is to be written to the local storage

        Returns:
            Nothing
        """
        table_path = os.path.join(self.local_dir, f"{table_name}.parquet.gzip")
        df.to_parquet(table_path, compression="gzip")

    def get_array_time_features_from_file(self, **kwargs):
        """This function will read the arraytype features and timestamp columns from the given file."""
        if len(self.array_time_features) != 0:
            return
        features_path = kwargs.get("features_path", None)
        if features_path == None:
            raise ValueError("features_path argument is required for Redshift")
        with open(features_path, "r") as f:
            column_names = json.load(f)
            self.array_time_features["arraytype_columns"] = column_names[
                "arraytype_columns"
            ]
            self.array_time_features["timestamp_columns"] = column_names[
                "timestamp_columns"
            ]

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
        "Created a local directory to store temporary files"
        Path(self.local_dir).mkdir(parents=True, exist_ok=True)

    def _delete_local_data_folder(self) -> None:
        """Deletes the local data folder."""
        try:
            shutil.rmtree(self.local_dir)
            logger.info("Local directory removed successfully")
        except OSError as o:
            logger.info("Local directory not present")
            pass

    def cleanup(self, *args, **kwargs) -> None:
        delete_local_data = kwargs.get("delete_local_data", None)
        if delete_local_data:
            self._delete_local_data_folder()

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def run_query(self, session, query: str, response: bool) -> Optional[Sequence]:
        pass

    @abstractmethod
    def get_table_as_dataframe(
        self, session, table_name: str, **kwargs
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_tablenames_from_schema(self, session) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_non_stringtype_features(
        self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(
        self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(
        self,
        session,
        table_name: str,
    ) -> List[str]:
        pass

    @abstractmethod
    def get_arraytype_columns(self, session, table_name: str) -> List[str]:
        pass

    @abstractmethod
    def fetch_create_metrics_table_query(self, metrics_df):
        pass
