import os
import json
import shutil
import inspect
import numpy as np
import pandas as pd
import redshift_connector
import redshift_connector.cursor

from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Any, Union, Optional, Sequence, Dict

import src.utils.utils as utils
from src.utils import constants
from src.utils.logger import logger
from src.connectors.Connector import Connector
from src.connectors.wh import ProfilesConnector

local_folder = constants.LOCAL_STORAGE_DIR


class RedshiftConnector(Connector):
    def __init__(self, folder_path: str) -> None:
        self.local_dir = os.path.join(folder_path, local_folder)
        path = Path(self.local_dir)
        path.mkdir(parents=True, exist_ok=True)
        self.array_time_features = {}
        return

    def get_local_dir(self) -> str:
        return self.local_dir

    def build_session(self, credentials: dict) -> redshift_connector.cursor.Cursor:
        """Builds the redshift connection cursor with given credentials (creds)

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor
        """
        self.schema = credentials.pop("schema")
        self.creds = credentials
        self.connection_parameters = self.remap_credentials(credentials)
        valid_params = inspect.signature(redshift_connector.connect).parameters
        conn_params = {
            k: v for k, v in self.connection_parameters.items() if k in valid_params
        }
        conn = redshift_connector.connect(**conn_params)
        self.creds["schema"] = self.schema
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SET search_path TO {self.schema};")
        return cursor

    def join_file_path(self, file_name: str) -> str:
        """
        Joins the given file name to the local data folder path.

        Args:
            file_name (str): The name of the file to be joined to the path.

        Returns:
            The joined file path as a string.
        """
        return os.path.join(self.local_dir, file_name)

    def run_query(
        self, cursor: redshift_connector.cursor.Cursor, query: str, response=True
    ) -> Optional[Tuple]:
        """Runs the given query on the redshift connection

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            query (str): Query to be executed on the Redshift connection
            response (bool): Whether to fetch the results of the query or not | Defaults to True

        Returns:
            Results of the query run on the Redshift connection
        """
        if response:
            return cursor.execute(query).fetchall()
        else:
            return cursor.execute(query)

    def call_procedure(self, *args, **kwargs):
        """Calls the given function for training

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            args (list): List of arguments to be passed to the training function
            kwargs (dict): Dictionary of keyword arguments to be passed to the training function

        Returns:
            Results of the training function
        """
        train_function = args[0]
        args = args[1:]
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
            "rudderstack-infra" if is_rudder_backend else user_preference_order_infra[0]
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

    def get_table(
        self, cursor: redshift_connector.cursor.Cursor, table_name: str, **kwargs
    ) -> pd.DataFrame:
        """Fetches the table with the given name from the Redshift schema as a pandas Dataframe object

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            table_name (str): Name of the table to be fetched from the Redshift schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        return self.get_table_as_dataframe(cursor, table_name, **kwargs)

    def get_table_as_dataframe(
        self, cursor: redshift_connector.cursor.Cursor, table_name: str, **kwargs
    ) -> pd.DataFrame:
        """Fetches the table with the given name from the Redshift schema as a pandas Dataframe object

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            table_name (str): Name of the table to be fetched from the Redshift schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        filter_condition = kwargs.get("filter_condition", "")
        query = f"SELECT * FROM {table_name.lower()}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ";"
        return cursor.execute(query).fetch_dataframe()

    def load_and_delete_json(self, json_file_name: str) -> dict:
        file_path = os.path.join(self.local_dir, json_file_name)
        with open(file_path, "r") as file:
            json_data = json.load(file)
        utils.delete_file(file_path)
        return json_data

    def write_table(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:
        """Writes the given pandas dataframe to the Redshift schema with the given name.

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the snowpark session
            table_name (str): Name with which the dataframe is to be written to the snowpark session
        Returns:
            Nothing
        """
        if kwargs.pop("local", True):
            self.write_table_locally(df, table_name)
        self.write_pandas(df, table_name, **kwargs)

    def write_pandas(self, df: pd.DataFrame, table_name_remote: str, **kwargs) -> None:
        """Writes the given pandas dataframe to the Redshift schema with the given name

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the snowpark session
            table_name (str): Name with which the dataframe is to be written to the snowpark session
            [From kwargs]
                if_exists (str): How to write the dataframe to the Redshift schema | Defaults to "append"
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
        cursor: redshift_connector.cursor.Cursor,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ) -> pd.DataFrame:
        """
        Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            label_table_name (str): Name of the table to be labelled
            label_column (str): Name of the column to be labelled
            entity_column (str): Name of the entity column
            label_value (Union[str,int,float]): Value to be labelled as '1'

        Returns:
            label_table (pd.DataFrame): The labelled table as a pandas Dataframe object
        """
        feature_table = self.get_table(cursor, label_table_name)
        if label_value is not None:
            feature_table[label_column] = np.where(
                feature_table[label_column] == label_value, 1, 0
            )
        label_table = feature_table[[entity_column, label_column]]
        return label_table

    def save_file(self, *args, **kwargs):
        """Function needed only for Snowflake Connector, hence an empty function for Redshift Connector."""
        pass

    def get_non_stringtype_features(
        self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        """
        Returns a list of strings representing the names of the Non-StringType(non-categorical) columns in the feature table.

        Args:
            feature_df (pd.DataFrame): A feature table dataframe
            label_column (str): A string representing the name of the label column.
            entity_column (str): A string representing the name of the entity column.

        Returns:
            List[str]: A list of strings representing the names of the non-StringType columns in the feature table.
        """
        non_stringtype_features = []
        for column in feature_df.columns:
            if column.lower() not in (label_column, entity_column) and (
                feature_df[column].dtype == "int64"
                or feature_df[column].dtype == "float64"
            ):
                non_stringtype_features.append(column)
        return non_stringtype_features

    def get_stringtype_features(
        self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        """
        Extracts the names of StringType(categorical) columns from a given feature table schema.

        Args:
            feature_df (pd.DataFrame): A feature table dataframe
            label_column (str): The name of the label column.
            entity_column (str): The name of the entity column.

        Returns:
            List[str]: A list of StringType(categorical) column names extracted from the feature table schema.
        """
        stringtype_features = []
        for column in feature_df.columns:
            if column.lower() not in (label_column, entity_column) and (
                feature_df[column].dtype != "int64"
                and feature_df[column].dtype != "float64"
            ):
                stringtype_features.append(column)
        return stringtype_features

    def get_arraytype_columns(
        self, cursor: redshift_connector.cursor.Cursor, table_name: str
    ) -> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connector cursor for data warehouse access
            table_name (str): Name of the table from which to retrieve the arraytype/super columns.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        cursor.execute(
            f"select * from pg_get_cols('{self.schema}.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);"
        )
        col_df = cursor.fetch_dataframe()
        arraytype_columns = []
        for _, row in col_df.iterrows():
            if row["col_type"] == "super":
                arraytype_columns.append(row["col_name"])
        return arraytype_columns

    def get_arraytype_columns_from_table(self, table: pd.DataFrame, **kwargs) -> list:
        """Returns the list of features to be ignored from the feature table.
        Args:
            table (snowflake.snowpark.Table): snowpark table.
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
        # TODO: remove this logger.info
        logger.info(
            f"Identifying high cardinality features in the Redshift feature table."
        )
        high_cardinal_features = list()
        for field in table.columns:
            if (table[field].dtype != "int64" and table[field].dtype != "float64") and (
                field.lower() not in (label_column.lower(), entity_column.lower())
            ):
                feature_data = table[field]
                total_rows = len(feature_data)
                top_10_freq_sum = sum(feature_data.value_counts().head(10))
                if top_10_freq_sum < cardinal_feature_threshold * total_rows:
                    high_cardinal_features.append(field)
        return high_cardinal_features

    def get_timestamp_columns(
        self,
        cursor: redshift_connector.cursor.Cursor,
        table_name: str,
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            cursor (redshift_connector.cursor.Cursor): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        cursor.execute(
            f"select * from pg_get_cols('{self.schema}.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);"
        )
        col_df = cursor.fetch_dataframe()
        timestamp_columns = []
        for _, row in col_df.iterrows():
            if row["col_type"] in [
                "timestamp without time zone",
                "date",
                "time without time zone",
            ]:
                timestamp_columns.append(row["col_name"])
        return timestamp_columns

    def get_timestamp_columns_from_table(
        self, table: pd.DataFrame, **kwargs
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            cursor (redshift_connector.cursor.Cursor): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        kwargs.get("features_path", None)
        timestamp_columns = self.array_time_features["timestamp_columns"]
        return timestamp_columns

    def get_default_label_value(
        self, cursor, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        label_value = list()
        table = self.get_table(cursor, table_name)
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

    def get_material_names_(
        self,
        cursor: redshift_connector.cursor.Cursor,
        material_table: str,
        start_time: str,
        end_time: str,
        model_name: str,
        model_hash: str,
        material_table_prefix: str,
        prediction_horizon_days: int,
        inputs: List[str],
    ) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Generates material names as list of tuple of feature table name and label table name required to create the training model and their corresponding training dates.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connector cursor for data warehouse access
            material_table (str): Name of the material table(present in constants.py file)
            start_time (str): train_start_dt
            end_time (str): train_end_dt
            model_name (str): Present in model_configs file
            model_hash (str) : latest model hash
            material_table_prefix (str): constant
            prediction_horizon_days (int): period of days

        Returns:
            Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]: Tuple of List of tuples of feature table names, label table names and their corresponding training dates
            ex: ([('material_shopify_user_features_fa138b1a_785', 'material_shopify_user_features_fa138b1a_786')] , [('2023-04-24 00:00:00', '2023-05-01 00:00:00')])
        """
        try:
            material_names = list()
            training_dates = list()

            df = self.get_material_registry_table(cursor, material_table)

            feature_df = (
                df.loc[
                    (df["model_name"] == model_name)
                    & (df["model_type"] == constants.ENTITY_VAR_MODEL)
                    & (df["model_hash"] == model_hash)
                    & (df["end_ts"] >= start_time)
                    & (df["end_ts"] <= end_time),
                    ["seq_no", "end_ts"],
                ]
                .drop_duplicates()
                .rename(
                    columns={"seq_no": "feature_seq_no", "end_ts": "feature_end_ts"}
                )
            )
            feature_df["label_end_ts"] = feature_df["feature_end_ts"] + timedelta(
                days=prediction_horizon_days
            )

            time_format = "%Y-%m-%d"
            label_start_time = datetime.strptime(start_time, time_format) + timedelta(
                days=prediction_horizon_days
            )
            label_end_time = datetime.strptime(end_time, time_format) + timedelta(
                days=prediction_horizon_days
            )
            label_df = (
                df.loc[
                    (df["model_name"] == model_name)
                    & (df["model_type"] == constants.ENTITY_VAR_MODEL)
                    & (df["model_hash"] == model_hash)
                    & (df["end_ts"] >= label_start_time)
                    & (df["end_ts"] <= label_end_time),
                    ["seq_no", "end_ts"],
                ]
                .drop_duplicates()
                .rename(columns={"seq_no": "label_seq_no", "end_ts": "label_end_ts"})
            )

            feature_label_df = pd.merge(
                feature_df, label_df, on="label_end_ts", how="inner"
            )

            for _, row in feature_label_df.iterrows():
                feat_seq_no = str(row["feature_seq_no"])
                feature_material_name = utils.generate_material_name(
                    material_table_prefix,
                    model_name,
                    model_hash,
                    feat_seq_no,
                )

                label_seq_no = str(row["label_seq_no"])
                label_meterial_name = utils.generate_material_name(
                    material_table_prefix,
                    model_name,
                    model_hash,
                    label_seq_no,
                )

                # Iterate over inputs and validate meterial names
                for input_material_query in inputs:
                    if self.validate_historical_materials_hash(
                        cursor, input_material_query, feat_seq_no, label_seq_no
                    ):
                        material_names.append(
                            (feature_material_name, label_meterial_name)
                        )
                        training_dates.append(
                            (str(row["feature_end_ts"]), str(row["label_end_ts"]))
                        )
            return material_names, training_dates
        except Exception as e:
            raise Exception(
                f"Following exception occured while retrieving material names with hash {model_hash} for {model_name} between dates {start_time} and {end_time}: {e}"
            )

    def get_creation_ts(
        self,
        cursor: redshift_connector.cursor.Cursor,
        material_table: str,
        model_name: str,
        model_hash: str,
        entity_key: str,
    ):
        """This function will return the model hash that is latest for given model name in material table

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access.
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file
            model_hash (str): latest model hash
            entity_key (str): entity key

        Returns:
            (): it's latest creation timestamp
        """
        redshift_df = self.get_material_registry_table(cursor, material_table)
        try:
            temp_hash_vector = (
                redshift_df.query(f'model_type == "{constants.ENTITY_VAR_MODEL}"')
                .query(f'model_hash == "{model_hash}"')
                .query(f'entity_key == "{entity_key}"')
                .sort_values(by="creation_ts", ascending=False)
                .reset_index(drop=True)[["creation_ts"]]
                .iloc[0]
            )

            creation_ts = temp_hash_vector["creation_ts"]
        except:
            raise Exception(
                f"Project is never materialzied with model name {model_name} and model hash {model_hash}."
            )
        return creation_ts

    def get_end_ts(
        self, cursor, material_table, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        """This function will return the end_ts with given model hash and model name

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name to be searched in material registry table
            model_hash (str): latest model hash
            seq_no (int): latest seq_no

        Returns:
            str: end_ts for given model hash and model name
        """
        df = self.get_material_registry_table(cursor, material_table)

        try:
            feature_table_info_df = (
                df.query(f'model_type == "{constants.ENTITY_VAR_MODEL}"')
                .query(f'model_name == "{model_name}"')
                .query(f'model_hash == "{model_hash}"')
                .query(f"seq_no == {seq_no}")
                .reset_index(drop=True)[["end_ts"]]
                .iloc[0]
            )

            end_ts = feature_table_info_df["end_ts"]
        except Exception as e:
            raise Exception(
                f"Project is never materialzied with model hash {model_hash}. Error message: {e}"
            )

        return end_ts

    def add_index_timestamp_colum_for_predict_data(
        self, predict_data, index_timestamp: str, end_ts: str
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
        cursor: redshift_connector.cursor.Cursor,
        stage_name: str,
        file_name: str,
        target_folder: str,
    ) -> None:
        """Fetches the given file from the given stage and saves it to the given target folder.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access.
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
        feature_table_filtered = feature_table.groupby(entity_column).head(
            max_row_count
        )
        if len(feature_table_filtered) < min_sample_for_training:
            raise Exception(
                f"Insufficient data for training. Only {len(feature_table_filtered)} user records found. Required minimum {min_sample_for_training} user records."
            )
        return feature_table_filtered

    def do_data_validation(
        self,
        feature_table: pd.DataFrame,
        label_column: str,
        task_type: str,
    ):
        try:
            # Check if label_column is present in feature_table
            if label_column not in feature_table.columns:
                raise Exception(
                    f"Label column {label_column} is not present in the feature table."
                )

            # Check if feature_table has at least one column apart from label_column and entity_column
            if feature_table.shape[1] < 3:
                raise Exception(
                    f"Feature table must have at least one column apart from the label column {label_column}."
                )

            if task_type == "classification":
                min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
                max_label_proportion = constants.CLASSIFIER_MAX_LABEL_PROPORTION

                # Check for the class imbalance
                label_proportion = feature_table[label_column].value_counts(
                    normalize=True
                )

                found_invalid_rows = (
                    (label_proportion < min_label_proportion)
                    | (label_proportion > max_label_proportion)
                ).any()

                if found_invalid_rows:
                    raise Exception(
                        f"Label column {label_column} has invalid proportions. \
                            Please check if the label column has valid labels."
                    )
            elif task_type == "regression":
                # Check for the label values
                distinct_values_count_list = feature_table[label_column].value_counts()

                if (
                    len(distinct_values_count_list)
                    < constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES
                ):
                    raise Exception(
                        f"Label column {label_column} has invalid number of distinct values. \
                            Please check if the label column has valid labels."
                    )
        except AttributeError:
            logger.warning(
                "Could not perform data validation. Please check if the required \
                    configuations are present in the constants.py file."
            )
            pass

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
        table["temp_1"] = pd.to_datetime(table[time_col])
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

    def get_material_registry_name(
        self,
        cursor: redshift_connector.cursor.Cursor,
        table_prefix: str = "material_registry",
    ) -> str:
        """This function will return the latest material registry table name

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connector cursor for data warehouse access
            table_name (str): Prefix of the name of the table | Defaults to "material_registry"

        Returns:
            str: latest material registry table name
        """
        material_registry_tables = list()

        def split_key(item):
            parts = item.split("_")
            if len(parts) > 1 and parts[-1].isdigit():
                return int(parts[-1])
            return 0

        cursor.execute(
            f"SELECT DISTINCT tablename FROM PG_TABLE_DEF WHERE schemaname = '{self.schema}';"
        )
        registry_df = cursor.fetch_dataframe()

        registry_df = registry_df[
            registry_df["tablename"].str.startswith(f"{table_prefix.lower()}")
        ]

        for _, row in registry_df.iterrows():
            material_registry_tables.append(row["tablename"])
        material_registry_tables.sort(reverse=True)
        sorted_material_registry_tables = sorted(
            material_registry_tables, key=split_key, reverse=True
        )

        return sorted_material_registry_tables[0]

    def get_material_registry_table(
        self,
        cursor: redshift_connector.cursor.Cursor,
        material_registry_table_name: str,
    ) -> pd.DataFrame:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access.
            material_registry_table_name (str): The material registry table name.

        Returns:
            pd.DataFrame: The filtered material registry table containing only the successfully materialized data.
        """
        material_registry_table = self.get_table_as_dataframe(
            cursor, material_registry_table_name
        )

        def safe_parse_json(x):
            try:
                return eval(x).get("complete", {}).get("status")
            except:
                return None

        material_registry_table["status"] = material_registry_table["metadata"].apply(
            safe_parse_json
        )
        return material_registry_table[material_registry_table["status"] == 2]

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

    """ The following functions are only specific to Redshift Connector and not used by any other connector."""

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

    def make_local_dir(self) -> None:
        "Created a local directory to store temporary files"
        Path(self.local_dir).mkdir(parents=True, exist_ok=True)

    def fetch_feature_df_path(self, feature_table_name: str) -> str:
        """This function will return the feature_df_path"""
        feature_df_path = os.path.join(
            self.local_dir, f"{feature_table_name}.parquet.gzip"
        )
        return feature_df_path

    def _delete_local_data_folder(self) -> None:
        """Deletes the local data folder."""
        try:
            shutil.rmtree(self.local_dir)
            logger.info("Local directory removed successfully")
        except OSError as o:
            logger.info("Local directory not present")
            pass

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

    def cleanup(self, *args, **kwargs) -> None:
        delete_local_data = kwargs.get("delete_local_data", None)
        if delete_local_data:
            self._delete_local_data_folder()
