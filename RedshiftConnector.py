import constants
import utils
from Connector import Connector



import numpy as np
import pandas as pd
import redshift_connector
from profiles_rudderstack.wh import ProfilesConnector


import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple
local_folder = "data"

class RedshiftConnector(Connector):
    def __init__(self) -> None:
        return

    def remap_credentials(self, creds: dict) -> dict:
        """ Updates the credentials for setting up the connection with the redshift warehouse"""
        new_creds = creds.copy()
        new_creds['database'] = new_creds['dbname']
        new_creds.pop('dbname')
        new_creds.pop('type')
        self.schema = new_creds['schema']
        new_creds.pop('schema')
        return new_creds

    def build_session(self, creds: dict):
        self.connection_parameters = self.remap_credentials(creds)
        self.creds = creds
        conn = redshift_connector.connect(**self.connection_parameters)
        conn.autocommit = True
        cursor = conn.cursor()
        self.run_query(cursor, f"SET search_path TO {self.schema};")
        return cursor

    def run_query(self, cursor, query: str):
        cursor.execute(query)
        return cursor.fetchall()

    def get_table(self, cursor, table_name: str):
        return self.get_table_as_dataframe(cursor, table_name)

    def get_table_as_dataframe(self, cursor, table_name: str) -> pd.DataFrame:
        cursor.execute(f"select * from \"{table_name.lower()}\";")
        return cursor.fetch_dataframe()

    def write_table(self, s3_config: dict, table, table_name_remote: str, write_mode) -> None:
        rs_conn = ProfilesConnector(self.creds)
        rs_conn.write_to_table(table, table_name_remote, schema=self.schema, if_exists='replace')
        return

    def label_table(self, cursor, label_table_name, label_column, entity_column, index_timestamp, label_value, label_ts_col):
        feature_table = self.get_table(cursor, label_table_name)
        feature_table[label_column] = np.where(feature_table[label_column] == label_value, 1, 0)
        feature_table = feature_table[[entity_column, label_column, index_timestamp]]
        feature_table.rename(columns={index_timestamp: label_ts_col}, inplace=True)
        return feature_table

    def save_file(self, cursor, file_name: str, stage_name: str, overwrite: bool) -> None:
        return

    def call_procedure(self, cursor, train_procedure, remote_table_name: str, figure_names: dict, merged_config: dict):
        return train_procedure(cursor, remote_table_name, figure_names, merged_config)

    def get_non_stringtype_features(self, cursor, feature_df: pd.DataFrame, label_column: str, entity_column: str) -> List[str]:
        """
        Returns a list of strings representing the names of the Non-StringType(non-categorical) columns in the feature table.

        Args:
            feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
            label_column (str): A string representing the name of the label column.
            entity_column (str): A string representing the name of the entity column.

        Returns:
            List[str]: A list of strings representing the names of the non-StringType columns in the feature table.
        """
        # cursor.execute(f"select * from pg_get_cols('rs_profiles_2.{feature_table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);")
        # col_df = cursor.fetch_dataframe()
        non_stringtype_features = []
        # for _, row in col_df.iterrows():
        #     if (~str(row['col_type']).startswith('character varying')) and row['col_name'].lower() not in (label_column.lower(), entity_column.lower()):
        #         non_stringtype_features.append(row['col_name'].upper())
        for column in feature_df.columns:
            if column.lower() not in (label_column, entity_column) and (feature_df[column].dtype == 'int64' or feature_df[column].dtype == 'float64'):
                non_stringtype_features.append(column)
        return non_stringtype_features

    def get_stringtype_features(self, cursor, feature_df: pd.DataFrame, label_column: str, entity_column: str)-> List[str]:
        """
        Extracts the names of StringType(categorical) columns from a given feature table schema.

        Args:
            feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
            label_column (str): The name of the label column.
            entity_column (str): The name of the entity column.

        Returns:
            List[str]: A list of StringType(categorical) column names extracted from the feature table schema.
        """
        # cursor.execute(f"select * from pg_get_cols('rs_profiles_2.{feature_table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);")
        # col_df = cursor.fetch_dataframe()
        stringtype_features = []
        # for _, row in col_df.iterrows():
        #     if (str(row['col_type']).startswith('character varying')) and row['col_name'].lower() not in (label_column.lower(), entity_column.lower()):
        #         stringtype_features.append(row['col_name'].upper())
        for column in feature_df.columns:
            if column.lower() not in (label_column, entity_column) and (feature_df[column].dtype != 'int64' and feature_df[column].dtype != 'float64'):
                stringtype_features.append(column)
        return stringtype_features

    def get_arraytype_features(self, cursor, table_name: str)-> list:
        """Returns the list of features to be ignored from the feature table.

        Args:
            table (snowflake.snowpark.Table): snowpark table.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        cursor.execute(f"select * from pg_get_cols('{self.schema}.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);")
        col_df = cursor.fetch_dataframe()
        arraytype_features = []
        for _, row in col_df.iterrows():
            if row['col_type'] == 'super':
                arraytype_features.append(row['col_name'])
        return arraytype_features

    def get_timestamp_columns(self, cursor, table_name: str, index_timestamp)-> list:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            feature_table (snowflake.snowpark.Table): The feature table from which to retrieve the timestamp columns.
            index_timestamp (str): The name of the column containing the index timestamp information.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        cursor.execute(f"select * from pg_get_cols('{self.schema}.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);")
        col_df = cursor.fetch_dataframe()
        timestamp_columns = []
        for _, row in col_df.iterrows():
            if row['col_type'] in ['timestamp without time zone', 'date', 'time without time zone'] and row['col_name'].lower() != index_timestamp.lower():
                timestamp_columns.append(row['col_name'])
        return timestamp_columns

    def get_material_registry_name(self, cursor, table_prefix: str="material_registry") -> str:
        """This function will return the latest material registry table name

        Args:
            session (snowflake.snowpark.Session): snowpark session
            table_name (str): name of the material registry table prefix

        Returns:
            str: latest material registry table name
        """
        material_registry_tables = list()
        def split_key(item):
            parts = item.split('_')
            if len(parts) > 1 and parts[-1].isdigit():
                return int(parts[-1])
            return 0
        cursor.execute(f"SELECT DISTINCT tablename FROM PG_TABLE_DEF WHERE schemaname = '{self.schema}';")
        registry_df = cursor.fetch_dataframe()

        registry_df = registry_df[registry_df['tablename'].str.startswith(f"{table_prefix.lower()}")]

        for _, row in registry_df.iterrows():
            material_registry_tables.append(row["tablename"])
        material_registry_tables.sort(reverse=True)
        sorted_material_registry_tables = sorted(material_registry_tables, key=split_key, reverse=True)

        return sorted_material_registry_tables[0]

    def get_material_names_(self, cursor,
                        material_table: str,
                        start_time: str,
                        end_time: str,
                        model_name:str,
                        model_hash: str,
                        material_table_prefix:str,
                        prediction_horizon_days: int) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Generates material names as list of tuple of feature table name and label table name required to create the training model and their corresponding training dates.

        Args:
            session (snowflake.snowpark.Session): Snowpark session for data warehouse access
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
        material_names = list()
        training_dates = list()

        df = self.get_table(cursor, material_table)

        feature_df = df.loc[
            (df["model_name"] == model_name) &
            (df["model_hash"] == model_hash) &
            (df["end_ts"] >= start_time) &
            (df["end_ts"] <= end_time),
            ["seq_no", "end_ts"]
        ]
        feature_df = feature_df.drop_duplicates()
        time_format = '%Y-%m-%d'
        label_start_time = datetime.strptime(start_time, time_format) + timedelta(days=prediction_horizon_days)
        label_end_time = datetime.strptime(end_time, time_format) + timedelta(days=prediction_horizon_days)
        label_df = df.loc[
            (df["model_name"] == model_name) &
            (df["model_hash"] == model_hash) &
            (df["end_ts"] >= label_start_time) &
            (df["end_ts"] <= label_end_time),
            ["seq_no", "end_ts"]
        ]
        label_df = label_df.drop_duplicates()

        feature_df.rename(columns = {'seq_no':'feature_seq_no', 'end_ts': 'feature_end_ts'}, inplace = True)
        label_df.rename(columns = {'seq_no':'label_seq_no', 'end_ts': 'label_end_ts'}, inplace = True)

        feature_label_df = pd.merge(feature_df, label_df, left_index=True, right_index=True)
        feature_label_df.loc[(feature_label_df["feature_end_ts"] - feature_label_df["label_end_ts"]).dt.days == prediction_horizon_days, :]

        for _, row in feature_label_df.iterrows():
            material_names.append((utils.generate_material_name(material_table_prefix, model_name, model_hash, str(row["feature_seq_no"])), utils.generate_material_name(material_table_prefix, model_name, model_hash, str(row["label_seq_no"]))))
            training_dates.append((str(row["feature_end_ts"]), str(row["label_end_ts"])))
        return material_names, training_dates

    def get_material_names(self, cursor, material_table: str, start_date: str, end_date: str,
                        package_name: str, model_name: str, model_hash: str, material_table_prefix: str, prediction_horizon_days: int,
                        output_filename: str)-> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """
        Retrieves the names of the feature and label tables, as well as their corresponding training dates, based on the provided inputs.
        If no materialized data is found within the specified date range, the function attempts to materialize the feature and label data using the `materialise_past_data` function.
        If no materialized data is found even after materialization, an exception is raised.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            material_table (str): The name of the material table (present in constants.py file).
            start_date (str): The start date for training data.
            end_date (str): The end date for training data.
            package_name (str): The name of the package.
            model_name (str): The name of the model.
            model_hash (str): The latest model hash.
            material_table_prefix (str): A constant.
            prediction_horizon_days (int): The period of days for prediction horizon.
            output_filename (str): The name of the output file.

        Returns:
            Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]: A tuple containing two lists:
                - material_names: A list of tuples containing the names of the feature and label tables.
                - training_dates: A list of tuples containing the corresponding training dates.
        """
        try:
            material_names, training_dates = self.get_material_names_(cursor, material_table, start_date, end_date, model_name, model_hash, material_table_prefix, prediction_horizon_days)

            if len(material_names) == 0:
                try:
                    feature_package_path = f"packages/{package_name}/models/{model_name}"
                    utils.materialise_past_data(start_date, feature_package_path, output_filename)
                    start_date_label = utils.get_label_date_ref(start_date, prediction_horizon_days)
                    utils.materialise_past_data(start_date_label, feature_package_path, output_filename)
                    material_names, training_dates = self.get_material_names_(cursor, material_table, start_date, end_date, model_name, model_hash, material_table_prefix, prediction_horizon_days)
                    if len(material_names) == 0:
                        raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {model_name} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}")
                except Exception as e:
                    print("Exception occured while materialising data. Please check the logs for more details")
                    raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {model_name} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}")
            return material_names, training_dates
        except Exception as e:
            print("Exception occured while retrieving material names. Please check the logs for more details")
            raise e

    def get_latest_material_hash(self, cursor, material_table: str, model_name:str) -> Tuple:
        """This function will return the model hash that is latest for given model name in material table

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file

        Returns:
            Tuple: latest model hash and it's creation timestamp
        """
        redshift_df = self.get_table_as_dataframe(cursor, material_table)

        temp_hash_vector = redshift_df.query(f"model_name == \"{model_name}\"")
        temp_hash_vector.sort_values(by="creation_ts", ascending=False, inplace=True)
        temp_hash_vector.reset_index(drop=True, inplace=True)
        temp_hash_vector = temp_hash_vector[["model_hash", "creation_ts"]].iloc[0]

        model_hash = temp_hash_vector["model_hash"]
        creation_ts = temp_hash_vector["creation_ts"]
        return model_hash, creation_ts

    def write_pandas(self, cursor, df: pd.DataFrame, table_name, auto_create_table, overwrite):
        Path(local_folder).mkdir(parents=True, exist_ok=True)
        df.to_csv(f"{local_folder}/{table_name}.csv", index=False)
        if table_name != constants.METRICS_TABLE:
            self.write_table(None, df, table_name, overwrite)
        return

    def fetch_staged_file(self, cursor, stage_name: str, file_name: str, target_folder: str)-> None:
        return

    def filter_columns(self, table: pd.DataFrame, column_element):
        return table.filter(items = [column_element])

    def drop_cols(self, table: pd.DataFrame, col_list: list):
        return table.drop(columns = col_list)

    def add_days_diff(self, table: pd.DataFrame, new_col, time_col_1, time_col_2):
        table["temp_1"] = pd.to_datetime(table[time_col_1])
        table["temp_2"] = pd.to_datetime(table[time_col_2])
        table[new_col] = (table['temp_1'] - table["temp_2"]).dt.days
        table.drop(columns = ["temp_1", "temp_2"], inplace = True)
        return table

    def sort_feature_table(self, feature_table: pd.DataFrame, entity_column: str, index_timestamp: str, feature_table_name_remote: str):
        df = feature_table.sort_values(by=[entity_column, index_timestamp], ascending=[True, False]).drop(columns=[index_timestamp])
        self.write_pandas(cursor=None, df=df, table_name=feature_table_name_remote, auto_create_table=False, overwrite=True)
        return df

    def join_feature_table_label_table(self, feature_table, label_table, entity_column):
        return feature_table.merge(label_table, on=[entity_column], how="inner")

    def join_file_path(self, file_name: str):
        return os.path.join(local_folder, file_name)