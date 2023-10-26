import os
import numpy as np
import pandas as pd
import redshift_connector
import redshift_connector.cursor

from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Any, Union

import utils
from Connector import Connector
from profiles_rudderstack.wh import ProfilesConnector
from wh import ProfilesConnector
local_folder = "data"

class RedshiftConnector(Connector):
    def __init__(self) -> None:
        return
    
    def remap_credentials(self, creds: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format from Redshift connector

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            dict: Data warehouse creadentials remapped in format that is required to create a Redshift connector
        """
        self.schema = creds['schema']
        new_creds = {k if k != 'dbname' else 'database': v for k, v in creds.items() if k not in ['type', 'schema']}
        return new_creds
    
    def build_session(self, creds: dict) -> redshift_connector.cursor.Cursor:
        """Builds the redshift connection cursor with given credentials (creds) 

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor
        """
        self.connection_parameters = self.remap_credentials(creds)
        self.creds = creds
        conn = redshift_connector.connect(**self.connection_parameters)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SET search_path TO {self.schema};")
        return cursor

    def run_query(self, cursor: redshift_connector.cursor.Cursor, query: str) -> Any:
        """Runs the given query on the redshift connection

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            query (str): Query to be executed on the Redshift connection

        Returns:
            Results of the query run on the Redshift connection
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    def get_table(self, cursor: redshift_connector.cursor.Cursor, table_name: str) -> pd.DataFrame:
        """Fetches the table with the given name from the Redshift schema as a pandas Dataframe object

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            table_name (str): Name of the table to be fetched from the Redshift schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        return self.get_table_as_dataframe(cursor, table_name)
    
    def get_table_as_dataframe(self, cursor: redshift_connector.cursor.Cursor, table_name: str) -> pd.DataFrame:
        """Fetches the table with the given name from the Redshift schema as a pandas Dataframe object

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            table_name (str): Name of the table to be fetched from the Redshift schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        cursor.execute(f"select * from \"{table_name.lower()}\";")
        return cursor.fetch_dataframe()

    def write_table(self, df: pd.DataFrame, table_name: str, **kwargs) -> Any:
        """Writes the given pandas dataframe to the Redshift schema with the given name as well as saves it locally.

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the snowpark session
            table_name (str): Name with which the dataframe is to be written to the snowpark session
            [From kwargs] s3_config: S3 configuration for the Redshift connector | Defaults to "None"
        Returns:
            Nothing
        """
        s3_config = kwargs.get("s3_config")
        Path(local_folder).mkdir(parents=True, exist_ok=True)
        df.to_parquet(f"{local_folder}/{table_name}.parquet.gzip", compression='gzip')
        self.write_pandas(df, table_name, s3_config=s3_config)
        return
    
    def write_pandas(self, df: pd.DataFrame, table_name_remote: str, **kwargs) -> Any:
        """Writes the given pandas dataframe to the Redshift schema with the given name

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the snowpark session
            table_name (str): Name with which the dataframe is to be written to the snowpark session
            [From kwargs] s3_config: S3 configuration for the Redshift connector | Defaults to "None"
        Returns:
            Nothing
        """
        rs_conn = ProfilesConnector(self.creds, **kwargs)
        if_exists = kwargs.get("if_exists", "append")
        rs_conn.write_to_table(df, table_name_remote, schema=self.schema, if_exists=if_exists)
        return

    def label_table(self, cursor: redshift_connector.cursor.Cursor,
                    label_table_name: str, label_column: str, entity_column: str, index_timestamp: str,
                    label_value: Union[str,int,float], label_ts_col: str) -> pd.DataFrame:
        """
        Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            label_table_name (str): Name of the table to be labelled
            label_column (str): Name of the column to be labelled
            entity_column (str): Name of the entity column
            index_timestamp (str): Name of the index timestamp column
            label_value (Union[str,int,float]): Value to be labelled as '1'
            label_ts_col (str): Name of the label timestamp column
        
        Returns:
            label_table (pd.DataFrame): The labelled table as a pandas Dataframe object
        """
        feature_table = self.get_table(cursor, label_table_name)
        feature_table[label_column] = np.where(feature_table[label_column] == label_value, 1, 0)
        label_table = feature_table[[entity_column, label_column, index_timestamp]].rename(columns={index_timestamp: label_ts_col})
        return label_table

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

    def get_non_stringtype_features(self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs) -> List[str]:
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
            if column.lower() not in (label_column, entity_column) and (feature_df[column].dtype == 'int64' or feature_df[column].dtype == 'float64'):
                non_stringtype_features.append(column)
        return non_stringtype_features

    def get_stringtype_features(self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs)-> List[str]:
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
            if column.lower() not in (label_column, entity_column) and (feature_df[column].dtype != 'int64' and feature_df[column].dtype != 'float64'):
                stringtype_features.append(column)
        return stringtype_features

    def get_arraytype_features(self, cursor: redshift_connector.cursor.Cursor, table_name: str)-> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connector cursor for data warehouse access
            table_name (str): Name of the table from which to retrieve the arraytype/super columns.

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
    
    def get_timestamp_columns(self, cursor: redshift_connector.cursor.Cursor, table_name: str, index_timestamp: str)-> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            cursor (redshift_connector.cursor.Cursor): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.
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
    
    def get_material_registry_name(self, cursor: redshift_connector.cursor.Cursor, table_prefix: str="material_registry") -> str:
        """This function will return the latest material registry table name

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connector cursor for data warehouse access
            table_name (str): Prefix of the name of the table | Defaults to "material_registry"

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
    
    def get_material_names_(self, cursor: redshift_connector.cursor.Cursor,
                        material_table: str, 
                        start_time: str, 
                        end_time: str, 
                        model_name:str,
                        model_hash: str,
                        material_table_prefix:str,
                        prediction_horizon_days: int) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
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
        material_names = list()
        training_dates = list()

        df = self.get_material_registry_table(cursor, material_table)

        feature_df = df.loc[
            (df["model_name"] == model_name) &
            (df["model_hash"] == model_hash) &
            (df["end_ts"] >= start_time) &
            (df["end_ts"] <= end_time),
            ["seq_no", "end_ts"]
        ].drop_duplicates().rename(columns = {'seq_no':'feature_seq_no', 'end_ts': 'feature_end_ts'})
        feature_df["label_end_ts"] = feature_df["feature_end_ts"] + timedelta(days=prediction_horizon_days)

        time_format = '%Y-%m-%d'
        label_start_time = datetime.strptime(start_time, time_format) + timedelta(days=prediction_horizon_days)
        label_end_time = datetime.strptime(end_time, time_format) + timedelta(days=prediction_horizon_days)
        label_df = df.loc[
            (df["model_name"] == model_name) &
            (df["model_hash"] == model_hash) &
            (df["end_ts"] >= label_start_time) &
            (df["end_ts"] <= label_end_time),
            ["seq_no", "end_ts"]
        ].drop_duplicates().rename(columns = {'seq_no':'label_seq_no', 'end_ts': 'label_end_ts'})

        feature_label_df = pd.merge(feature_df, label_df, on="label_end_ts", how="inner")

        for _, row in feature_label_df.iterrows():
            material_names.append((utils.generate_material_name(material_table_prefix, model_name, model_hash, str(row["feature_seq_no"])), utils.generate_material_name(material_table_prefix, model_name, model_hash, str(row["label_seq_no"]))))
            training_dates.append((str(row["feature_end_ts"]), str(row["label_end_ts"])))
        return material_names, training_dates

    def get_material_names(self, cursor: redshift_connector.cursor.Cursor, material_table: str, start_date: str, end_date: str, 
                        package_name: str, model_name: str, model_hash: str, material_table_prefix: str, prediction_horizon_days: int,
                        output_filename: str, site_config_path: str, project_folder: str)-> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """
        Retrieves the names of the feature and label tables, as well as their corresponding training dates, based on the provided inputs.
        If no materialized data is found within the specified date range, the function attempts to materialize the feature and label data using the `materialise_past_data` function.
        If no materialized data is found even after materialization, an exception is raised.

        Args:
            cursor (redshift_connector.cursor.Cursor): A Redshift connection cursor for data warehouse access.
            material_table (str): The name of the material table (present in constants.py file).
            start_date (str): The start date for training data.
            end_date (str): The end date for training data.
            package_name (str): The name of the package.
            model_name (str): The name of the model.
            model_hash (str): The latest model hash.
            material_table_prefix (str): A constant.
            prediction_horizon_days (int): The period of days for prediction horizon.
            output_filename (str): The name of the output file.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file

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
                    utils.materialise_past_data(start_date, feature_package_path, output_filename, site_config_path, project_folder)
                    start_date_label = utils.get_label_date_ref(start_date, prediction_horizon_days)
                    utils.materialise_past_data(start_date_label, feature_package_path, output_filename, site_config_path, project_folder)
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

    def get_material_registry_table(self, cursor: redshift_connector.cursor.Cursor, material_registry_table_name: str) -> pd.DataFrame:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access.
            material_registry_table_name (str): The material registry table name.

        Returns:
            pd.DataFrame: The filtered material registry table containing only the successfully materialized data.
        """
        material_registry_table = self.get_table_as_dataframe(cursor, material_registry_table_name)
        material_registry_table["json_metadata"] = material_registry_table["metadata"].apply(lambda x: eval(x))
        material_registry_table["status"] = material_registry_table["json_metadata"].apply(lambda x: x["complete"]["status"])
        material_registry_table = material_registry_table[material_registry_table["status"] == 2]
        material_registry_table.drop(columns=["json_metadata"], inplace=True)
        return material_registry_table

    def get_latest_material_hash(self, cursor: redshift_connector.cursor.Cursor, material_table: str, model_name:str) -> Tuple:
        """This function will return the model hash that is latest for given model name in material table

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access.
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file

        Returns:
            Tuple: latest model hash and it's creation timestamp
        """
        redshift_df = self.get_material_registry_table(cursor, material_table)

        temp_hash_vector = redshift_df.query(f"model_name == '{model_name}'").sort_values(by="creation_ts", ascending=False).reset_index(drop=True)[["model_hash", "creation_ts"]].iloc[0]
    
        model_hash = temp_hash_vector["model_hash"]
        creation_ts = temp_hash_vector["creation_ts"]
        return model_hash, creation_ts

    def fetch_staged_file(self, *args)-> None:
        """Function needed only for Snowflake Connector, hence an empty function for Redshift Connector."""
        return
    
    def filter_columns(self, table: pd.DataFrame, column_element: str) -> pd.DataFrame:
        """
        Filters the given table based on the given column element.

        Args:
            table (pd.DataFrame): The table to be filtered.
            column_element (str): The name of the column to be used for filtering.

        Returns:
            The filtered table as a Pandas DataFrame object.
        """
        return table.filter(items = [column_element])

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
        ignore_features_ = [col for col in table.columns if col in ignore_features_upper or col in ignore_features_lower]
        return table.drop(columns = ignore_features_)

    def add_days_diff(self, table: pd.DataFrame, new_col: str, time_col_1: str, time_col_2: str) -> pd.DataFrame:
        """
        Adds a new column to the given table containing the difference in days between the given timestamp columns.

        Args:
            table (pd.DataFrame): The table to be filtered.
            new_col (str): The name of the new column to be added.
            time_col_1 (str): The name of the first timestamp column.
            time_col_2 (str): The name of the  timestamp column from which to find the difference.

        Returns:
            The table with the new column added as a Pandas DataFrame object.
        """
        table["temp_1"] = pd.to_datetime(table[time_col_1])
        table["temp_2"] = pd.to_datetime(table[time_col_2])
        table[new_col] = (table['temp_2'] - table["temp_1"]).dt.days
        table.drop(columns = ["temp_1", "temp_2"], inplace = True)
        return table
    
    def sort_feature_table(self, feature_table: pd.DataFrame, entity_column: str, index_timestamp: str) -> pd.DataFrame:
        """
        Sorts the given feature table based on the given entity column and index timestamp.

        Args:
            feature_table (pd.DataFrame): The table to be filtered.
            entity_column (str): The name of the entity column to be used for sorting.
            index_timestamp (str): The name of the index timestamp column to be used for sorting.

        Returns:
            The sorted feature table as a Pandas DataFrame object.
        """
        return feature_table.sort_values(by=[entity_column, index_timestamp], ascending=[True, False]).drop(columns=[index_timestamp])
    
    def join_feature_table_label_table(self, feature_table: pd.DataFrame,label_table: pd.DataFrame,
                                       entity_column: str, join_type: str='inner') -> pd.DataFrame:
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
    
    def join_file_path(self, file_name: str) -> str:
        """
        Joins the given file name to the local data folder path.

        Args:
            file_name (str): The name of the file to be joined to the path.
        
        Returns:
            The joined file path as a string.
        """
        return os.path.join(local_folder, file_name)
    
    def save_file(self, *args, **kwargs) -> None:
        """Function needed only for Snowflake Connector, hence an empty function for Redshift Connector."""
        return
    
    def get_arraytype_features_from_table(self, table: pd.DataFrame)-> list:
        """Returns the list of features to be ignored from the feature table.
        Args:
            table (snowflake.snowpark.Table): snowpark table.
        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        arraytype_features = []
        for column in table.columns:
            if table[column].dtype == 'super':
                arraytype_features.append(column)
        # arraytype_features = ['TOTAL_PRODUCTS_ADDED', 'PRODUCTS_ADDED_IN_PAST_1_DAYS', 'ITEMS_PURCHASED_EVER', 'PRODUCTS_ADDED_IN_PAST_365_DAYS', 'PRODUCTS_ADDED_IN_PAST_7_DAYS']
        # arraytype_features = [col.lower() for col in arraytype_features]
        return arraytype_features

    def generate_type_hint(self, df: pd.DataFrame):        
        """Returns the type hints for given pandas DataFrame's fields

        Args:
            sp_df (snowflake.snowpark.Table): pandas DataFrame

        Returns:
            _type_: Returns the type hints for given snowpark DataFrame's fields
        """
        types = []
        for col in df.columns:
            if df[col].dtype == 'object':
                types.append(str)
            else:
                types.append(float)
        return types
    
    def get_timestamp_columns_from_table(self, table: pd.DataFrame, index_timestamp: str)-> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            cursor (redshift_connector.cursor.Cursor): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.
            index_timestamp (str): The name of the column containing the index timestamp information.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        
        # timestamp_columns = []
        # for col in table.columns:
        #     # if table[col].dtype == 'datetime':
        #     if str(table[col].dtype).startswith('datetime'):
        #         timestamp_columns.append(col)
        timestamp_columns = ['first_seen_date', 'last_seen_date']
        return timestamp_columns
    
    def call_prediction_procedure(self, predict_data: pd.DataFrame, prediction_procedure: Any, entity_column: str, index_timestamp: str,
                                  score_column_name: str, percentile_column_name: str, output_label_column: str, train_model_id: str,
                                  prob_th: float, input: pd.DataFrame):
        preds = predict_data[[entity_column, index_timestamp]]
        preds[score_column_name] = prediction_procedure(input)
        preds['model_id'] = train_model_id
        preds[output_label_column] = preds[score_column_name].apply(lambda x: True if x >= prob_th else False)
        preds[percentile_column_name] = preds[score_column_name].rank(pct=True) * 100
        return preds
