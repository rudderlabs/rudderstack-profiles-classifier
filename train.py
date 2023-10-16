#!/usr/bin/env python
# coding: utf-8
import gzip
import json
import joblib
import os
import shutil
import time
import redshift_connector
import yaml
import numpy as np 
import pandas as pd

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Tuple, List, Union, Any, Dict
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from xgboost import XGBClassifier, XGBRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor

from logger import logger
from sklearn.metrics import precision_recall_fscore_support, average_precision_score, mean_absolute_error, mean_squared_error

import snowflake.snowpark
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

from profiles_rudderstack.wh import ProfilesConnector

import utils
import constants
logger.info("Start")

trainer_utils = utils.TrainerUtils()
class Connector(ABC):
    @abstractmethod
    def build_session(self, creds):
        pass

    @abstractmethod
    def run_query(self, query: str) -> None:
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
    def get_material_names_(self, session,
                        material_table: str, 
                        start_time: str, 
                        end_time: str, 
                        model_name:str,
                        model_hash: str,
                        material_table_prefix:str,
                        prediction_horizon_days: int) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
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

class SnowflakeConnector(Connector):
    train_procedure = 'train_sproc'
    def __init__(self) -> None:
        return
    
    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format from snowflake session

        Args:
            credentials (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            dict: Data warehouse creadentials remapped in format that is required to create a snowpark session
        """
        new_creds = {k if k != 'dbname' else 'database': v for k, v in credentials.items() if k != 'type'}
        return new_creds
    
    def build_session(self, creds):
        self.connection_parameters = self.remap_credentials(creds)
        session = Session.builder.configs(self.connection_parameters).create()
        return session
    
    def run_query(self, session: snowflake.snowpark.Session, query: str) -> None:
        return session.sql(query).collect()

    def get_table(self, session: snowflake.snowpark.Session, table_name: str) -> snowflake.snowpark.Table:
        return session.table(table_name)

    def get_table_as_dataframe(self, session: snowflake.snowpark.Session, table_name: str) -> pd.DataFrame:
        return self.get_table(session, table_name).toPandas()
    
    def write_table(self, s3_config: dict, table: snowflake.snowpark.Table, table_name_remote: str, write_mode: str) -> None:
        return table.write.mode(write_mode).save_as_table(table_name_remote)
    
    def label_table(self, session: snowflake.snowpark.Session, label_table_name: str, label_column: str, entity_column: str, index_timestamp: str, label_value: Union[str,int,float], label_ts_col: str ):
        table = (session.table(label_table_name)
                        .withColumn(label_column, utils.F.when(utils.F.col(label_column)==label_value, utils.F.lit(1)).otherwise(utils.F.lit(0)))
                        .select(entity_column, label_column, index_timestamp)
                        .withColumnRenamed(utils.F.col(index_timestamp), label_ts_col))
        return table

    def save_file(self, session: snowflake.snowpark.Session, file_name: str, stage_name: str, overwrite: bool) -> None:
        return session.file.put(file_name, stage_name, overwrite=overwrite)
    
    def get_file(self, session:snowflake.snowpark.Session, file_stage_path: str, target_folder: str):
        return session.file.get(file_stage_path, target_folder)

    def write_pandas(self, session: snowflake.snowpark.Session, df: pd.DataFrame, table_name: str, auto_create_table: bool, overwrite: bool) -> Any:
        return session.write_pandas(df, table_name=table_name, auto_create_table=auto_create_table, overwrite=overwrite)
    
    def create_stage(self, session: snowflake.snowpark.Session, stage_name: str):
        return self.run_query(session, f"create stage if not exists {stage_name.replace('@', '')}")

    def call_procedure(self, session: snowflake.snowpark.Session, *args):
        return session.call(*args)
    
    def delete_import_files(self, session: snowflake.snowpark.Session, stage_name: str, import_paths: List[str]) -> None:
        """
        Deletes files from the specified Snowflake stage that match the filenames extracted from the import paths.

        Args:
            session (snowflake.snowpark.Session): A Snowflake session object.
            stage_name (str): The name of the Snowflake stage.
            import_paths (List[str]): The paths of the files to be deleted from the stage.

        Returns:
            None: The function does not return any value.
        """
        import_files = [element.split('/')[-1] for element in import_paths]
        files = self.run_query(session, f"list {stage_name}")
        for row in files:
            if any(substring in row.name for substring in import_files):
                self.run_query(session, f"remove @{row.name}")
    
    def delete_procedures(self, session: snowflake.snowpark.Session) -> None:
        """
        Deletes Snowflake train procedures based on a given name pattern.

        Args:
            session (snowflake.snowpark.Session): A Snowflake session object.
            train_procedure (str): The name pattern of the train procedures to be deleted.

        Returns:
            None

        Example:
            session = snowflake.snowpark.Session(...)
            delete_procedures(session, 'train_model')

        This function retrieves a list of procedures that match the given train procedure name pattern using a SQL query. 
        It then iterates over each procedure and attempts to drop it using another SQL query. If an error occurs during the drop operation, it is ignored.
        """
        procedures = self.run_query(session, f"show procedures like '{self.train_procedure}%'")
        for row in procedures:
            try:
                words = row.arguments.split(' ')[:-2]
                procedure_arguments = ' '.join(words)
                self.run_query(session, f"drop procedure if exists {procedure_arguments}")
            except:
                pass
    
    def get_material_registry_name(self, session: snowflake.snowpark.Session, table_prefix: str='MATERIAL_REGISTRY') -> str:
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
        registry_df = self.run_query(session, f"show tables starts with '{table_prefix}'")
        for row in registry_df:
            material_registry_tables.append(row.name)
        material_registry_tables.sort(reverse=True)
        sorted_material_registry_tables = sorted(material_registry_tables, key=split_key, reverse=True)
        return sorted_material_registry_tables[0]
    
    def get_material_registry_table(self, session: snowflake.snowpark.Session, material_registry_table_name: str) -> snowflake.snowpark.Table:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            material_registry_table_name (str): The material registry table name.

        Returns:
            snowflake.snowpark.Table: The filtered material registry table containing only the successfully materialized data.
        """
        material_registry_table = (session.table(material_registry_table_name)
                                .withColumn("status", F.get_path("metadata", F.lit("complete.status")))
                                .filter(F.col("status")==2)
                                )
        return material_registry_table

    def get_latest_material_hash(self, session: snowflake.snowpark.Session, material_table: str, model_name:str) -> Tuple:
        """This function will return the model hash that is latest for given model name in material table

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file

        Returns:
            Tuple: latest model hash and it's creation timestamp
        """
        # snowpark_df = self.get_table(session, material_table)
        snowpark_df = self.get_material_registry_table(session, material_table)
        temp_hash_vector = snowpark_df.filter(col("model_name") == model_name).sort(col("creation_ts"), ascending=False).select(col("model_hash"), col("creation_ts")).collect()[0]
        model_hash = temp_hash_vector.MODEL_HASH
        creation_ts = temp_hash_vector.CREATION_TS
        return model_hash, creation_ts

    def get_material_names_(self, session: snowflake.snowpark.Session,
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

        snowpark_df = self.get_table(session, material_table)

        feature_snowpark_df = (snowpark_df
                    .filter(col("model_name") == model_name)
                    .filter(col("model_hash") == model_hash)
                    .filter(f"end_ts between \'{start_time}\' and \'{end_time}\'")
                    .select("seq_no", "end_ts")
                    ).distinct()
        label_snowpark_df = (snowpark_df
                    .filter(col("model_name") == model_name)
                    .filter(col("model_hash") == model_hash) 
                    .filter(f"end_ts between dateadd(day, {prediction_horizon_days}, \'{start_time}\') and dateadd(day, {prediction_horizon_days}, \'{end_time}\')")
                    .select("seq_no", "end_ts")
                    ).distinct()
        
        feature_label_snowpark_df = feature_snowpark_df.join(label_snowpark_df,
                                                                F.datediff("day", feature_snowpark_df.end_ts, label_snowpark_df.end_ts)==prediction_horizon_days
                                                                ).select(feature_snowpark_df.seq_no.alias("feature_seq_no"),feature_snowpark_df.end_ts.alias("feature_end_ts"),
                                                                        label_snowpark_df.seq_no.alias("label_seq_no"), label_snowpark_df.end_ts.alias("label_end_ts"))
        for row in feature_label_snowpark_df.collect():
            material_names.append((material_table_prefix+model_name+"_"+model_hash+"_"+str(row.FEATURE_SEQ_NO), material_table_prefix+model_name+"_"+model_hash+"_"+str(row.LABEL_SEQ_NO)))
            training_dates.append((str(row.FEATURE_END_TS), str(row.LABEL_END_TS)))
        return material_names, training_dates

    def get_material_names(self, session: snowflake.snowpark.Session, material_table: str, start_date: str, end_date: str, 
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
            material_names, training_dates = self.get_material_names_(session, material_table, start_date, end_date, model_name, model_hash, material_table_prefix, prediction_horizon_days)

            if len(material_names) == 0:
                try:
                    # logger.info("No materialised data found in the given date range. So materialising feature data and label data")
                    feature_package_path = f"packages/{package_name}/models/{model_name}"
                    utils.materialise_past_data(start_date, feature_package_path, output_filename)
                    start_date_label = utils.get_label_date_ref(start_date, prediction_horizon_days)
                    utils.materialise_past_data(start_date_label, feature_package_path, output_filename)
                    material_names, training_dates = self.get_material_names_(session, material_table, start_date, end_date, model_name, model_hash, material_table_prefix, prediction_horizon_days)
                    if len(material_names) == 0:
                        raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {model_name} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}")
                except Exception as e:
                    # logger.exception(e)
                    print("Exception occured while materialising data. Please check the logs for more details")
                    raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {model_name} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}")
            return material_names, training_dates
        except Exception as e:
            print("Exception occured while retrieving material names. Please check the logs for more details")
            raise e
    
    def get_non_stringtype_features(self, session, feature_table_name: str, label_column: str, entity_column: str) -> List[str]:
        """
        Returns a list of strings representing the names of the Non-StringType(non-categorical) columns in the feature table.

        Args:
            feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
            label_column (str): A string representing the name of the label column.
            entity_column (str): A string representing the name of the entity column.

        Returns:
            List[str]: A list of strings representing the names of the non-StringType columns in the feature table.
        """
        feature_table = self.get_table(session, feature_table_name)
        non_stringtype_features = []
        for field in feature_table.schema.fields:
            if field.datatype != T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
                non_stringtype_features.append(field.name)
        return non_stringtype_features

    def get_stringtype_features(self, session, feature_table_name: str, label_column: str, entity_column: str)-> List[str]:
        """
        Extracts the names of StringType(categorical) columns from a given feature table schema.

        Args:
            feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
            label_column (str): The name of the label column.
            entity_column (str): The name of the entity column.

        Returns:
            List[str]: A list of StringType(categorical) column names extracted from the feature table schema.
        """
        feature_table = self.get_table(session, feature_table_name)
        stringtype_features = []
        for field in feature_table.schema.fields:
            if field.datatype == T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
                stringtype_features.append(field.name)
        return stringtype_features

    def get_arraytype_features(self, session: snowflake.snowpark.Session, table_name: str)-> list:
        """Returns the list of features to be ignored from the feature table.

        Args:
            table (snowflake.snowpark.Table): snowpark table.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        table = self.get_table(session, table_name)
        arraytype_features = [row.name for row in table.schema.fields if row.datatype == T.ArrayType()]
        return arraytype_features
    
    def get_timestamp_columns(self, session: snowflake.snowpark.Session, table_name: str, index_timestamp)-> list:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            feature_table (snowflake.snowpark.Table): The feature table from which to retrieve the timestamp columns.
            index_timestamp (str): The name of the column containing the index timestamp information.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        table = self.get_table(session, table_name)
        timestamp_columns = []
        for field in table.schema.fields:
            if field.datatype in [T.TimestampType(), T.DateType(), T.TimeType()] and field.name.lower() != index_timestamp.lower():
                timestamp_columns.append(field.name)
        return timestamp_columns

    def fetch_staged_file(self, session: snowflake.snowpark.Session, stage_name: str, file_name: str, target_folder: str)-> None:
        """
        Fetches a file from a Snowflake stage and saves it to a local target folder.

        Args:
            session (snowflake.snowpark.Session): The Snowflake session object used to connect to the Snowflake account.
            stage_name (str): The name of the Snowflake stage where the file is located.
            file_name (str): The name of the file to fetch from the stage.
            target_folder (str): The local folder where the fetched file will be saved.

        Returns:
            None
        """
        file_stage_path = f"{stage_name}/{file_name}"
        _ = self.get_file(session, file_stage_path, target_folder)
        input_file_path = os.path.join(target_folder, f"{file_name}.gz")
        output_file_path = os.path.join(target_folder, file_name)

        with gzip.open(input_file_path, 'rb') as gz_file:
            with open(output_file_path, 'wb') as target_file:
                shutil.copyfileobj(gz_file, target_file)
        os.remove(input_file_path)
    
    def filter_columns(self, table: snowflake.snowpark.Table, column_elements):
        return table.filter(column_elements)
    
    def drop_cols(self, table: snowflake.snowpark.Table, col_list: list):
        return table.drop(col_list)
    
    def sort_feature_table(self, feature_table: snowflake.snowpark.Table, entity_column: str, index_timestamp: str, feature_table_name_remote: str):
        sorted_feature_table = feature_table.sort(col(entity_column).asc(), col(index_timestamp).desc()).drop([index_timestamp])
        self.write_table(None, sorted_feature_table, feature_table_name_remote, write_mode="overwrite")
        return sorted_feature_table
    
    def add_days_diff(self, table: snowflake.snowpark.Table, new_col, time_col_1, time_col_2):
        return table.withColumn(new_col, F.datediff('day', F.col(time_col_1), F.col(time_col_2)))
    
    def join_feature_table_label_table(self, feature_table, label_table, entity_column):
        return feature_table.join(label_table, [entity_column], join_type="inner")
    
    def join_file_path(self, file_name: str):
        return os.path.join('/tmp', file_name)

class RedshiftConnector(Connector):
    def __init__(self) -> None:
        return
    
    def remap_credentials(self, creds: dict) -> dict:
        """ Updates the credentials for setting up the connection with the redshift warehouse"""
        new_creds = creds.copy()
        new_creds['database'] = new_creds['dbname']
        new_creds.pop('dbname')
        new_creds.pop('type')
        new_creds.pop('schema')
        return new_creds
    
    def build_session(self, creds: dict):
        self.connection_parameters = self.remap_credentials(creds)
        self.creds = creds
        conn = redshift_connector.connect(**self.connection_parameters)
        conn.autocommit = True
        return conn.cursor()
    
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
        rs_conn.write_to_table(table, table_name_remote, schema="rs_profiles_2", if_exists='replace')
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

    def get_non_stringtype_features(self, cursor, feature_df, label_column: str, entity_column: str) -> List[str]:
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

    def get_stringtype_features(self, cursor, feature_df: str, label_column: str, entity_column: str)-> List[str]:
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
        cursor.execute(f"select * from pg_get_cols('rs_profiles_2.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);")
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
        cursor.execute(f"select * from pg_get_cols('rs_profiles_2.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);")
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
        cursor.execute("SET search_path TO rs_profiles_2;")
        cursor.execute(f"SELECT DISTINCT tablename FROM PG_TABLE_DEF WHERE schemaname = 'rs_profiles_2';")
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
            material_names.append((material_table_prefix+model_name+"_"+model_hash+"_"+str(row["feature_seq_no"]), material_table_prefix+model_name+"_"+model_hash+"_"+str(row["label_seq_no"])))
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
        Path("data").mkdir(parents=True, exist_ok=True)
        df.to_csv(f"data/{table_name}.csv", index=False)
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
        return os.path.join('data', file_name)

@dataclass
class MLTrainer(ABC):

    def __init__(self,
                 label_value: int,
                 label_column: str,
                 entity_column: str,
                 package_name: str,
                 features_profiles_model: str,
                 output_profiles_ml_model: str,
                 index_timestamp: str,
                 eligible_users: str,
                 train_start_dt: str,
                 train_end_dt: str,
                 prediction_horizon_days: int,
                 prep: utils.PreprocessorConfig):
        self.label_value = label_value
        self.label_column = label_column
        self.entity_column = entity_column
        self.package_name = package_name
        self.features_profiles_model = features_profiles_model
        self.output_profiles_ml_model = output_profiles_ml_model
        self.index_timestamp = index_timestamp
        self.eligible_users = eligible_users
        self.train_start_dt = train_start_dt
        self.train_end_dt = train_end_dt
        self.prediction_horizon_days = prediction_horizon_days
        self.prep = prep
    hyperopts_expressions_map = {exp.__name__: exp for exp in [hp.choice, hp.quniform, hp.uniform, hp.loguniform]}    
    def get_preprocessing_pipeline(self, numeric_columns: List[str], 
                                    categorical_columns: List[str], 
                                    numerical_pipeline_config: List[str], 
                                    categorical_pipeline_config: List[str]):        
        """Returns a preprocessing pipeline for given numeric and categorical columns and pipeline config

        Args:
            numeric_columns (list): name of the columns that are numeric in nature
            categorical_columns (list): name of the columns that are categorical in nature
            numerical_pipeline_config (list): configs for numeric pipeline from model_configs file
            categorical_pipeline_config (list): configs for categorical pipeline from model_configs file

        Raises:
            ValueError: If num_params_name is invalid for numeric pipeline
            ValueError: If cat_params_name is invalid for catagorical pipeline

        Returns:
            _type_: preprocessing pipeline
        """
        numerical_pipeline_config_ = deepcopy(numerical_pipeline_config)
        categorical_pipeline_config_ = deepcopy(categorical_pipeline_config)
        for numerical_params in numerical_pipeline_config_:
            num_params_name = numerical_params.pop('name')
            if num_params_name == 'SimpleImputer':
                missing_values = numerical_params.get('missing_values')
                if missing_values == 'np.nan':
                    numerical_params['missing_values'] = np.nan
                num_imputer_params = numerical_params
            else:
                error_message = f"Invalid num_params_name: {num_params_name} for numeric pipeline."
                logger.error(error_message)
                raise ValueError(error_message)
        num_pipeline = Pipeline([
            ('imputer', SimpleImputer(**num_imputer_params)),
        ])
        
        pipeline_params_ = dict()
        for categorical_params in categorical_pipeline_config_:
            cat_params_name = categorical_params.pop('name')
            pipeline_params_[cat_params_name] = categorical_params
            try:
                assert cat_params_name in ['SimpleImputer', 'OneHotEncoder']
            except AssertionError:
                error_message = f"Invalid cat_params_name: {cat_params_name} for categorical pipeline."
                logger.error(error_message)
                raise ValueError(error_message)
            
        cat_pipeline = Pipeline([('imputer', SimpleImputer(**pipeline_params_['SimpleImputer'])),
                                ('encoder', OneHotEncoder(**pipeline_params_['OneHotEncoder']))])

        preprocessor = ColumnTransformer(
            transformers=[('num', num_pipeline, numeric_columns),
                        ('cat', cat_pipeline, categorical_columns)])
        return preprocessor
        
    def get_model_pipeline(self, preprocessor, clf):           
        pipe = Pipeline([('preprocessor', preprocessor), 
                        ('model', clf)])
        return pipe
    def generate_hyperparameter_space(self, hyperopts: List[dict]) -> dict:
        """Returns a dict of hyper-parameters expression map

        Args:
            hyperopts (List[dict]): list of all the hyper-parameter that are needed to be optimized

        Returns:
            dict: hyper-parameters expression map
        """
        space = {}
        for expression in hyperopts:
            expression_ = expression.copy()
            exp_type = expression_.pop("type")
            name = expression_.pop("name")

            # Handle expression for explicit choices and 
            # implicit choices using "low", "high" and optinal "step" values
            if exp_type == "choice":
                options = expression_["options"]
                if not isinstance(options, list):
                    expression_["options"] = list(range( options["low"], options["high"], options.get("step", 1)))
                    
            space[name] = self.hyperopts_expressions_map[f"hp_{exp_type}"](name, **expression_)
        return space

    @abstractmethod
    def select_best_model(self, models, train_x, train_y, val_x, val_y, models_map):
        pass
    @abstractmethod
    def plot_diagnostics(self, connector: Connector,
                        session,
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str):
        pass
    @abstractmethod
    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config):
        pass
    @abstractmethod
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        pass
    @abstractmethod
    def prepare_feature_table(self, connector, session, feature_table_name, label_table_name):
        pass

@dataclass
class ClassificationTrainer(MLTrainer):
    label_value: Union[str,int,float]
    evalution_metrics_map = {metric.__name__: metric for metric in [average_precision_score, precision_recall_fscore_support]}
    models_map = { model.__name__: model for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]}        
    def prepare_feature_table(self, connector: Connector,
                            session,
                            feature_table_name: str, 
                            label_table_name: str) -> Any:
        """This function creates a feature table as per the requirement of customer that is further used for training and prediction.

        Args:
            session (snowflake.snowpark.Session): Snowpark session for data warehouse access
            feature_table_name (str): feature table from the retrieved material_names tuple
            label_table_name (str): label table from the retrieved material_names tuple
        Returns:
            snowflake.snowpark.Table: feature table made using given instance from material names
        """
        try:
            label_ts_col = f"{self.index_timestamp}_label_ts"
            feature_table = connector.get_table(session, feature_table_name) #.withColumn(label_ts_col, F.dateadd("day", F.lit(prediction_horizon_days), F.col(index_timestamp)))
            arraytype_features = connector.get_arraytype_features(session, feature_table_name)
            ignore_features = utils.merge_lists_to_unique(self.prep.ignore_features, arraytype_features)
            if self.eligible_users:
                feature_table = connector.filter_columns(feature_table, self.eligible_users)
            feature_table = connector.drop_cols(feature_table, [self.label_column])
            timestamp_columns = self.prep.timestamp_columns
            if len(timestamp_columns) == 0:
                timestamp_columns = connector.get_timestamp_columns(session, feature_table_name, self.index_timestamp)
            for col in timestamp_columns:
                feature_table = connector.add_days_diff(feature_table, col, col, self.index_timestamp)
            label_table = connector.label_table(session, label_table_name, self.label_column, self.entity_column, self.index_timestamp, self.label_value, label_ts_col)
            uppercase_list = lambda names: [name.upper() for name in names]
            lowercase_list = lambda names: [name.lower() for name in names]
            ignore_features_ = [col for col in feature_table.columns if col in uppercase_list(ignore_features) or col in lowercase_list(ignore_features)]
            self.prep.ignore_features = ignore_features_
            self.prep.timestamp_columns = timestamp_columns
            feature_table = connector.join_feature_table_label_table(feature_table, label_table, self.entity_column)
            feature_table = connector.drop_cols(feature_table, [label_ts_col])
            feature_table = connector.drop_cols(feature_table, ignore_features_)
            return feature_table
        except Exception as e:
            print("Exception occured while preparing feature table. Please check the logs for more details")
            raise e

    @abstractmethod   
    def prepare_label_table(self, session: snowflake.snowpark.Session, label_table_name : str, label_ts_col : str):
        pass
    @abstractmethod
    def select_best_model(self, models, train_x, train_y, val_x, val_y, models_map):
        pass
    @abstractmethod
    def plot_diagnostics(self, session: snowflake.snowpark.Session, 
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str):
        pass
    @abstractmethod
    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config):
        pass
    @abstractmethod
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        pass


class ClassificationTrainer(MLTrainer):

    evalution_metrics_map = {metric.__name__: metric for metric in [average_precision_score, precision_recall_fscore_support]}
    models_map = { model.__name__: model for model in [XGBClassifier, RandomForestClassifier, MLPClassifier]}  

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.figure_names = {
            "roc-auc-curve": f"01-test-roc-auc-{self.output_profiles_ml_model}.png",
            "pr-auc-curve": f"02-test-pr-auc-{self.output_profiles_ml_model}.png",
            "lift-chart": f"03-test-lift-chart-{self.output_profiles_ml_model}.png",
            "feature-importance-chart": f"04-feature-importance-chart-{self.output_profiles_ml_model}.png"
        }
                  
    def prepare_label_table(self,session, label_table_name, label_ts_col):
        label_table = (session.table(label_table_name)
                    .withColumn(self.label_column, utils.F.when(utils.F.col(self.label_column)==self.label_value, utils.F.lit(1)).otherwise(utils.F.lit(0)))
                    .select(self.entity_column, self.label_column, self.index_timestamp)
                    .withColumnRenamed(utils.F.col(self.index_timestamp), label_ts_col))
        return label_table

    def build_model(self, X_train:pd.DataFrame, 
                    y_train:pd.DataFrame,
                    X_val:pd.DataFrame, 
                    y_val:pd.DataFrame,
                    model_class: Union[XGBClassifier, RandomForestClassifier, MLPClassifier],
                    model_config: Dict) -> Tuple:
        """Returns the classifier with best hyper-parameters after performing hyper-parameter tuning.

        Args:
            X_train (pd.DataFrame): X_train dataframe
            y_train (pd.DataFrame): y_train dataframe
            X_val (pd.DataFrame): X_val dataframe
            y_val (pd.DataFrame): y_val dataframe
            model_class (Union[XGBClassifier, RandomForestClassifier, MLPClassifier]): classifier to build model
            model_config (dict): configurations for the given model

        Returns:
            Tuple: classifier with best hyper-parameters found out using val_data along with trials info
        """
        hyperopt_space = self.generate_hyperparameter_space(model_config["hyperopts"])

        #We can set evaluation set for xgboost model which we cannot directly configure from configuration file
        fit_params = model_config.get("fitparams", {}).copy()
        if model_class.__name__ == "XGBClassifier":                         
            fit_params["eval_set"] = [( X_train, y_train), ( X_val, y_val)]

        #Objective method to run for different hyper-parameter space
        def objective(space):
            clf = model_class(**model_config["modelparams"], **space)
            clf.fit(X_train, y_train, **fit_params)
            pred = clf.predict_proba(X_val)
            eval_metric_name = model_config["evaluation_metric"]
            pr_auc = self.evalution_metrics_map[eval_metric_name](y_val, pred[:, 1])
            
            return {'loss': (0  - pr_auc), 'status': STATUS_OK , "config": space}

        trials = Trials()
        best_hyperparams = fmin(fn = objective,
                                space = hyperopt_space,
                                algo = tpe.suggest,
                                max_evals = model_config["hyperopts_config"]["max_evals"],
                                return_argmin=False,
                                trials = trials)

        clf = model_class(**best_hyperparams, **model_config["modelparams"])
        return clf, trials
    def select_best_model(self, models, train_x, train_y, val_x, val_y):
        """
        Selects the best classifier model based on the given list of models and their configurations.

        Args:
            models (list): A list of dictionaries representing the models to be trained.
            train_x (pd.DataFrame): The training data features.
            train_y (pd.DataFrame): The training data labels.
            val_x (pd.DataFrame): The validation data features.
            val_y (pd.DataFrame): The validation data labels.
        Returns:
            final_clf (object): The selected classifier model with the best hyperparameters.
        """
        best_acc = 0
        for model_config in models:
            name = model_config["name"]

            if name in self.models_map.keys():
                print(f"Training {name}")

                clf, trials = self.build_model(train_x, train_y, val_x, val_y, self.models_map[name], model_config)

                if best_acc < max([ -1*loss for loss in trials.losses()]):
                    final_clf = clf
                    best_acc = max([ -1*loss for loss in trials.losses()])

        return final_clf
    
    def plot_diagnostics(self, connector: Connector, session,
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str) -> None:
        """Plots the diagnostics for the given model

        Args:
            session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
            model (object): trained model
            stage_name (str): name of the stage
            x (pd.DataFrame): test data features
            y (pd.DataFrame): test data labels
            figure_names (dict): dict of figure names
            label_column (str): name of the label column
        """
        try:
            roc_auc_file = connector.join_file_path(figure_names['roc-auc-curve'])
            utils.plot_roc_auc_curve(model, x, y, roc_auc_file, label_column)
            connector.save_file(session, roc_auc_file, stage_name, overwrite=True)

            pr_auc_file = connector.join_file_path(figure_names['pr-auc-curve'])
            utils.plot_pr_auc_curve(model, x, y, pr_auc_file, label_column)
            connector.save_file(session, pr_auc_file, stage_name, overwrite=True)

            lift_chart_file = connector.join_file_path(figure_names['lift-chart'])
            utils.plot_lift_chart(model, x, y, lift_chart_file, label_column)
            connector.save_file(session, lift_chart_file, stage_name, overwrite=True)
        except Exception as e:
            logger.error(f"Could not generate plots. {e}")
        pass

    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config) -> dict:
        model_metrics, _, prob_th = trainer_utils.get_metrics_classifier(model, train_x, train_y, test_x, test_y, val_x, val_y, train_config)
        model_metrics['prob_th'] = prob_th
        result_dict = {"output_model_name": self.output_profiles_ml_model,
                       "prob_th": prob_th,
                        "metrics": model_metrics}
        return result_dict
    
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        training_summary ={"timestamp": model_timestamp,
                           "data": {"metrics": model_results['metrics'], 
                                    "threshold": model_results['prob_th']}}
        return training_summary

class RegressionTrainer(MLTrainer):

    evalution_metrics_map = {
        metric.__name__: metric
        for metric in [mean_absolute_error, mean_squared_error, r2_score]
    }
    
    models_map = {
        model.__name__: model
        for model in [XGBRegressor, RandomForestRegressor, MLPRegressor]
    }

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.figure_names = {}

    def prepare_label_table(self,session, label_table_name, label_ts_col):
        label_table = (session.table(label_table_name)
                    .select(self.entity_column, self.label_column, self.index_timestamp)
                    .withColumnRenamed(utils.F.col(self.index_timestamp), label_ts_col))
        return label_table

    def build_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        model_class: Union[XGBRegressor, RandomForestRegressor, MLPRegressor],
        model_config: Dict,
    ) -> Tuple:
        """
        Returns the regressor with best hyper-parameters after performing hyper-parameter tuning.

        Args:
            X_train (pd.DataFrame): X_train dataframe
            y_train (pd.DataFrame): y_train dataframe
            X_val (pd.DataFrame): X_val dataframe
            y_val (pd.DataFrame): y_val dataframe
            model_class: Regressor class to build the model
            model_config (dict): configurations for the given model

        Returns:
            Tuple: regressor with best hyper-parameters found out using val_data along with trials info
        """
        hyperopt_space = self.generate_hyperparameter_space(
            model_config["hyperopts"]
        )

        # We can set evaluation set for XGB Regressor model which we cannot directly configure from the configuration file
        fit_params = model_config.get("fitparams", {}).copy()
        if model_class.__name__ == "XGBRegressor":
            fit_params["eval_set"] = [(X_train, y_train), (X_val, y_val)]

        # Objective method to run for different hyper-parameter space
        def objective(space):
            reg = model_class(**model_config["modelparams"], **space)
            reg.fit(X_train, y_train)
            pred = reg.predict(X_val)
            eval_metric_name = model_config["evaluation_metric"]
            loss = self.evalution_metrics_map[eval_metric_name](y_val, pred)

            return {"loss": loss, "status": STATUS_OK, "config": space}

        trials = Trials()
        best_hyperparams = fmin(
            fn=objective,
            space=hyperopt_space,
            algo=tpe.suggest,
            max_evals=model_config["hyperopts_config"]["max_evals"],
            return_argmin=False,
            trials=trials,
        )

        reg = model_class(**best_hyperparams, **model_config["modelparams"])
        return reg, trials
    
    def select_best_model(self, models, train_x, train_y, val_x, val_y):
        """
        Selects the best regressor model based on the given list of models and their configurations.

        Args:
            models (list): A list of dictionaries representing the models to be trained.
            train_x (pd.DataFrame): The training data features.
            train_y (pd.DataFrame): The training data labels.
            val_x (pd.DataFrame): The validation data features.
            val_y (pd.DataFrame): The validation data labels.

        Returns:
            final_reg (object): The selected regressor model with the best hyperparameters.
        """
        best_loss = float("inf")

        for model_config in models:
            name = model_config["name"]
            print(f"Training {name}")

            if name in self.models_map.keys():
                reg, trials = self.build_model(
                    train_x, train_y, val_x, val_y, self.models_map[name], model_config
                )

                if best_loss > min(trials.losses()):
                    final_reg = reg
                    best_loss = min(trials.losses())

        return final_reg
    
    def plot_diagnostics(self, session: snowflake.snowpark.Session, 
                        model, 
                        stage_name: str, 
                        x: pd.DataFrame, 
                        y: pd.DataFrame, 
                        figure_names: dict, 
                        label_column: str):
        # To implemenet for regression - can be residual plot, binned lift chart adjusted to quantiles etc
        pass

    def get_metrics(self, model, train_x, train_y, test_x, test_y, val_x, val_y, train_config) -> dict:
        model_metrics = trainer_utils.get_metrics_regressor(model, train_x, train_y, test_x, test_y, val_x, val_y)
        result_dict = {"output_model_name": self.output_profiles_ml_model,
                       "prob_th": None,
                        "metrics": model_metrics}
        return result_dict
    
    def prepare_training_summary(self, model_results: dict, model_timestamp: str) -> dict:
        training_summary ={"timestamp": model_timestamp,
                           "data": {"metrics": model_results['metrics']}}
        return training_summary

    def prepare_feature_table(self, session: snowflake.snowpark.Session,
                                feature_table_name: str, 
                                label_table_name: str) -> snowflake.snowpark.Table:
        pass

def train_model(trainer, feature_df: pd.DataFrame, categorical_columns, numeric_columns, merged_config: dict, model_file):  
    train_config = merged_config['train']
    models = train_config["model_params"]["models"]
    model_id = str(int(time.time()))
    model_file_name = constants.MODEL_FILE_NAME

    X_train, X_val, X_test = utils.split_train_test(feature_df=feature_df,
                                                    label_column=trainer.label_column,
                                                    entity_column=trainer.entity_column,
                                                    train_size=trainer.prep.train_size,
                                                    val_size=trainer.prep.val_size,
                                                    test_size=trainer.prep.test_size)
    train_x, train_y = utils.split_label_from_features(X_train, trainer.label_column, trainer.entity_column)
    val_x, val_y = utils.split_label_from_features(X_val, trainer.label_column, trainer.entity_column)
    test_x, test_y = utils.split_label_from_features(X_test, trainer.label_column, trainer.entity_column)

    train_x = utils.transform_null(train_x, numeric_columns, categorical_columns)
    val_x = utils.transform_null(val_x, numeric_columns, categorical_columns)

    preprocessor_pipe_x = trainer.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer.prep.numeric_pipeline.get("pipeline"), trainer.prep.categorical_pipeline.get("pipeline"))
    train_x_processed = preprocessor_pipe_x.fit_transform(train_x)
    val_x_processed = preprocessor_pipe_x.transform(val_x)

    final_model = trainer.select_best_model(models, train_x_processed, train_y, val_x_processed, val_y)
    preprocessor_pipe_optimized = trainer.get_preprocessing_pipeline(numeric_columns, categorical_columns, trainer.prep.numeric_pipeline.get("pipeline"), trainer.prep.categorical_pipeline.get("pipeline"))
    pipe = trainer.get_model_pipeline(preprocessor_pipe_optimized, final_model)
    pipe.fit(train_x, train_y)

    joblib.dump(pipe, model_file)

    results = trainer.get_metrics(pipe, train_x, train_y, test_x, test_y, val_x, val_y, train_config)
    results["model_id"] = model_id
    metrics_df = pd.DataFrame.from_dict(results).reset_index()

    return X_train, X_val, X_test, train_x, test_x, test_y, pipe, model_file, model_id, metrics_df, results

def train(creds: dict, inputs: str, output_filename: str, config: dict, site_config_path: str, project_folder: str) -> None:
# def train(creds: dict, inputs: str, output_filename: str, config: dict, s3_config: dict=None) -> None:
    """Trains the model and saves the model with given output_filename.

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        inputs (str): Not being used currently. Can pass a blank string. For future support
        output_filename (str): path to the file where the model details including model id etc are written. Used in prediction step.
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

    Raises:
        ValueError: If num_params_name is invalid for numeric pipeline
        ValueError: If cat_params_name is invalid for catagorical pipeline

    Returns:
        None: saves the model but returns nothing
    """
    model_file_name = constants.MODEL_FILE_NAME
    stage_name = constants.STAGE_NAME
    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX

    current_dir = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.join(current_dir, 'utils.py')
    constants_path = os.path.join(current_dir, 'constants.py')
    logger_path = os.path.join(current_dir, "logger.py")
    config_path = os.path.join(current_dir, 'config', 'model_configs.yaml')
    folder_path = os.path.dirname(output_filename)
    target_path = utils.get_output_directory(folder_path)
    import_paths = [utils_path, constants_path, logger_path]
    metrics_table = constants.METRICS_TABLE

    logger.info("Initialising trainer")

    notebook_config = utils.load_yaml(config_path)
    merged_config = utils.combine_config(notebook_config, config)
    prep_config = utils.PreprocessorConfig(**merged_config["preprocessing"])
    prediction_task = merged_config['data'].pop('task', 'classification') # Assuming default as classification
    if prediction_task == 'classification':
        trainer = ClassificationTrainer(**merged_config["data"], **{"prep": prep_config})
    elif prediction_task == 'regression':
        trainer = RegressionTrainer(**merged_config["data"], **{"prep": prep_config})

    def train_rs(session,
                feature_table_name: str,
                figure_names: dict,
                merged_config: dict) -> dict:

        model_file = connector.join_file_path(model_file_name)
        feature_df = pd.read_csv(f"data/{feature_table_name}.csv")
        feature_df.columns = [col.upper() for col in feature_df.columns]
        
        stringtype_features = connector.get_stringtype_features(session, feature_df, trainer.label_column, trainer.entity_column)
        categorical_columns = utils.merge_lists_to_unique(trainer.prep.categorical_pipeline['categorical_columns'], stringtype_features)

        non_stringtype_features = connector.get_non_stringtype_features(session, feature_df, trainer.label_column, trainer.entity_column)
        numeric_columns = utils.merge_lists_to_unique(trainer.prep.numeric_pipeline['numeric_columns'], non_stringtype_features)
        
        X_train, X_val, X_test, train_x, test_x, test_y, pipe, model_file, model_id, metrics_df, results = train_model(trainer, feature_df, categorical_columns, numeric_columns, merged_config, model_file)

        column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
        column_name_file = connector.join_file_path(f"{trainer.output_profiles_ml_model}_{model_id}_column_names.json")
        json.dump(column_dict, open(column_name_file,"w"))

        trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
        connector.write_pandas(session, X_train, table_name=f"{trainer.output_profiles_ml_model.upper()}_TRAIN", auto_create_table=True, overwrite=True)
        connector.write_pandas(session, X_val, table_name=f"{trainer.output_profiles_ml_model.upper()}_VAL", auto_create_table=True, overwrite=True)
        connector.write_pandas(session, X_test, table_name=f"{trainer.output_profiles_ml_model.upper()}_TEST", auto_create_table=True, overwrite=True)
        connector.save_file(session, model_file, stage_name, overwrite=True)
        connector.save_file(session, column_name_file, stage_name, overwrite=True)
        trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
        try:
            figure_file = os.path.join('data', figure_names['feature-importance-chart'])
            shap_importance = utils.plot_top_k_feature_importance(pipe, train_x, numeric_columns, categorical_columns, figure_file, top_k_features=5)
            connector.write_pandas(session, shap_importance, f"FEATURE_IMPORTANCE", True,False)
            connector.save_file(session, figure_file, stage_name, overwrite=True)
        except Exception as e:
            logger.error(f"Could not generate plots {e}")

        connector.write_pandas(session, metrics_df, table_name=f"{metrics_table}", auto_create_table=True, overwrite=False)
        return results

    warehouse = creds['type']
    logger.info("Building session")
    if warehouse == 'snowflake':
        logger.info("Building session for Snowflake")
        train_procedure = 'train_sproc'
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
        connector.create_stage(session, stage_name)
        connector.delete_import_files(session, stage_name, import_paths)
        connector.delete_procedures(session)

        @sproc(name=train_procedure, is_permanent=True, stage_location=stage_name, replace=True, imports= [current_dir]+import_paths, 
            packages=["snowflake-snowpark-python==0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "PyYAML", "numpy==1.23.1", "pandas", "hyperopt", "shap==0.41.0", "matplotlib==3.7.1", "seaborn==0.12.2", "scikit-plot==0.3.7"])
        def train_sp(session: snowflake.snowpark.Session,
                    feature_table_name: str,
                    figure_names: dict,
                    merged_config: dict) -> dict:
            """Creates and saves the trained model pipeline after performing preprocessing and classification and returns the model id attached with the results generated.

            Args:
                session (snowflake.snowpark.Session): valid snowpark session to access data warehouse
                feature_table_name (str): name of the user feature table generated by profiles feature table model, and is input to training and prediction
                figure_names (dict): A dict with the file names to be generated as its values, and the keys as the names of the figures.
                merged_config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

            Returns:
                list: returns the model_id which is basically the time converted to key at which results were generated along with precision, recall, fpr and tpr to generate pr-auc and roc-auc curve.
            """
            feature_df = connector.get_table_as_dataframe(session, feature_table_name)
            model_file = connector.join_file_path(model_file_name)
            stringtype_features = connector.get_stringtype_features(session, feature_table_name, trainer.label_column, trainer.entity_column)
            categorical_columns = utils.merge_lists_to_unique(trainer.prep.categorical_pipeline['categorical_columns'], stringtype_features)

            non_stringtype_features = connector.get_non_stringtype_features(session, feature_table_name, trainer.label_column, trainer.entity_column)
            numeric_columns = utils.merge_lists_to_unique(trainer.prep.numeric_pipeline['numeric_columns'], non_stringtype_features)

            X_train, X_val, X_test, train_x, test_x, test_y, pipe, model_file, model_id, metrics_df, results = train_model(trainer, feature_df, categorical_columns, numeric_columns, merged_config, model_file)

            column_dict = {'numeric_columns': numeric_columns, 'categorical_columns': categorical_columns}
            column_name_file = connector.join_file_path(f"{trainer.output_profiles_ml_model}_{model_id}_column_names.json")
            json.dump(column_dict, open(column_name_file,"w"))

            trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
            connector.write_pandas(session, X_train, table_name=f"{trainer.output_profiles_ml_model.upper()}_TRAIN", auto_create_table=True, overwrite=True)
            connector.write_pandas(session, X_val, table_name=f"{trainer.output_profiles_ml_model.upper()}_VAL", auto_create_table=True, overwrite=True)
            connector.write_pandas(session, X_test, table_name=f"{trainer.output_profiles_ml_model.upper()}_TEST", auto_create_table=True, overwrite=True)
            connector.save_file(session, model_file, stage_name, overwrite=True)
            connector.save_file(session, column_name_file, stage_name, overwrite=True)
            trainer.plot_diagnostics(connector, session, pipe, stage_name, test_x, test_y, figure_names, trainer.label_column)
            try:
                figure_file = os.path.join('tmp', figure_names['feature-importance-chart'])
                shap_importance = utils.plot_top_k_feature_importance(pipe, train_x, numeric_columns, categorical_columns, figure_file, top_k_features=5)
                connector.write_pandas(session, shap_importance, f"FEATURE_IMPORTANCE", True,False)
                connector.save_file(session, figure_file, stage_name, overwrite=True)
            except Exception as e:
                logger.error(f"Could not generate plots {e}")

            connector.write_pandas(session, metrics_df, table_name=f"{metrics_table}", auto_create_table=True, overwrite=False)
            return results
    elif warehouse == 'redshift':
        logger.info("Building session for RedShift")
        train_procedure = train_rs
        connector = RedshiftConnector()
        session = connector.build_session(creds)

    figure_names = {"roc-auc-curve": f"01-test-roc-auc.png",
                    "pr-auc-curve": f"02-test-pr-auc.png",
                    "lift-chart": f"03-test-lift-chart.png",
                    "feature-importance-chart": f"04-feature-importance-chart.png"}

    logger.info("Getting past data for training")
    material_table = connector.get_material_registry_name(session, material_registry_table_prefix)

    model_hash, creation_ts = connector.get_latest_material_hash(session, material_table, trainer.features_profiles_model)
    start_date, end_date = trainer.train_start_dt, trainer.train_end_dt
    if start_date == None or end_date == None:
        start_date, end_date = utils.get_date_range(creation_ts, trainer.prediction_horizon_days)

    material_names, training_dates = connector.get_material_names(session, material_table, start_date, end_date, 
                                                              trainer.package_name, 
                                                              trainer.features_profiles_model, 
                                                              model_hash, 
                                                              material_table_prefix, 
                                                              trainer.prediction_horizon_days, 
                                                              output_filename)

    material_names, training_dates = connector.get_material_names(session, material_table, start_date, end_date, 
                                                              trainer.package_name, 
                                                              trainer.features_profiles_model, 
                                                              model_hash, 
                                                              material_table_prefix, 
                                                              trainer.prediction_horizon_days,
                                                              output_filename,
                                                              site_config_path,
                                                              project_folder)
    # material_names, training_dates = connector.get_material_names(session, material_table, start_date, end_date, 
    #                                                           trainer.package_name, 
    #                                                           trainer.features_profiles_model, 
    #                                                           model_hash, 
    #                                                           material_table_prefix, 
    #                                                           trainer.prediction_horizon_days, 
    #                                                           output_filename)
 
    feature_table = None
    for row in material_names:
        feature_table_name, label_table_name = row
        feature_table_instance = trainer.prepare_feature_table(connector, session,
                                                               feature_table_name, 
                                                               label_table_name)
        if feature_table is None:
            feature_table = feature_table_instance
        else:
            feature_table = feature_table.unionAllByName(feature_table_instance)

    feature_table_name_remote = f"{trainer.output_profiles_ml_model}_features"
    sorted_feature_table = connector.sort_feature_table(feature_table, trainer.entity_column, trainer.index_timestamp, feature_table_name_remote)

    logger.info("Training and fetching the results")

    train_results_json = connector.call_procedure(session, train_procedure,
                                        feature_table_name_remote,
                                        figure_names,
                                        merged_config)
    logger.info("Saving train results to file")
    if type(train_results_json) != dict:
        train_results = json.loads(train_results_json)
    else:
        train_results = train_results_json
    model_id = train_results["model_id"]

    # train_results_json = connector.call_procedure(session, train_procedure, 
    #                                     feature_table_name_remote,
    #                                     figure_names,
    #                                     merged_config)
    # logger.info("Saving train results to file")
    # train_results = json.loads(train_results_json)
    # model_id = train_results["model_id"]
    
    results = {"config": {'training_dates': training_dates,
                        'material_names': material_names,
                        'material_hash': model_hash,
                        **asdict(trainer)},
            "model_info": {'file_location': {'stage': stage_name, 
                                             'file_name': f"{trainer.output_profiles_ml_model}_{model_file_name}"}, 
                                             'model_id': model_id,
                                             "threshold": train_results['prob_th']},
            "input_model_name": trainer.features_profiles_model}
    json.dump(results, open(output_filename,"w"))
    # results = {"config": {'training_dates': training_dates,
    #                     'material_names': material_names,
    #                     'material_hash': model_hash,
    #                     **asdict(trainer)},
    #         "model_info": {'file_location': {'stage': stage_name, 'file_name': f"{trainer.output_profiles_ml_model}_{model_file_name}"}, 'model_id': model_id},
    #         "input_model_name": trainer.features_profiles_model}
    # json.dump(results, open(output_filename,"w"))

    model_timestamp = datetime.utcfromtimestamp(int(model_id)).strftime('%Y-%m-%dT%H:%M:%SZ')
    summary = trainer.prepare_training_summary(train_results, model_timestamp)
    json.dump(summary, open(os.path.join(target_path, 'training_summary.json'), "w"))
    logger.info("Fetching visualisations to local")
    for figure_name in figure_names.values():
        try:
            connector.fetch_staged_file(session, stage_name, figure_name, target_path)
        except:
            print(f"Could not fetch {figure_name}")

if __name__ == "__main__":
    homedir = os.path.expanduser("~") 
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
        # creds = yaml.safe_load(f)["connections"]["shopify_wh_rs"]["outputs"]["dev"]
        # s3_config = yaml.safe_load(f)["connections"]["py_models"]["s3"]
    inputs = None
    output_folder = 'output/dev/seq_no/7'
    output_file_name = f"{output_folder}/train_output.json"
    siteconfig_path = os.path.join(homedir, ".pb/siteconfig.yaml")
    
    from pathlib import Path
    path = Path(output_folder)
    path.mkdir(parents=True, exist_ok=True)

    project_folder = '~/git_repos/rudderstack-profiles-shopify-churn'    #change path of project directory as per your system
       
    train(creds, inputs, output_file_name, None, siteconfig_path, project_folder)
    train(creds, inputs, output_file_name, None)
