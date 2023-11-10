import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union

# from .utils import utils
# from logger import logger

import os
import gzip
import shutil

from typing import Any, List, Tuple, Union

import snowflake.snowpark
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session

# import utils
# import constants
# from logger import logger

import subprocess
from datetime import datetime, timedelta, timezone

# from Connector import Connector
local_folder = "/tmp"


class Connector(ABC):
    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format for connection to warehouses

        Args:
            credentials (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            dict: Data warehouse creadentials remapped in format that is required to create a connection to warehouse
        """
        new_creds = {k if k != 'dbname' else 'database': v for k, v in credentials.items() if k != 'type'}
        return new_creds
    
    def date_add(reference_date: str, add_days: int) -> str:
        """
        Adds the horizon days to the reference date and returns the new date as a string.

        Args:
            reference_date (str): The Reference date in the format "YYYY-MM-DD".
            add_days (int): The number of days to add to the reference date.

        Returns:
            str: The new date is returned as a string in the format "YYYY-MM-DD".
        """
        new_timestamp = datetime.strptime(reference_date, "%Y-%m-%d") + timedelta(days=add_days)
        new_date = new_timestamp.strftime("%Y-%m-%d")
        return new_date

    def get_pb_path() -> str:
        """In Rudder-sources check if pb command works. Else, it returns the exact location where pb installable is present.

        Returns:
            str: _description_
        """
        try:
            _ = subprocess.check_output(["which", "pb"])
            return "pb"
        except:
            # logger.warning("pb command not found in the path. Using the default rudder-sources path /venv/bin/pb")
            return '/venv/bin/pb'
        
    def subprocess_run(args):
        response = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if response.returncode == 0:
            return True
        else:
            # logger.warning("Error occurred. Exit code:", response.returncode)
            # logger.warning("Standard Output:\n", response.stdout)
            # logger.warning("Standard Error:\n", response.stderr)
            return False
        
    def materialise_past_data(features_valid_time: str, feature_package_path: str, output_path: str, site_config_path: str, project_folder: str)-> None:
        """
        Materializes past data for a given date using the 'pb' command-line tool.

        Args:
            features_valid_time (str): The date for which the past data needs to be materialized.
            feature_package_path (str): The path to the feature package.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file

        Returns:
            None.

        Example Usage:
            materialise_past_data("2022-01-01", "packages/feature_table/models/shopify_user_features", "output/path")
        """
        try:
            features_valid_time_unix = int(datetime.strptime(features_valid_time, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            if project_folder is None:
                path_components = output_path.split(os.path.sep)
                output_index = path_components.index('output')
                project_folder = os.path.sep.join(path_components[:output_index])
            pb = get_pb_path()
            args = [pb, "run", "-p", project_folder, "-m", feature_package_path, "--migrate_on_load=True", "--end_time", str(features_valid_time_unix)]
            if site_config_path is not None:
                args.extend(['-c', site_config_path])
            # logger.info(f"Materialising historic data for {features_valid_time} using pb: {' '.join(args)} ")
            pb_run_for_past_data = subprocess_run(args)
            if pb_run_for_past_data:
                # logger.info(f"Successfully materialised data for date {features_valid_time} ")
                print(f"Successfully materialised data for date {features_valid_time} ")
            else:
                raise Exception(f"Error occurred while materialising data for date {features_valid_time} ")
        except Exception as e:
            # logger.error(f"Exception occured while materialising data for date {features_valid_time} ")
            print(e)
    
    def get_material_names(self, session, material_table: str, start_date: str, end_date: str, 
                        package_name: str, features_profiles_model: str, model_hash: str, material_table_prefix: str, prediction_horizon_days: int, 
                        output_filename: str, site_config_path: str, project_folder: str, input_models: List[str])-> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
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
            features_profiles_model (str): The name of the model.
            model_hash (str): The latest model hash.
            material_table_prefix (str): A constant.
            prediction_horizon_days (int): The period of days for prediction horizon.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
            input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model. If this is empty, we infer this frmo the package_name and features_profiles_model - for backward compatibility

        Returns:
            Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]: A tuple containing two lists:
                - material_names: A list of tuples containing the names of the feature and label tables.
                - training_dates: A list of tuples containing the corresponding training dates.
        """
        try:
            material_names, training_dates = self.get_material_names_(session, material_table, start_date, end_date, features_profiles_model, model_hash, material_table_prefix, prediction_horizon_days)

            if len(material_names) == 0:
                try:
                    if len(input_models) == 0:
                        # logger.warning("No input models provided. Inferring input models from package_name and features_profiles_model, assuming that python model is defined in application project and feature table is imported as a package.")
                        feature_package_path = f"packages/{package_name}/models/{features_profiles_model}"
                    else:
                        feature_package_path = ','.join(input_models) #Syntax: pb run models/m1,models/m2 
                    feature_date = date_add(start_date, prediction_horizon_days)
                    label_date = date_add(feature_date, prediction_horizon_days)
                    materialise_past_data(feature_date, feature_package_path, output_filename, site_config_path, project_folder)
                    materialise_past_data(label_date, feature_package_path, output_filename, site_config_path, project_folder)
                    material_names, training_dates = self.get_material_names_(session, material_table, start_date, end_date, features_profiles_model, model_hash, material_table_prefix, prediction_horizon_days)
                    if len(material_names) == 0:
                        raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {features_profiles_model} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}. This error means the model is unable to find historic data for training. In the python_model spec, ensure to give the paths to the feature table model correctly in train/inputs and point the same in train/config/data")
                except Exception as e:
                    # logger.exception(e)
                    # logger.error("Exception occured while materialising data. Please check the logs for more details")
                    raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {features_profiles_model} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}. This error means the model is unable to find historic data for training. In the python_model spec, ensure to give the paths to the feature table model correctly in train/inputs and point the same in train/config/data")
            return material_names, training_dates
        except Exception as e:
            # logger.error(e)
            raise Exception("Exception occured while retrieving material names. Please check the logs for more details")

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def join_file_path(self, file_name: str) -> str:
        pass

    @abstractmethod
    def run_query(self, session, query: str) -> Any:
        pass
    
    @abstractmethod
    def get_table(self, session, table_name: str, **kwargs):
        pass
    
    @abstractmethod
    def get_table_as_dataframe(self, session, table_name: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def write_pandas(self, df: pd.DataFrame, table_name: str, auto_create_table: bool, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def label_table(self, session,
                    label_table_name: str, label_column: str, entity_column: str, index_timestamp: str,
                    label_value: Union[str,int,float], label_ts_col: str):
        pass

    @abstractmethod
    def save_file(self, session, file_name: str, stage_name: str, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def call_procedure(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def get_non_stringtype_features(self, feature_table, label_column: str, entity_column: str, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(self, feature_table, label_column: str, entity_column: str, **kwargs)-> List[str]:
        pass

    @abstractmethod
    def get_arraytype_features(self, session, table_name: str)-> List[str]:
        pass

    @abstractmethod
    def get_high_cardinal_features(self, session, feature_table_name, label_column, entity_column, cardinal_feature_threshold) -> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(self, session, table_name: str, index_timestamp: str)-> List[str]:
        pass

    @abstractmethod
    def get_default_label_value(self, session, table_name: str, label_column: str, positive_boolean_flags: list):
        pass

    @abstractmethod
    def get_material_names_(self, session, material_table: str, start_time: str, end_time: str, model_name:str, model_hash: str,
                        material_table_prefix:str, prediction_horizon_days: int) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        pass
    
    @abstractmethod
    def get_material_registry_name(self, session, table_prefix: str) -> str:
        pass

    @abstractmethod
    def get_latest_material_hash(self, session, material_table: str, model_name:str) -> Tuple:
        pass

    @abstractmethod
    def fetch_staged_file(self, session, stage_name: str, file_name: str, target_folder: str)-> None:
        pass

    @abstractmethod
    def drop_cols(self, table, col_list: list):
        pass

    @abstractmethod
    def filter_feature_table(self, feature_table, entity_column: str, index_timestamp: str, max_row_count: int):
        pass

    @abstractmethod
    def add_days_diff(self, table, new_col, time_col_1, time_col_2):
        pass

    @abstractmethod
    def join_feature_table_label_table(self, feature_table, label_table, entity_column: str):
        pass

    @abstractmethod
    def get_distinct_values_in_column(self, table, column_name: str) -> List:
        pass


class SnowflakeConnector(Connector):
    train_procedure = 'train_sproc'
    def __init__(self) -> None:
        return

    def build_session(self, credentials: dict) -> snowflake.snowpark.Session:
        """Builds the snowpark session with given credentials (creds) 

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            session (snowflake.snowpark.Session): Snowpark session object
        """
        self.connection_parameters = self.remap_credentials(credentials)
        session = Session.builder.configs(self.connection_parameters).create()
        return session

    def join_file_path(self, file_name: str) -> str:
        """
        Joins the given file name to the local temp folder path.

        Args:
            file_name (str): The name of the file to be joined to the path.
        
        Returns:
            The joined file path as a string.
        """
        return os.path.join(local_folder, file_name)

    def run_query(self, session: snowflake.snowpark.Session, query: str) -> Any:
        """Runs the given query on the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            query (str): Query to be executed on the snowpark session

        Returns:
            Results of the query run on the snowpark session
        """
        return session.sql(query).collect()

    def get_table(self, session: snowflake.snowpark.Session, table_name: str, **kwargs) -> snowflake.snowpark.Table:
        """Fetches the table with the given name from the snowpark session as a snowpark table object

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            table_name (str): Name of the table to be fetched from the snowpark session

        Returns:
            table (snowflake.snowpark.Table): The table as a snowpark table object
        """
        filter_condition = kwargs.get('filter_condition', None)
        table = session.table(table_name)
        if filter_condition:
            table = self.filter_table(table, filter_condition)
        return table

    def get_table_as_dataframe(self, session: snowflake.snowpark.Session, table_name: str, **kwargs) -> pd.DataFrame:
        """Fetches the table with the given name from the snowpark session as a pandas Dataframe object

        Args:
            session (snowflake.snowpark.Session): Snowpark session object
            table_name (str): Name of the table to be fetched from the snowpark session

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        return self.get_table(session, table_name, **kwargs).toPandas()

    def write_table(self, table: snowflake.snowpark.Table, table_name_remote: str, **kwargs) -> None:
        """Writes the given snowpark table object to the snowpark session with the name as the given name

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            table (snowflake.snowpark.Table): Snowpark table object to be written to the snowpark session
            table_name_remote (str): Name with which the table is to be written to the snowpark session

        Returns:
            Nothing
        """
        write_mode = kwargs.get('write_mode', "append")
        table.write.mode(write_mode).save_as_table(table_name_remote)

    def write_pandas(self, df: pd.DataFrame, table_name: str, **kwargs):
        """Writes the given pandas dataframe to the snowpark session with the given name

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the snowpark session
            table_name (str): Name with which the dataframe is to be written to the snowpark session
            From kwargs:
            - session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            - auto_create_table (bool): Flag to indicate whether to create the table if it does not exist
            - overwrite (bool): Flag to indicate whether to overwrite the table if it already exists in the snowpark session

        Returns:
            Nothing
        """
        session = kwargs.get('session', None)
        if session == None:
            raise Exception("Session object not found")
        auto_create_table = kwargs.get('auto_create_table', True)
        overwrite = kwargs.get('overwrite', False)
        session.write_pandas(df, table_name=table_name, auto_create_table=auto_create_table, overwrite=overwrite)

    def label_table(self, session: snowflake.snowpark.Session,
                    label_table_name: str, label_column: str, entity_column: str, index_timestamp: str,
                    label_value: Union[str,int,float], label_ts_col: str) -> snowflake.snowpark.Table:
        """
        Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively.

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            label_table_name (str): Name of the table to be labelled
            label_column (str): Name of the column to be labelled
            entity_column (str): Name of the entity column
            index_timestamp (str): Name of the index timestamp column
            label_value (Union[str,int,float]): Value to be labelled as '1'
            label_ts_col (str): Name of the label timestamp column
        
        Returns:
            label_table (snowflake.snowpark.Table): The labelled table as a snowpark table object
        """
        if label_value == None:
            table = (session.table(label_table_name)
                     .select(entity_column, label_column, index_timestamp)
                     .withColumnRenamed(F.col(index_timestamp), label_ts_col))
        else:
            table = (session.table(label_table_name)
                        .withColumn(label_column, F.when(F.col(label_column)==label_value, F.lit(1)).otherwise(F.lit(0)))
                        .select(entity_column, label_column, index_timestamp)
                        .withColumnRenamed(F.col(index_timestamp), label_ts_col))
        return table

    def save_file(self, session: snowflake.snowpark.Session, file_name: str, stage_name: str, overwrite: bool):
        """Saves the given file to the given stage in the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            file_name (str): Name of the file to be saved to the snowpark session
            stage_name (str): Name of the stage to which the file is to be saved
            overwrite (bool): Flag to indicate whether to overwrite the file if it already exists in the stage

        Returns:
            Nothing
        """
        session.file.put(file_name, stage_name, overwrite=overwrite)

    def call_procedure(self, *args, **kwargs):
        """Calls the given procedure on the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            args (list): List of arguments to be passed to the procedure
        
        Returns:
            Results of the procedure call
        """
        session = kwargs.get('session', None)
        if session == None:
            raise Exception("Session object not found")
        return session.call(*args)

    def get_non_stringtype_features(self, feature_table_name: str, label_column: str, entity_column: str, **kwargs) -> List[str]:
        """
        Returns a list of strings representing the names of the Non-StringType(non-categorical) columns in the feature table.

        Args:
            feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
            label_column (str): A string representing the name of the label column.
            entity_column (str): A string representing the name of the entity column.
            [From kwargs] session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.

        Returns:
            List[str]: A list of strings representing the names of the non-StringType columns in the feature table.
        
        Example:
            Make the call as follows:
            feature_table = snowflake.snowpark.Table(...)
            label_column = "label"
            entity_column = "entity"
            non_stringtype_features = connector.get_non_stringtype_features(feature_table, label_column, entity_column, session=your_session)
        """
        session = kwargs.get('session', None)
        if session == None:
            raise Exception("Session object not found")
        feature_table = self.get_table(session, feature_table_name)
        non_stringtype_features = []
        for field in feature_table.schema.fields:
            if field.datatype != T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
                non_stringtype_features.append(field.name)
        return non_stringtype_features

    def get_stringtype_features(self, feature_table_name: str, label_column: str, entity_column: str, **kwargs)-> List[str]:
        """
        Extracts the names of StringType(categorical) columns from a given feature table schema.

        Args:
            feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
            label_column (str): The name of the label column.
            entity_column (str): The name of the entity column.
            [From kwargs] session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.

        Returns:
            List[str]: A list of StringType(categorical) column names extracted from the feature table schema.
        
        Example:
            Make the call as follows:
            feature_table = snowflake.snowpark.Table(...)
            label_column = "label"
            entity_column = "entity"
            stringtype_features = connector.get_stringtype_features(feature_table, label_column, entity_column, session=your_session)
        """
        session = kwargs.get('session', None)
        if session == None:
            raise Exception("Session object not found")
        feature_table = self.get_table(session, feature_table_name)
        stringtype_features = []
        for field in feature_table.schema.fields:
            if field.datatype == T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
                stringtype_features.append(field.name)
        return stringtype_features

    def get_arraytype_features(self, session: snowflake.snowpark.Session, table_name: str)-> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the arraytype columns.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        table = self.get_table(session, table_name)
        arraytype_features = [row.name for row in table.schema.fields if row.datatype == T.ArrayType()]
        return arraytype_features
    
    def get_high_cardinal_features(self, session: snowflake.snowpark.Session, feature_table_name: str, label_column: str, entity_column: str, cardinal_feature_threshold: float) -> List[str]:
        """
        Identify high cardinality features in the feature table based on condition that 
                the sum of frequency of ten most popular categories is less than 1% of the total row count,.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            feature_table_name (str): name of the feature table.
            label_column (str): The name of the label column in the feature table.
            entity_column (str): The name of the entity column in the feature table.

        Returns:
            List[str]: A list of strings representing the names of the high cardinality features in the feature table.

        Example:
            session = snowflake.snowpark.Session(...)
            feature_table_name = "..."
            label_column = "label"
            entity_column = "entity"
            high_cardinal_features = get_high_cardinal_features(session, feature_table_name, label_column, entity_column)
            print(high_cardinal_features)
        """
        high_cardinal_features = list()
        feature_table = self.get_table(session, feature_table_name)
        total_rows = feature_table.count()
        for field in feature_table.schema.fields:
            top_10_freq_sum = 0
            if field.datatype == T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
                feature_data = feature_table.filter(F.col(field.name) != '').group_by(F.col(field.name)).count().sort(F.col('count').desc()).first(10)
                for row in feature_data:
                    top_10_freq_sum += row.COUNT
                if top_10_freq_sum < (cardinal_feature_threshold * total_rows):
                    high_cardinal_features.append(field.name)
        return high_cardinal_features

    def get_timestamp_columns(self, session: snowflake.snowpark.Session, table_name: str, index_timestamp: str)-> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.
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
    
    def get_default_label_value(self, session, table_name: str, label_column: str, positive_boolean_flags: list):
        """
        Returns the default label value for the given label column in the given table.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            table_name (str): The name of the table from which to retrieve the default label value.
            label_column (str): The name of the label column from which to retrieve the default label value.
            positive_boolean_flags (list): The sample names of the positive labels.

        Returns:
            The default label value for the given label column in the given table.
        """
        label_value = list()
        table = self.get_table(session, table_name)
        distinct_labels = table.select(F.col(label_column).alias("distinct_labels")).distinct().collect()

        if len(distinct_labels) != 2:
            raise Exception("The feature to be predicted should be boolean")
        for row in distinct_labels:
            if row.DISTINCT_LABELS in positive_boolean_flags:
                label_value.append(row.DISTINCT_LABELS)
        
        if len(label_value) == 0:
            raise Exception(f"Label column {label_column} doesn't have any positive flags. Please provide custom label_value from label_column to bypass the error.")
        elif len(label_value) > 1:
            raise Exception(f"Label column {label_column} has multiple positive flags. Please provide custom label_value out of {label_value} to bypass the error.")
        return label_value[0]

    def generate_material_name(self, material_table_prefix: str, model_name: str, model_hash: str, seq_no: str) -> str:
        """Generates a valid table name from the model hash, model name, and seq no. 

        Args:
            material_table_prefix (str): a standard prefix defined in constants.py, common for all the material tables
            model_name (str): name of the profiles model, defined in profiles project
            model_hash (str): hash of the model, generated by profiles
            seq_no (str): sequence number of the material table - determines the timestamp of the material table

        Returns:
            str: name of the material table in warehouse 
        """
        return f'{material_table_prefix}{model_name}_{model_hash}_{seq_no}'
    
    def is_valid_table(self, session: snowflake.snowpark.Session, table_name: str) -> bool:
        """
        Checks whether a table exists in the data warehouse.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            table_name (str): The name of the table to be checked.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            session.sql(f"select * from {table_name} limit 1").collect()
            return True
        except:
            return False

    def get_material_names_(self, session: snowflake.snowpark.Session,
                        material_table: str, 
                        start_time: str, 
                        end_time: str, 
                        features_profiles_model:str,
                        model_hash: str,
                        material_table_prefix:str,
                        prediction_horizon_days: int) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Generates material names as list of tuple of feature table name and label table name required to create the training model and their corresponding training dates.

        Args:
            session (snowflake.snowpark.Session): Snowpark session for data warehouse access
            material_table (str): Name of the material table(present in constants.py file)
            start_time (str): train_start_dt
            end_time (str): train_end_dt
            features_profiles_model (str): Present in model_configs file
            model_hash (str) : latest model hash
            material_table_prefix (str): constant
            prediction_horizon_days (int): period of days

        Returns:
            Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]: Tuple of List of tuples of feature table names, label table names and their corresponding training dates
            ex: ([('material_shopify_user_features_fa138b1a_785', 'material_shopify_user_features_fa138b1a_786')] , [('2023-04-24 00:00:00', '2023-05-01 00:00:00')])
        """
        material_names = list()
        training_dates = list()

        snowpark_df = self.get_material_registry_table(session, material_table)

        feature_snowpark_df = (snowpark_df
                    .filter(col("model_name") == features_profiles_model)
                    .filter(col("model_hash") == model_hash)
                    .filter(f"end_ts between \'{start_time}\' and \'{end_time}\'")
                    .select("seq_no", "end_ts")
                    ).distinct()
        label_snowpark_df = (snowpark_df
                    .filter(col("model_name") == features_profiles_model)
                    .filter(col("model_hash") == model_hash) 
                    .filter(f"end_ts between dateadd(day, {prediction_horizon_days}, \'{start_time}\') and dateadd(day, {prediction_horizon_days}, \'{end_time}\')")
                    .select("seq_no", "end_ts")
                    ).distinct()
        
        feature_label_snowpark_df = feature_snowpark_df.join(label_snowpark_df,
                                                                F.datediff("day", feature_snowpark_df.end_ts, label_snowpark_df.end_ts)==prediction_horizon_days
                                                                ).select(feature_snowpark_df.seq_no.alias("feature_seq_no"),feature_snowpark_df.end_ts.alias("feature_end_ts"),
                                                                        label_snowpark_df.seq_no.alias("label_seq_no"), label_snowpark_df.end_ts.alias("label_end_ts"))
        for row in feature_label_snowpark_df.collect():
            feature_table_name_ = self.generate_material_name(material_table_prefix, features_profiles_model, model_hash, str(row.FEATURE_SEQ_NO))
            label_table_name_ = self.generate_material_name(material_table_prefix, features_profiles_model, model_hash, str(row.LABEL_SEQ_NO))
            if self.is_valid_table(session, feature_table_name_) and self.is_valid_table(session, label_table_name_):
                material_names.append((feature_table_name_, label_table_name_))
                training_dates.append((str(row.FEATURE_END_TS), str(row.LABEL_END_TS)))
        return material_names, training_dates

    def get_material_registry_name(self, session: snowflake.snowpark.Session, table_prefix: str='MATERIAL_REGISTRY') -> str:
        """This function will return the latest material registry table name
        Args:
            session (snowflake.snowpark.Session): snowpark session
            table_name (str): name of the material registry table prefix | Defaults to "MATERIAL_REGISTRY"
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
        sorted_material_registry_tables = sorted(material_registry_tables, key=split_key, reverse=True)
        return sorted_material_registry_tables[0]

    def get_latest_material_hash(self, session: snowflake.snowpark.Session, material_table: str, model_name:str) -> Tuple:
        """This function will return the model hash that is latest for given model name in material table

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file

        Returns:
            Tuple: latest model hash and it's creation timestamp
        """
        snowpark_df = self.get_material_registry_table(session, material_table)
        try:
            temp_hash_vector = snowpark_df.filter(col("model_name") == model_name).sort(col("creation_ts"), ascending=False).select(col("model_hash"), col("creation_ts")).collect()[0]
        except IndexError:
            raise Exception(f"Unable to fetch the latest model hash. model name: {model_name}, material table: {material_table}")
        model_hash = temp_hash_vector.MODEL_HASH
        creation_ts = temp_hash_vector.CREATION_TS
        return model_hash, creation_ts

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
        self.get_file(session, file_stage_path, target_folder)
        input_file_path = os.path.join(target_folder, f"{file_name}.gz")
        output_file_path = os.path.join(target_folder, file_name)

        with gzip.open(input_file_path, 'rb') as gz_file:
            with open(output_file_path, 'wb') as target_file:
                shutil.copyfileobj(gz_file, target_file)
        os.remove(input_file_path)

    def filter_table(self, table: snowflake.snowpark.Table, filter_condition: str) -> snowflake.snowpark.Table:
        """
        Filters the given table based on the given column element.

        Args:
            table (snowflake.snowpark.Table): The table to be filtered.
            column_element (str): The name of the column to be used for filtering.

        Returns:
            The filtered table as a snowpark table object.
        """
        return table.filter(filter_condition)

    def drop_cols(self, table: snowflake.snowpark.Table, col_list: list) -> snowflake.snowpark.Table:
        """
        Drops the columns in the given list from the given table.

        Args:
            table (snowflake.snowpark.Table): The table to be filtered.
            col_list (list): The list of columns to be dropped.

        Returns:
            The table after the columns have been dropped as a snowpark table object.
        """
        return table.drop(col_list)

    def filter_feature_table(self, feature_table: snowflake.snowpark.Table, entity_column: str, index_timestamp: str, max_row_count: int) -> snowflake.snowpark.Table:
        """
        Sorts the given feature table based on the given entity column and index timestamp.

        Args:
            feature_table (snowflake.snowpark.Table): The table to be filtered.
            entity_column (str): The name of the entity column to be used for sorting.
            index_timestamp (str): The name of the index timestamp column to be used for sorting.

        Returns:
            The sorted feature table as a snowpark table object.
        """
        table = feature_table.withColumn('row_num', F.row_number().over(Window.partition_by(F.col(entity_column)).order_by(
                                                        F.col(index_timestamp).desc()))).filter(F.col('row_num') == 1).drop(
                                                            ['row_num', index_timestamp]).sample(n = int(max_row_count))
        return table

    def add_days_diff(self, table: snowflake.snowpark.Table, new_col, time_col_1, time_col_2) -> snowflake.snowpark.Table:
        """
        Adds a new column to the given table containing the difference in days between the given timestamp columns.

        Args:
            table (snowflake.snowpark.Table): The table to be filtered.
            new_col (str): The name of the new column to be added.
            time_col_1 (str): The name of the first timestamp column.
            time_col_2 (str): The name of the timestamp column from which to find the difference.

        Returns:
            The table with the new column added as a snowpark table object.
        """
        return table.withColumn(new_col, F.datediff('day', F.col(time_col_1), F.col(time_col_2)))

    def join_feature_table_label_table(self, feature_table: snowflake.snowpark.Table, label_table: snowflake.snowpark.Table,
                                       entity_column: str, join_type: str='inner') -> snowflake.snowpark.Table:
        """
        Joins the given feature table and label table based on the given entity column.

        Args:
            feature_table (snowflake.snowpark.Table): The feature table to be joined.
            label_table (snowflake.snowpark.Table): The label table to be joined.
            entity_column (str): The name of the entity column to be used for joining.
            join_type (str): How to join the tables | Defaults to 'inner'.

        Returns:
            The table after the join action as a snowpark table object.
        """
        return feature_table.join(label_table, [entity_column], join_type=join_type)
    
    def get_distinct_values_in_column(self, table: snowflake.snowpark.Table, column_name: str) -> List:
        """Returns the distinct values in the given column of the given table.

        Args:
            table (snowflake.snowpark.Table): The table from which the distinct values are to be extracted.
            column_name (str): The name of the column from which the distinct values are to be extracted.
        
        Returns:
            List: The list of distinct values in the given column of the given table.
        """
        return table.select(column_name).distinct().collect()

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

    def get_file(self, session:snowflake.snowpark.Session, file_stage_path: str, target_folder: str):
        """Fetches the file with the given path from the snowpark session to the target folder

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            file_stage_path (str): Path of the file to be fetched from the snowpark session
            target_folder (str): Folder to which the file is to be fetched

        Returns:
            Nothing
        """
        _ = session.file.get(file_stage_path, target_folder)

    def create_stage(self, session: snowflake.snowpark.Session, stage_name: str):
        """
        Creates a Snowflake stage with the given name if it does not exist.

        Args:
            session (snowflake.snowpark.Session): A Snowflake session object to access the warehouse.
            stage_name (str): The name of the Snowflake stage to be created.
        
        Returns:
            Nothing
        """
        self.run_query(session, f"create stage if not exists {stage_name.replace('@', '')}")

    def delete_import_files(self, session: snowflake.snowpark.Session, stage_name: str, import_paths: List[str]) -> None:
        """
        Deletes files from the specified Snowflake stage that match the filenames extracted from the import paths.

        Args:
            session (snowflake.snowpark.Session): A Snowflake session object to access the warehouse
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
