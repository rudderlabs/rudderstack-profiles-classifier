import os
import gzip
import shutil
import pandas as pd

from datetime import datetime
from typing import Any, List, Tuple, Union

import snowflake.snowpark
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session

import utils
import constants
from logger import logger
from Connector import Connector
local_folder = constants.SF_LOCAL_STORAGE_DIR

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

    def run_query(self, session: snowflake.snowpark.Session, query: str, **args) -> List:
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
        if not utils.is_valid_table(session, table_name):
            raise Exception(f"Table {table_name} does not exist or not authorized")
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
            table = (self.get_table(session, label_table_name)
                     .select(entity_column, label_column, index_timestamp)
                     .withColumnRenamed(F.col(index_timestamp), label_ts_col))
        else:
            table = (self.get_table(session, label_table_name)
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
        arraytype_features = self.get_arraytype_features_from_table(table)
        return arraytype_features

    def get_arraytype_features_from_table(self, table: snowflake.snowpark.Table, **kwargs)-> list:
        """Returns the list of features to be ignored from the feature table.
        Args:
            table (snowflake.snowpark.Table): snowpark table.
        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
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
        timestamp_columns = self.get_timestamp_columns_from_table(table, index_timestamp)
        return timestamp_columns

    def get_timestamp_columns_from_table(self, table: snowflake.snowpark.Table, index_timestamp: str, **kwargs)-> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.
            index_timestamp (str): The name of the column containing the index timestamp information.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
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
            feature_table_name_ = utils.generate_material_name(material_table_prefix, features_profiles_model, model_hash, str(row.FEATURE_SEQ_NO))
            label_table_name_ = utils.generate_material_name(material_table_prefix, features_profiles_model, model_hash, str(row.LABEL_SEQ_NO))
            if utils.is_valid_table(session, feature_table_name_) and utils.is_valid_table(session, label_table_name_):
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

    def get_creation_ts(self, session: snowflake.snowpark.Session, material_table: str, model_name:str, model_hash:str):
        """This function will return the model hash that is latest for given model name in material table

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file
            model_hash (str): latest model hash

        Returns:
            (): it's latest creation timestamp
        """
        snowpark_df = self.get_material_registry_table(session, material_table)
        try:
            temp_hash_vector = snowpark_df.filter(col("model_name") == model_name).filter(col("model_hash") == model_hash).sort(col("creation_ts"), ascending=False).select(col("creation_ts")).collect()[0]
            creation_ts = temp_hash_vector.CREATION_TS
        except:
            raise Exception(f"Project is never materialzied with model name {model_name} and model hash {model_hash}.")
        return creation_ts

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
        ignore_features_upper = [col.upper() for col in col_list]
        ignore_features_lower = [col.lower() for col in col_list]
        ignore_features_ = [col for col in table.columns if col in ignore_features_upper or col in ignore_features_lower]
        return table.drop(ignore_features_)

    def filter_feature_table(self, feature_table: snowflake.snowpark.Table, entity_column: str, index_timestamp: str, max_row_count: int, min_sample_for_training: int) -> snowflake.snowpark.Table:
        """
        Sorts the given feature table based on the given entity column and index timestamp.

        Args:
            feature_table (snowflake.snowpark.Table): The table to be filtered.
            entity_column (str): The name of the entity column to be used for sorting.
            index_timestamp (str): The name of the index timestamp column to be used for sorting.

        Returns:
            The sorted feature table as a snowpark table object.
        """
        table = (feature_table.withColumn('row_num', 
                                          F.row_number()
                                          .over(Window.partition_by(F.col(entity_column))
                                                .order_by(F.col(index_timestamp).desc())))
                 .filter(F.col('row_num') == 1)
                 .drop(['row_num', index_timestamp])
                 .sample(n = int(max_row_count)))
        if table.count() < min_sample_for_training:
            raise Exception(f"Insufficient data for training. Only {table.count()} user records found. Required minimum {min_sample_for_training} user records.")
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
        material_registry_table = (self.get_table(session, material_registry_table_name)
                                .withColumn("status", F.get_path("metadata", F.lit("complete.status")))
                                .filter(F.col("status")==2)
                                )
        return material_registry_table

    def generate_type_hint(self, df: snowflake.snowpark.Table):        
        """Returns the type hints for given snowpark DataFrame's fields

        Args:
            df (snowflake.snowpark.Table): snowpark DataFrame

        Returns:
            _type_: Returns the type hints for given snowpark DataFrame's fields
        """
        type_map = {
            T.BooleanType(): float,
            T.DoubleType(): float,
            T.DecimalType(36,6): float,
            T.LongType(): float,
            T.StringType(): str
        }
        types = [type_map[d.datatype] for d in df.schema.fields]
        return T.PandasDataFrame[tuple(types)]

    def call_prediction_udf(self, predict_data: snowflake.snowpark.Table, prediction_udf: Any, entity_column: str, index_timestamp: str,
                                  score_column_name: str, percentile_column_name: str, output_label_column: str, train_model_id: str,
                                  column_names_path: str, prob_th: float, input: snowflake.snowpark.Table) -> pd.DataFrame:
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
            column_names_path (str): Path to the column names file
            prob_th (float): Probability threshold
            input (pd.DataFrame): Input dataframe
        Returns:
            Results of the predict function
        """
        preds = (predict_data.select(entity_column, index_timestamp, prediction_udf(*input).alias(score_column_name))
             .withColumn("model_id", F.lit(train_model_id)))
        preds = preds.withColumn(output_label_column, F.when(F.col(score_column_name)>=prob_th, F.lit(True)).otherwise(F.lit(False)))
        preds_with_percentile = preds.withColumn(percentile_column_name, F.percent_rank().over(Window.order_by(F.col(score_column_name)))).select(
                                                        entity_column, index_timestamp, "model_id", score_column_name, percentile_column_name, output_label_column)
        return preds_with_percentile

    def clean_up(self) -> None:
        pass


    """ The following functions are only specific to Snowflake Connector and not used by any other connector."""
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

    def drop_fn_if_exists(self, session: snowflake.snowpark.Session, fn_name: str) -> bool:
        """Snowflake caches the functions and it reuses these next time. To avoid the caching, we manually search for the same function name and drop it before we create the udf.

        Args:
            session (snowflake.snowpark.Session): snowpark session to access warehouse
            fn_name (str): Name of the function to be dropped

        Returns:
            bool
        """
        fn_list = session.sql(f"show user functions like '{fn_name}'").collect()
        if len(fn_list) == 0:
            logger.info(f"Function {fn_name} does not exist")
            return True
        else:
            logger.info("Function name match found. Dropping all functions with the same name")
            for fn in fn_list:
                fn_signature = fn["arguments"].split("RETURN")[0]
                drop = session.sql(f"DROP FUNCTION IF EXISTS {fn_signature}")
                logger.info(drop.collect()[0].status)
            logger.info("All functions with the same name dropped")
            return True

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
