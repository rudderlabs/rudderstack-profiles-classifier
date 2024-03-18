import os
import gzip
import json
import uuid
import shutil
import pandas as pd

from typing import Any, Iterable, List, Union, Optional, Sequence, Dict

import snowflake.snowpark
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, to_date
from snowflake.snowpark.session import Session

from ..utils import utils
from ..utils import constants
from ..utils.logger import logger
from ..connectors.Connector import Connector

local_folder = constants.SF_LOCAL_STORAGE_DIR


class SnowflakeConnector(Connector):
    def __init__(self) -> None:
        self.run_id = str(uuid.uuid4())
        current_dir = os.path.dirname(os.path.abspath(__file__))
        train_script_dir = os.path.dirname(current_dir)

        self.stage_name = f"@rs_{self.run_id}"
        self.udf_name = f"prediction_score_{self.stage_name.replace('@','')}"
        self.stored_procedure_name = f"train_and_store_model_results_sf_{self.run_id}"
        self.delete_files = [train_script_dir]
        self.feature_table_name = f"features_{self.run_id}"
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

    def run_query(
        self, session: snowflake.snowpark.Session, query: str, **args
    ) -> List:
        """Runs the given query on the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            query (str): Query to be executed on the snowpark session

        Returns:
            Results of the query run on the snowpark session
        """
        return session.sql(query).collect()

    def call_procedure(self, *args, **kwargs):
        """Calls the given procedure on the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            args (list): List of arguments to be passed to the procedure

        Returns:
            Results of the procedure call
        """
        session = kwargs.get("session", None)
        if session == None:
            raise Exception("Session object not found")
        args = list(args)
        args.insert(2, self.feature_table_name)
        feature_table_df = args.pop(
            1
        )  # Snowflake stored procedure for training requires feature_table_name saved on warehouse instead of feature_table_df
        del feature_table_df

        return session.call(*args)

    def get_merged_table(self, base_table, incoming_table):
        """Returns the merged table of base_table and incoming_table.

        Args:
            base_table (snowflake.snowpark.Table): base_table
            incoming_table (snowflake.snowpark.Table): incoming_table

        Returns:
            snowflake.snowpark.Table: Merged table of feature table and feature table instance
        """
        return (
            incoming_table
            if base_table is None
            else base_table.unionAllByName(incoming_table)
        )

    def fetch_processor_mode(
        self, user_preference_order_infra: List[str], is_rudder_backend: bool
    ) -> str:
        return constants.WAREHOUSE_MODE

    def get_udf_name(self, model_path: str) -> str:
        """Returns the udf name using info from the model_path

        Args:
            model_path (str): Path of the model

        Returns:
            str: UDF name
        """
        with open(model_path, "r") as f:
            results = json.load(f)
        stage_name = results["model_info"]["file_location"]["stage"]
        self.udf_name = f"prediction_score_{stage_name.replace('@','')}"
        return self.udf_name

    def is_valid_table(
        self, session: snowflake.snowpark.Session, table_name: str
    ) -> bool:
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

    def check_table_entry_in_material_registry(
        self,
        session: snowflake.snowpark.Session,
        registry_table_name: str,
        material: dict,
    ) -> bool:
        """
        Checks wether an entry is there in the material registry for the given
        material table name and wether its sucessfully materialised or not as well
        """

        material_registry_table = self.get_table(session, registry_table_name)
        num_rows = (
            material_registry_table.withColumn(
                "status", F.get_path("metadata", F.lit("complete.status"))
            )
            .filter(F.col("status") == 2)
            .filter(col("model_name") == material["model_name"])
            .filter(col("model_hash") == material["model_hash"])
            .filter(col("seq_no") == material["seq_no"])
            .count()
        )

        return num_rows != 0

    def get_table(
        self, session: snowflake.snowpark.Session, table_name: str, **kwargs
    ) -> snowflake.snowpark.Table:
        """Fetches the table with the given name from the snowpark session as a snowpark table object

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            table_name (str): Name of the table to be fetched from the snowpark session

        Returns:
            table (snowflake.snowpark.Table): The table as a snowpark table object
        """
        filter_condition = kwargs.get("filter_condition", None)
        if not self.is_valid_table(session, table_name):
            raise Exception(f"Table {table_name} does not exist or not authorized")
        table = session.table(table_name)
        if filter_condition:
            table = self.filter_table(table, filter_condition)
        return table

    def get_table_as_dataframe(
        self, session: snowflake.snowpark.Session, table_name: str, **kwargs
    ) -> pd.DataFrame:
        """Fetches the table with the given name from the snowpark session as a pandas Dataframe object

        Args:
            session (snowflake.snowpark.Session): Snowpark session object
            table_name (str): Name of the table to be fetched from the snowpark session

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        return self.get_table(session, table_name, **kwargs).toPandas()

    def send_table_to_train_env(self, table: snowflake.snowpark.Table, **kwargs) -> Any:
        """Sends the given snowpark table to the training env(ie. snowflake warehouse in this case) with the name as given"""
        self.write_table(table, self.feature_table_name, **kwargs)

    def write_table(
        self, table: snowflake.snowpark.Table, table_name_remote: str, **kwargs
    ) -> None:
        """Writes the given snowpark table object to the snowpark session with the name as the given name

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            table (snowflake.snowpark.Table): Snowpark table object to be written to the snowpark session
            table_name_remote (str): Name with which the table is to be written to the snowpark session

        Returns:
            Nothing
        """
        write_mode = kwargs.get("write_mode", "append")
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
        session = kwargs.get("session", None)
        if session is None:
            raise Exception("Session object not found")
        auto_create_table = kwargs.get("auto_create_table", True)
        overwrite = kwargs.get("overwrite", False)
        session.write_pandas(
            df,
            table_name=table_name,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
        )

    def label_table(
        self,
        session: snowflake.snowpark.Session,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ) -> snowflake.snowpark.Table:
        """
        Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively.

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            label_table_name (str): Name of the table to be labelled
            label_column (str): Name of the column to be labelled
            entity_column (str): Name of the entity column
            label_value (Union[str,int,float]): Value to be labelled as '1'

        Returns:
            label_table (snowflake.snowpark.Table): The labelled table as a snowpark table object
        """
        if label_value is None:
            table = self.get_table(session, label_table_name).select(
                entity_column, label_column
            )
        else:
            table = (
                self.get_table(session, label_table_name)
                .withColumn(
                    label_column,
                    F.when(F.col(label_column) == label_value, F.lit(1)).otherwise(
                        F.lit(0)
                    ),
                )
                .select(entity_column, label_column)
            )
        return table

    def save_file(
        self,
        session: snowflake.snowpark.Session,
        file_name: str,
        stage_name: str,
        overwrite: bool,
    ):
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

    def get_non_stringtype_features(
        self, feature_table_name: str, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
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
        session = kwargs.get("session", None)
        if session == None:
            raise Exception("Session object not found")
        feature_table = self.get_table(session, feature_table_name)
        non_stringtype_features = []
        for field in feature_table.schema.fields:
            if not isinstance(
                field.datatype, T.StringType
            ) and field.name.lower() not in (
                label_column.lower(),
                entity_column.lower(),
            ):
                non_stringtype_features.append(field.name)
        return non_stringtype_features

    def get_stringtype_features(
        self, feature_table_name: str, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
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
        session = kwargs.get("session", None)
        if session == None:
            raise Exception("Session object not found")
        feature_table = self.get_table(session, feature_table_name)
        stringtype_features = []
        for field in feature_table.schema.fields:
            if isinstance(field.datatype, T.StringType) and field.name.lower() not in (
                label_column.lower(),
                entity_column.lower(),
            ):
                stringtype_features.append(field.name)
        return stringtype_features

    def get_arraytype_columns(
        self, session: snowflake.snowpark.Session, table_name: str
    ) -> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the arraytype columns.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        table = self.get_table(session, table_name)
        arraytype_columns = self.get_arraytype_columns_from_table(table)
        return arraytype_columns

    def get_arraytype_columns_from_table(
        self, table: snowflake.snowpark.Table, **kwargs
    ) -> list:
        """Returns the list of features to be ignored from the feature table.
        Args:
            table (snowflake.snowpark.Table): snowpark table.
        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        arraytype_columns = [
            row.name
            for row in table.schema.fields
            if isinstance(row.datatype, T.ArrayType)
        ]
        return arraytype_columns

    def get_high_cardinal_features(
        self,
        feature_table: snowflake.snowpark.Table,
        label_column: str,
        entity_column: str,
        cardinal_feature_threshold: float,
    ) -> List[str]:
        """
        Identify high cardinality features in the feature table based on condition that
                the sum of frequency of ten most popular categories is less than 1% of the total row count,.

        Args:
            feature_table (snowflake.snowpark.Table): feature table.
            label_column (str): The name of the label column in the feature table.
            entity_column (str): The name of the entity column in the feature table.
            cardinal_feature_threshold (float): The threshold value for the cardinality of the feature.

        Returns:
            List[str]: A list of strings representing the names of the high cardinality features in the feature table.

        Example:
            feature_table_name = snowflake.snowpark.Table(...)
            label_column = "label"
            entity_column = "entity"
            cardinal_feature_threshold = 0.01
            high_cardinal_features = get_high_cardinal_features(feature_table, label_column, entity_column, cardinal_feature_threshold)
            print(high_cardinal_features)
        """
        high_cardinal_features = list()
        total_rows = feature_table.count()
        for field in feature_table.schema.fields:
            top_10_freq_sum = 0
            if field.datatype == T.StringType() and field.name.lower() not in (
                label_column.lower(),
                entity_column.lower(),
            ):
                feature_data = (
                    feature_table.filter(F.col(field.name) != "")
                    .group_by(F.col(field.name))
                    .count()
                    .sort(F.col("count").desc())
                    .first(10)
                )
                for row in feature_data:
                    top_10_freq_sum += row.COUNT
                if top_10_freq_sum < (cardinal_feature_threshold * total_rows):
                    high_cardinal_features.append(field.name)
        return high_cardinal_features

    def get_timestamp_columns(
        self, session: snowflake.snowpark.Session, table_name: str
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        table = self.get_table(session, table_name)
        timestamp_columns = self.get_timestamp_columns_from_table(table)
        return timestamp_columns

    def get_timestamp_columns_from_table(
        self, table: snowflake.snowpark.Table, **kwargs
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        timestamp_columns = []
        for field in table.schema.fields:
            if isinstance(field.datatype, (T.TimestampType, T.DateType, T.TimeType)):
                timestamp_columns.append(field.name)
        return timestamp_columns

    def get_default_label_value(
        self, session, table_name: str, label_column: str, positive_boolean_flags: list
    ):
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
        distinct_labels = (
            table.select(F.col(label_column).alias("distinct_labels"))
            .distinct()
            .collect()
        )

        if len(distinct_labels) != 2:
            raise Exception("The feature to be predicted should be boolean")
        for row in distinct_labels:
            if row.DISTINCT_LABELS in positive_boolean_flags:
                label_value.append(row.DISTINCT_LABELS)

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
        """Fetches the filtered table based on the given parameters."""
        filtered_snowpark_df = (
            df.filter(col("model_name") == features_profiles_model)
            .filter(col("model_hash") == model_hash)
            .filter(
                (to_date(col("end_ts")) >= start_time)
                & (to_date(col("end_ts")) <= end_time)
            )
            .select(columns)
        ).distinct()
        return filtered_snowpark_df

    def join_feature_label_tables(
        self,
        session: snowflake.snowpark.Session,
        registry_table_name: str,
        features_model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        prediction_horizon_days: int,
    ) -> Iterable:
        snowpark_df = self.get_material_registry_table(session, registry_table_name)
        feature_snowpark_df = self.fetch_filtered_table(
            snowpark_df,
            features_model_name,
            model_hash,
            start_time,
            end_time,
            columns=["seq_no", "end_ts"],
        )
        label_snowpark_df = self.fetch_filtered_table(
            snowpark_df,
            features_model_name,
            model_hash,
            utils.date_add(start_time, prediction_horizon_days),
            utils.date_add(end_time, prediction_horizon_days),
            columns=["seq_no", "end_ts"],
        )

        return (
            feature_snowpark_df.join(
                label_snowpark_df,
                F.datediff("day", feature_snowpark_df.end_ts, label_snowpark_df.end_ts)
                == prediction_horizon_days,
                join_type="full",
            )
            .select(
                feature_snowpark_df.seq_no.alias("feature_seq_no"),
                feature_snowpark_df.end_ts.alias("feature_end_ts"),
                label_snowpark_df.seq_no.alias("label_seq_no"),
                label_snowpark_df.end_ts.alias("label_end_ts"),
            )
            .collect()
        )

    def get_tables_by_prefix(self, session: snowflake.snowpark.Session, prefix: str):
        tables = list()
        registry_df = self.run_query(session, f"show tables starts with '{prefix}'")
        for row in registry_df:
            tables.append(row.name)
        return tables

    def get_creation_ts(
        self,
        session: snowflake.snowpark.Session,
        material_table: str,
        model_hash: str,
        entity_key: str,
    ):
        """This function will return the model hash that is latest for given model name in material table

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name from model_configs file
            model_hash (str): latest model hash
            entity_key (str): entity key

        Returns:
            (): it's latest creation timestamp
        """
        snowpark_df = self.get_material_registry_table(session, material_table)
        try:
            temp_hash_vector = (
                snowpark_df.filter(col("model_hash") == model_hash)
                .filter(col("entity_key") == entity_key)
                .sort(col("creation_ts"), ascending=False)
                .select(col("creation_ts"))
                .collect()[0]
            )

            creation_ts = temp_hash_vector.CREATION_TS

        except:
            raise Exception(
                f"Project is never materialzied with model hash {model_hash}."
            )
        return creation_ts

    def get_end_ts(
        self, session, material_table, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        """This function will return the end_ts with given model hash and model name

        Args:
            session (snowflake.snowpark.Session): snowpark session
            material_table (str): name of material registry table
            model_name (str): model_name to be searched in material registry table
            model_hash (str): latest model hash
            seq_no (int): latest seq_no

        Returns:
            Tuple[str, str]: end_ts and model name
        """
        snowpark_df = self.get_material_registry_table(session, material_table)

        try:
            feature_table_info = (
                snowpark_df.filter(col("model_name") == model_name)
                .filter(col("model_hash") == model_hash)
                .filter(col("seq_no") == seq_no)
                .select("end_ts")
                .collect()[0]
            )

            end_ts = feature_table_info.END_TS
        except Exception as e:
            raise Exception(
                f"Project is never materialzied with model hash {model_hash}. Erro message: {e}"
            )

        return end_ts

    def add_index_timestamp_colum_for_predict_data(
        self, predict_data, index_timestamp: str, end_ts: str
    ) -> snowflake.snowpark.Table:
        """This function will add index_timestamp column to predict data

        Args:
            predict_data (snowflake.snowpark.Table): predict data
            index_timestamp (str): index timestamp
            end_ts (str): end timestamp value

        Returns:
            snowflake.snowpark.Table: predict data with index timestamp column
        """
        predict_data = predict_data.withColumn(
            index_timestamp, F.to_timestamp(F.lit(end_ts))
        )
        return predict_data

    def fetch_staged_file(
        self,
        session: snowflake.snowpark.Session,
        stage_name: str,
        file_name: str,
        target_folder: str,
    ) -> None:
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

        with gzip.open(input_file_path, "rb") as gz_file:
            with open(output_file_path, "wb") as target_file:
                shutil.copyfileobj(gz_file, target_file)
        os.remove(input_file_path)

    def filter_table(
        self, table: snowflake.snowpark.Table, filter_condition: str
    ) -> snowflake.snowpark.Table:
        """
        Filters the given table based on the given column element.

        Args:
            table (snowflake.snowpark.Table): The table to be filtered.
            column_element (str): The name of the column to be used for filtering.

        Returns:
            The filtered table as a snowpark table object.
        """
        return table.filter(filter_condition)

    def drop_cols(
        self, table: snowflake.snowpark.Table, col_list: list
    ) -> snowflake.snowpark.Table:
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
        ignore_features_ = [
            col
            for col in table.columns
            if col in ignore_features_upper or col in ignore_features_lower
        ]
        return table.drop(ignore_features_)

    def filter_feature_table(
        self,
        feature_table: snowflake.snowpark.Table,
        entity_column: str,
        max_row_count: int,
        min_sample_for_training: int,
    ) -> snowflake.snowpark.Table:
        """
        Sorts the given feature table based on the given entity column and index timestamp.

        Args:
            feature_table (snowflake.snowpark.Table): The table to be filtered.
            entity_column (str): The name of the entity column to be used for sorting.
            max_row_count (int): The maximum number of rows to be returned.
            min_sample_for_training (int): The minimum number of rows required for training.

        Returns:
            The sorted feature table as a snowpark table object.
        """
        table = (
            feature_table.withColumn(
                "row_num",
                F.row_number().over(
                    Window.partition_by(F.col(entity_column)).order_by(
                        F.col(entity_column).desc()
                    )
                ),
            )
            .filter(F.col("row_num") == 1)
            .drop(["row_num"])
            .sample(n=int(max_row_count))
        )
        if table.count() < min_sample_for_training:
            raise Exception(
                f"Insufficient data for training. Only {table.count()} user records found. \
                    Required minimum {min_sample_for_training} user records."
            )
        return table

    def check_for_classification_data_requirement(
        self,
        session: snowflake.snowpark.Session,
        materials: List[constants.TrainTablesInfo],
        label_column: str,
        label_value: str,
    ) -> bool:
        label_materials = [m.label_table_name for m in materials]
        label_table = None

        for label_material in label_materials:
            temp_table = self.get_table(session, label_material)
            label_table = self.get_merged_table(label_table, temp_table)

        total_samples = label_table.count()

        total_negative_samples = label_table.filter(
            F.col(label_column) != label_value
        ).count()

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
        session: snowflake.snowpark.Session,
        materials: List[constants.TrainTablesInfo],
    ) -> bool:
        feature_table = None
        for material in materials:
            feature_material = material.feature_table_name
            temp_table = self.get_table(session, feature_material)
            feature_table = self.get_merged_table(feature_table, temp_table)

        total_samples = feature_table.count()
        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES

        if total_samples < min_no_of_samples:
            logger.debug(
                "Number training samples are not meeting the minimum requirement, "
                f"total samples - {total_samples}, minimum samples required - {min_no_of_samples}"
            )
            return False

        return True

    def validate_columns_are_present(
        self, feature_table: snowflake.snowpark.Table, label_column: str
    ) -> bool:
        """This function will do the data validation on feature table and label column
        Args:
            feature_table (snowflake.snowpark.Table): feature table
            label_column (str): label column name
        Returns:
            None
        """
        if label_column.upper() not in feature_table.columns:
            raise Exception(
                f"Label column {label_column} is not present in the feature table."
            )
        if len(feature_table.columns) < 3:
            raise Exception(
                f"Feature table must have at least one column apart from the label column {label_column} and entity_column."
            )
        return True

    def validate_class_proportions(
        self, feature_table: snowflake.snowpark.Table, label_column: str
    ) -> bool:
        distinct_values_count = feature_table.groupBy(label_column).count()
        total_count = int(feature_table.count())
        result_table = distinct_values_count.withColumn(
            "NORMALIZED_COUNT", F.col("count") / total_count
        ).collect()

        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        max_label_proportion = constants.CLASSIFIER_MAX_LABEL_PROPORTION

        no_invalid_rows = [
            row
            for row in result_table
            if row["NORMALIZED_COUNT"] < min_label_proportion
            or row["NORMALIZED_COUNT"] > max_label_proportion
        ]

        if len(no_invalid_rows) > 0:
            raise Exception(
                f"Label column {label_column} has invalid proportions. {no_invalid_rows} \
                    Please check if the label column has valid labels."
            )
        return True

    def validate_label_distinct_values(
        self, feature_table: snowflake.snowpark.Table, label_column: str
    ) -> bool:
        distinct_values_count = feature_table.groupBy(label_column).count()
        if distinct_values_count.count() < int(
            constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES
        ):
            raise Exception(
                f"Label column {label_column} has invalid number of distinct values. \
                    Please check if the label column has valid labels."
            )
        return True

    def add_days_diff(
        self, table: snowflake.snowpark.Table, new_col, time_col, end_ts
    ) -> snowflake.snowpark.Table:
        """
        Adds a new column to the given table containing the difference in days between the given timestamp columns.

        Args:
            table (snowflake.snowpark.Table): The table to be filtered.
            new_col (str): The name of the new column to be added.
            time_col (str): The name of the first timestamp column.
            end_ts (str): The timestamp value to calculate the difference.

        Returns:
            The table with the new column added as a snowpark table object.
        """
        return table.withColumn(
            new_col, F.datediff("day", F.col(time_col), F.to_timestamp(F.lit(end_ts)))
        )

    def join_feature_table_label_table(
        self,
        feature_table: snowflake.snowpark.Table,
        label_table: snowflake.snowpark.Table,
        entity_column: str,
        join_type: str = "inner",
    ) -> snowflake.snowpark.Table:
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

    def get_distinct_values_in_column(
        self, table: snowflake.snowpark.Table, column_name: str
    ) -> List:
        """Returns the distinct values in the given column of the given table.

        Args:
            table (snowflake.snowpark.Table): The table from which the distinct values are to be extracted.
            column_name (str): The name of the column from which the distinct values are to be extracted.

        Returns:
            List: The list of distinct values in the given column of the given table.
        """
        return table.select(column_name).distinct().collect()

    def get_material_registry_table(
        self, session: snowflake.snowpark.Session, material_registry_table_name: str
    ) -> snowflake.snowpark.Table:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            material_registry_table_name (str): The material registry table name.

        Returns:
            snowflake.snowpark.Table: The filtered material registry table containing only the successfully materialized data.
        """
        material_registry_table = (
            self.get_table(session, material_registry_table_name)
            .withColumn("status", F.get_path("metadata", F.lit("complete.status")))
            .filter(F.col("status") == 2)
        )
        return material_registry_table

    def generate_type_hint(
        self, df: snowflake.snowpark.Table, column_types: Dict[str, List[str]]
    ):
        types = []
        for col in df.columns:
            if col in column_types["categorical_columns"]:
                types.append(str)
            elif col in column_types["numeric_columns"]:
                types.append(float)
            else:
                raise Exception(
                    f"Column {col} not found in the training data config either as categorical or numeric column"
                )
        return T.PandasDataFrame[tuple(types)]

    def call_prediction_udf(
        self,
        predict_data: snowflake.snowpark.Table,
        prediction_udf: Any,
        entity_column: str,
        index_timestamp: str,
        score_column_name: str,
        percentile_column_name: str,
        output_label_column: str,
        train_model_id: str,
        prob_th: Optional[float],
        input: snowflake.snowpark.Table,
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
        preds = predict_data.select(
            entity_column,
            index_timestamp,
            prediction_udf(*input).alias(score_column_name),
        ).withColumn("model_id", F.lit(train_model_id))
        if prob_th:
            preds = preds.withColumn(
                output_label_column,
                F.when(F.col(score_column_name) >= prob_th, F.lit(True)).otherwise(
                    F.lit(False)
                ),
            )
        preds_with_percentile = preds.withColumn(
            percentile_column_name,
            F.percent_rank().over(Window.order_by(F.col(score_column_name))),
        )
        return preds_with_percentile

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
        self.run_query(
            session, f"create stage if not exists {stage_name.replace('@', '')}"
        )

    def _delete_import_files(
        self,
        session: snowflake.snowpark.Session,
        stage_name: str,
        import_paths: List[str],
    ) -> None:
        """
        Deletes files from the specified Snowflake stage that match the filenames extracted from the import paths.

        Args:
            session (snowflake.snowpark.Session): A Snowflake session object to access the warehouse
            stage_name (str): The name of the Snowflake stage.
            import_paths (List[str]): The paths of the files to be deleted from the stage.

        Returns:
            None: The function does not return any value.
        """
        import_files = [element.split("/")[-1] for element in import_paths]
        files = self.run_query(session, f"list {stage_name}")
        for row in files:
            if any(substring in row.name for substring in import_files):
                self.run_query(session, f"remove @{row.name}")

    def _delete_procedures(
        self, session: snowflake.snowpark.Session, procedure_name: str
    ) -> None:
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
        It then iterates over each procedure and attempts to drop it using another SQL query.
        If an error occurs during the drop operation, it is ignored.
        """
        procedures = self.run_query(session, f"show procedures like '{procedure_name}'")
        for row in procedures:
            try:
                words = row.arguments.split(" ")[:-2]
                procedure_arguments = " ".join(words)
                self.run_query(
                    session, f"drop procedure if exists {procedure_arguments}"
                )
            except Exception as e:
                raise Exception(f"Error while dropping procedure {e}")

    def _drop_fn_if_exists(
        self, session: snowflake.snowpark.Session, fn_name: str
    ) -> bool:
        """Snowflake caches the functions and it reuses these next time. To avoid the caching,
        we manually search for the same function name and drop it before we create the udf.

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
            logger.info(
                "Function name match found. Dropping all functions with the same name"
            )
            for fn in fn_list:
                fn_signature = fn["arguments"].split("RETURN")[0]
                drop = session.sql(f"DROP FUNCTION IF EXISTS {fn_signature}")
                logger.info(drop.collect()[0].status)
            logger.info("All functions with the same name dropped")
            return True

    def _delete_table_from_train_env(
        self, session: snowflake.snowpark.Session, table_name: str, **kwargs
    ):
        """Deletes the table with the given name from the snowpark session"""
        self.run_query(session, f"drop table if exists {table_name}")

    def get_file(
        self,
        session: snowflake.snowpark.Session,
        file_stage_path: str,
        target_folder: str,
    ):
        """Fetches the file with the given path from the snowpark session to the target folder

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            file_stage_path (str): Path of the file to be fetched from the snowpark session
            target_folder (str): Folder to which the file is to be fetched

        Returns:
            Nothing
        """
        _ = session.file.get(file_stage_path, target_folder)

    def select_relevant_columns(
        self,
        table: snowflake.snowpark.Table,
        training_features_columns_upper_case: Sequence[str],
    ) -> snowflake.snowpark.Table:
        table_cols = [col.upper() for col in table.columns]
        for col in training_features_columns_upper_case:
            if col not in table_cols:
                raise Exception(
                    f"Expected feature column {col} not found in the predictions input table"
                )
        shortlisted_columns = []
        shortlisted_columns = [
            col for col in table_cols if col in training_features_columns_upper_case
        ]
        return table.select(*shortlisted_columns)

    def _job_cleanup(self, session: snowflake.snowpark.Session):
        if self.stored_procedure_name:
            self._delete_procedures(session, self.stored_procedure_name)
        if self.udf_name:
            self._drop_fn_if_exists(session, self.udf_name)
        if self.delete_files:
            self._delete_import_files(session, self.stage_name, self.delete_files)

    def pre_job_cleanup(self, session: snowflake.snowpark.Session):
        self._job_cleanup(session)

    def post_job_cleanup(self, session: snowflake.snowpark.Session):
        self._job_cleanup(session)
        if self.feature_table_name:
            self._delete_table_from_train_env(session, self.feature_table_name)
        session.close()
