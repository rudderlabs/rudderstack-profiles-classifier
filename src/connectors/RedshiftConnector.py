import json
import inspect
import pandas as pd
from typing import List, Tuple, Optional, Dict

import redshift_connector
import redshift_connector.cursor

from src.utils import constants
from src.connectors.CommonWarehouseConnector import CommonWarehouseConnector


class RedshiftConnector(CommonWarehouseConnector):
    def build_session(self, credentials: dict) -> redshift_connector.cursor.Cursor:
        """Builds the redshift connection session with given credentials (creds)

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            session (redshift_connector.cursor.Cursor): Redshift connection session
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
        session = conn.cursor()
        session.execute(f"SET search_path TO {self.schema};")
        return session

    def run_query(
        self, session: redshift_connector.cursor.Cursor, query: str, response=True
    ) -> Optional[Tuple]:
        """Runs the given query on the redshift connection

        Args:
            session (redshift_connector.cursor.Cursor): Redshift connection session for warehouse access
            query (str): Query to be executed on the Redshift connection
            response (bool): Whether to fetch the results of the query or not | Defaults to True

        Returns:
            Results of the query run on the Redshift connection
        """
        if response:
            return session.execute(query).fetchall()
        else:
            return session.execute(query)

    def get_table_as_dataframe(
        self, session: redshift_connector.cursor.Cursor, table_name: str, **kwargs
    ) -> pd.DataFrame:
        """Fetches the table with the given name from the Redshift schema as a pandas Dataframe object

        Args:
            session (redshift_connector.cursor.Cursor): Redshift connection session for warehouse access
            table_name (str): Name of the table to be fetched from the Redshift schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        query = self._create_get_table_query(table_name, **kwargs)
        return session.execute(query).fetch_dataframe()

    def get_tablenames_from_schema(
        self, session: redshift_connector.cursor.Cursor
    ) -> pd.DataFrame:
        """
        Fetches the table names from the Redshift schema.

        Args:
            session (redshift_connector.cursor.Cursor): The Redshift connection session for warehouse access.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the table names from the Redshift schema.
        """
        query = f"SELECT DISTINCT tablename FROM PG_TABLE_DEF WHERE schemaname = '{self.schema}';"
        return session.execute(query).fetch_dataframe()

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

    def get_timestamp_columns(
        self,
        session,
        table_name: str,
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        session.execute(
            f"select * from pg_get_cols('{self.schema}.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);"
        )
        col_df = session.fetch_dataframe()
        timestamp_columns = []
        for _, row in col_df.iterrows():
            if row["col_type"] in [
                "timestamp without time zone",
                "date",
                "time without time zone",
            ]:
                timestamp_columns.append(row["col_name"])
        return timestamp_columns

    def get_arraytype_columns(self, session, table_name: str) -> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the table from which to retrieve the arraytype/super columns.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        session.execute(
            f"select * from pg_get_cols('{self.schema}.{table_name}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);"
        )
        col_df = session.fetch_dataframe()
        arraytype_columns = []
        for _, row in col_df.iterrows():
            if row["col_type"] == "super":
                arraytype_columns.append(row["col_name"])
        return arraytype_columns

    def fetch_create_metrics_table_query(self, metrics_df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
        database_dtypes = json.loads(constants.rs_dtypes)
        metrics_table = constants.METRICS_TABLE
        metrics_table_query = ""

        for col in metrics_df.columns:
            if metrics_df[col].dtype == "object":
                metrics_df[col] = metrics_df[col].apply(lambda x: json.dumps(x))
                metrics_table_query += f"{col} {database_dtypes['text']},"
            elif metrics_df[col].dtype == "float64" or metrics_df[col].dtype == "int64":
                metrics_table_query += f"{col} {database_dtypes['num']},"
            elif metrics_df[col].dtype == "bool":
                metrics_table_query += f"{col} {database_dtypes['bool']},"
            elif metrics_df[col].dtype == "datetime64[ns]":
                metrics_table_query += f"{col} {database_dtypes['timestamp']},"

        metrics_table_query = metrics_table_query[:-1]
        create_metrics_table_query = f"CREATE TABLE IF NOT EXISTS {metrics_table} ({metrics_table_query});"
        return metrics_df, create_metrics_table_query