import json
import inspect
import pandas as pd
from collections import namedtuple
from typing import List, Tuple, Optional

import redshift_connector
import redshift_connector.cursor

from ..utils import constants
from .CommonWarehouseConnector import CommonWarehouseConnector


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

    def fetch_schema(
        self, session: redshift_connector.cursor.Cursor, table_name: str
    ) -> List:
        """Fetches the (column_name, data_type) tuple of the given table."""
        query = f"""SELECT column_name, data_type
                    FROM information_schema.columns
                    where table_schema='{self.schema}'
                        and table_name='{table_name}';"""
        schema_list = self.run_query(session, query)
        schemaFields = namedtuple("schemaFields", ["name", "field_type"])
        named_schema_list = [schemaFields(*row) for row in schema_list]
        return named_schema_list

    def get_non_stringtype_features(
        self,
        session,
        table_name: str,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        numeric_data_types = (
            "integer",
            "bigint",
            "float",
            "smallint",
            "decimal",
            "numeric",
            "real",
            "double precision",
        )
        return self.fetch_given_data_type_columns(
            session, table_name, numeric_data_types, label_column, entity_column
        )

    def get_stringtype_features(
        self,
        session,
        table_name: str,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        stringtype_data_types = ("character varying", "super")
        return self.fetch_given_data_type_columns(
            session, table_name, stringtype_data_types, label_column, entity_column
        )

    def get_timestamp_columns(
        self,
        session,
        table_name: str,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        timestamp_data_types = (
            "timestamp without time zone",
            "date",
            "time without time zone",
        )
        return self.fetch_given_data_type_columns(
            session, table_name, timestamp_data_types, label_column, entity_column
        )

    def get_arraytype_columns(
        self,
        session,
        table_name: str,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the table from which to retrieve the arraytype/super columns.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        arraytype_data_types = "array"
        return self.fetch_given_data_type_columns(
            session, table_name, arraytype_data_types, label_column, entity_column
        )

    def fetch_create_metrics_table_query(
        self, metrics_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, str]:
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
        create_metrics_table_query = (
            f"CREATE TABLE IF NOT EXISTS {metrics_table} ({metrics_table_query});"
        )
        return metrics_df, create_metrics_table_query
