import inspect
import pandas as pd
from typing import Tuple, Optional, Dict

import redshift_connector
import redshift_connector.cursor

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