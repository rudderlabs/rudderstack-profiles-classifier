#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Base class for all other warehouse connectors."""

import pandas as pd


class ConnectorBase:
    """
    Base class for all warehouse connectors.
    """

    def __init__(self, creds: dict, db_config: dict, **kwargs):
        """Initializes base class for all warehouse connectors.

        Args:
            creds: Dictionary containing credentials for the warehouse.
            db_config: Dictionary containing database configuration.
        """
        self.creds = creds
        self.db_config = db_config
        self.kwargs = kwargs
        self.engine = None
        self.connection = None

    def run_query(self, query: str):
        """Runs a query on the warehouse.

        Args:
            query: Query to be run.
        """
        query_result = self.connection.execute(query)
        df = pd.DataFrame(query_result.fetchall())
        if len(df) > 0:
            df.columns = query_result.keys()
        else:
            columns = query_result.keys()
            df = pd.DataFrame(columns=columns)
        return df

    def write_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = None,
        if_exists: str = "append",
    ):
        """Writes given dataframe to warehouse table.

        Args:
            df: Pandas dataframe containing data to be written to warehouse.
            table_name: Table name
            schema: Database schema
            if_exists: tag, Wether to append or replace data in the table.

        Raises:
            NotImplementedError: This method is not implemented (Should be implemented in subclass).
        """
        raise NotImplementedError()

    def create_table(self, df: pd.DataFrame, table_name: str, schema: str = None):
        """Creates a table in the warehouse.

        Args:
            df: Pandas dataframe containing data to be written to warehouse.
            table_name: Table name
            schema: Database schema
        """
        try:
            create_statement = get_create_statement(df, table_name, schema)
            has_commit = hasattr(self.connection, "commit")
            if not has_commit:
                self.connection.autocommit = True

            self.connection.execute(create_statement)

            if has_commit:
                self.connection.commit()
            else:
                self.connection.autocommit = False

        except Exception as e:
            raise e

    def __del__(self):
        """Delete function for connector base class.

        Properly closes connection to warehouse.
        """
        self.connection.close()
        if self.engine is not None:
            self.engine.dispose()
