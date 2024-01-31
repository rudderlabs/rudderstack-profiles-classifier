#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SnowFlake connector."""


import pandas as pd
import urllib.parse

from sqlalchemy import create_engine

from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect

from src.wh.connector_base import ConnectorBase, register_connector
from logging import Logger

@register_connector
class SnowflakeConnector(ConnectorBase):
    def __init__(self, creds: dict, db_config: dict, **kwargs) -> None:
        super().__init__(creds, db_config, **kwargs)
        self.logger = Logger("snowflake_connector")

        encoded_password = urllib.parse.quote(creds["password"], safe="") 
        url = f"snowflake://{creds['user']}:{encoded_password}@{creds['account_identifier']}"
        if "database" in db_config:
            url += f"/{db_config['database']}"
            if "schema" in db_config:
                url += f"/{db_config['schema']}"
                if "warehouse" in creds:
                    url += f"?warehouse={creds['warehouse']}"
                    if "role" in creds:
                        url += f"&role={creds['role']}"
        self.engine = create_engine(url)
        self.connection = self.engine.connect()
        self.creds = creds
        self.db_config = db_config

    def write_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = None,
        if_exists: str = "append",
    ):
        table_name, schema = (
            table_name.split(".") if "." in table_name else (table_name, schema)
        )
        
        try:
            connection_params = {"user": self.creds["user"],
                                 "password": self.creds["password"],
                                 "account": self.creds["account_identifier"],
                                 "database": self.db_config["database"],
                                 "schema":schema}

            if "warehouse" in self.creds:
                connection_params["warehouse"] = self.creds["warehouse"]

            if "role" in self.creds:
                connection_params["role"] = self.creds["role"]

            write_conn = connect(**connection_params)

            success, nchunks, nrows, _ = write_pandas(write_conn, df, table_name, quote_identifiers=False)

            # if success:
            #     print(f"Successfully wrote {nrows} rows across {nchunks} chunks to table {table_name}")
            # else:
            #     print("Write failed")
        except Exception as e:
            self.logger.error(f"Error while writing to Snowflake: {e}")

            #Check for non existing schema
            err_str = f"table '{table_name}' does not exist".lower()
            if err_str in str(e).lower():
                self.create_table(df, table_name, schema)
                # Try again
                self.logger.info("Trying again")
                self.write_to_table(df, table_name, schema, if_exists)