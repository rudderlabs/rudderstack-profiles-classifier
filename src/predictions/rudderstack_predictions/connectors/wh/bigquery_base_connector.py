#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bigquery connector."""

import pandas as pd
from sqlalchemy import create_engine
from ...utils.logger import logger
from .connector_base import ConnectorBase, register_connector


@register_connector
class BigqueryConnector(ConnectorBase):
    def __init__(self, creds: dict, db_config: dict, **kwargs) -> None:
        super().__init__(creds, db_config, **kwargs)
        self.engine = create_engine(
            "bigquery://", credentials_info=creds["credentials"]
        )
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
        if "." in table_name:
            schema, table_name = table_name.split(".")
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                index=False,
                if_exists=if_exists,
            )

        except Exception as e:
            logger.error(f"Error while writing to warehouse: {e}")

            # Check for non-existing schema
            err_str = f"table '{table_name}' does not exist".lower()
            if err_str in str(e).lower():
                self.create_table(df, table_name, schema)
                # Try again`
                logger.info("Trying again")
                self.write_to_table(df, table_name, schema, if_exists)
