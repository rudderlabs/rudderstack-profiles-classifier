#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bigquery connector."""

import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
from ...utils.logger import logger
from .connector_base import ConnectorBase


class BigqueryConnector(ConnectorBase):
    def __init__(self, creds: dict, db_config: dict, **kwargs) -> None:
        super().__init__(creds, db_config, **kwargs)
        self.engine = create_engine(
            "bigquery://", credentials_info=creds["credentials"]
        )
        self.bq_credentials = service_account.Credentials.from_service_account_info(
            creds["credentials"]
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
            destination_table_path = f"""{self.db_config["project_id"]}.{schema.replace("`", "")}.{table_name.replace("`", "")}"""
            pd.DataFrame.to_gbq(
                df,
                destination_table_path,
                project_id=self.db_config["project_id"],
                if_exists="append",
                credentials=self.bq_credentials,
            )

        except Exception as e:
            logger.get().error(f"Error while writing to warehouse: {e}")
            raise Exception(f"Error while writing to warehouse: {e}")
