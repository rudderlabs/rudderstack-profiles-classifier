#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Amazon RedShift Connector."""

import pandas as pd
import urllib.parse
import pandas_redshift as pr
from typing import Literal

from sqlalchemy import create_engine
from sqlalchemy import orm as sa_orm

from logging import Logger
from wh.connector_base import ConnectorBase, register_connector

@register_connector
class RedShiftConnector(ConnectorBase):
    def __init__(self, creds: dict, db_config: dict, **kwargs) -> None:
        super().__init__(creds, db_config, **kwargs)
        self.logger = Logger("RedShiftConnector")

        self.s3_config = kwargs.get("s3_config", None)
        encoded_password = urllib.parse.quote(creds["password"], safe="")
        connection_string = f"postgresql://{creds['user']}:{encoded_password}@{creds['host']}:{creds['port']}/{db_config['database']}"
        self.engine = create_engine(connection_string)

        Session = sa_orm.sessionmaker()
        Session.configure(bind=self.engine)
        self.connection = Session()
        if db_config.get("schema", None):
            self.connection.execute(f"SET search_path TO {db_config['schema']}")
            self.connection.commit()

    def write_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: Literal['fail', 'replace', 'append'] = "append",
    ):
        table_name, schema = (
            table_name.split(".") if "." in table_name else (table_name, schema)
        )

        try:
            if self.s3_config is None:
                df.to_sql(name=table_name.lower(), con=self.engine, schema=schema, index=False, if_exists=if_exists)
            else:
                pr.connect_to_redshift(
                    dbname=self.db_config["database"],
                    host=self.creds["host"],
                    port=self.creds["port"],
                    user=self.creds["user"],
                    password=self.creds["password"],
                )

                s3_bucket = self.s3_config.get("bucket", None)
                s3_sub_dir = self.s3_config.get("path", None)

                pr.connect_to_s3(
                    aws_access_key_id=self.s3_config["access_key_id"],
                    aws_secret_access_key=self.s3_config["access_key_secret"],
                    bucket=s3_bucket,
                    subdirectory=s3_sub_dir
                    # As of release 1.1.1 you are able to specify an aws_session_token (if necessary):
                    # aws_session_token = <aws_session_token>
                )

                # Write the DataFrame to S3 and then to redshift
                pr.pandas_to_redshift(
                    data_frame=df,
                    redshift_table_name=f"{schema}.{table_name}",
                    append=if_exists == "append",
                )
        except Exception as e:
            #Check for non existing schema
            if "cannot copy into nonexistent table" in str(e).lower():
                self.logger.info(f"{table_name} not found. Creating it")
                self.create_table(df, table_name, schema)
                # Try again
                self.logger.info("Trying again")
                self.write_to_table(df, table_name, schema, if_exists)
            else:
                raise e