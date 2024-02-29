#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Module for handling various warehouse connections"""

from typing import Union

from src.connectors.wh.redshift_base_connector import RedShiftConnector
from src.connectors.wh.snowflake_base_connector import SnowflakeConnector
from src.connectors.wh.bigquery_base_connector import BigqueryConnector
from src.connectors.wh.connector_base import connector_classes


# SnowflakeConnector not used currently in profiles_rudderstack
def ProfilesConnector(
    config: dict, **kwargs
) -> Union[RedShiftConnector, SnowflakeConnector, BigqueryConnector]:
    """Creates a connector object based on the config provided

    Args:
        config: A dictionary containing the credentials and database information for the connector.
        **kwargs: Additional keyword arguments to pass to the connector.

    Returns:
        ConnectorBase: Connector object.

    Raises:
        Exception: Connector not found
    """

    warehouse_type = config.get("type").lower()
    connector = connector_classes.get(warehouse_type, None)
    if connector is None:
        raise Exception(f"Connector {warehouse_type} not found")

    creds = {
        "user": config.get("user"),
        "password": config.get("password"),
        "account_identifier": config.get("account"),
        "warehouse": config.get("warehouse"),
        "host": config.get("host"),
        "port": config.get("port"),
    }

    if "role" in config:
        creds["role"] = config.get("role")
    if "access_token" in config:
        creds["access_token"] = config.get("access_token")
    if "http_endpoint" in config:
        creds["http_endpoint"] = config.get("http_endpoint")
    if "credentials" in config:
        creds["credentials"] = config.get("credentials")

    db_config = {"database": config.get("dbname"), "schema": config.get("schema")}

    if "catalog" in config:
        db_config["catalog"] = config.get("catalog")

    if "project_id" in config:
        db_config["project_id"] = config.get("project_id")

    connector = connector(creds, db_config, **kwargs)
    return connector
