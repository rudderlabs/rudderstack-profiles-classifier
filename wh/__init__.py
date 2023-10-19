#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Module for handling various warehouse connections"""

from typing import Union

from connector_base import connector_classes
from redshift_connector import RedShiftConnector
from snowflake_connector import SnowflakeConnector

# SnowflakeConnector not used currently in profiles_rudderstack
def ProfilesConnector(config: dict, **kwargs) -> Union[RedShiftConnector, SnowflakeConnector]:
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

    db_config = {
        "database": config.get("dbname"),
        "schema": config.get("schema")
    }

    connector = connector(creds, db_config, **kwargs)
    return connector