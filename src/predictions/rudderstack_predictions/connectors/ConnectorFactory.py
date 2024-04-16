from .Connector import Connector
from ..utils.logger import logger

from .RedshiftConnector import RedshiftConnector
from .SnowflakeConnector import SnowflakeConnector

try:
    from .BigQueryConnector import BigQueryConnector
except Exception as e:
    logger.warning(f"Could not import BigQueryConnector")


class ConnectorFactory:
    def create(warehouse, creds: dict, folder_path: str = None) -> Connector:
        connector = None
        if warehouse == "snowflake":
            connector = SnowflakeConnector(creds)
        elif warehouse == "redshift":
            connector = RedshiftConnector(creds, folder_path)
        elif warehouse == "bigquery":
            connector = BigQueryConnector(creds, folder_path)
        else:
            raise Exception(f"Invalid warehouse {warehouse}")
        return connector
