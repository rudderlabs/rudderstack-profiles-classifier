from .Connector import Connector
from ..utils.logger import logger

from .RedshiftConnector import RedshiftConnector
from .SnowflakeConnector import SnowflakeConnector

try:
    from .BigQueryConnector import BigQueryConnector
except Exception as e:
    logger.get().error(f"Could not import BigQueryConnector")


class ConnectorFactory:
    def create(creds: dict, folder_path: str = None) -> Connector:
        connector = None
        warehouse = creds["type"]
        if warehouse == "snowflake":
            connector = SnowflakeConnector(creds)
        elif warehouse == "redshift":
            connector = RedshiftConnector(creds, folder_path)
        elif warehouse == "bigquery":
            connector = BigQueryConnector(creds, folder_path)
        else:
            raise Exception(f"Invalid warehouse {warehouse}")
        return connector
