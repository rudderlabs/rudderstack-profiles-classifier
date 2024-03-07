from ..utils import logger

from .RedshiftConnector import RedshiftConnector
from .SnowflakeConnector import SnowflakeConnector

try:
    from .BigQueryConnector import BigQueryConnector
except Exception as e:
    logger.warning(f"Could not import BigQueryConnector")


class ConnectorFactory:
    def create(warehouse, folder_path: str = None):
        connector = None
        if warehouse == "snowflake":
            connector = SnowflakeConnector()
        elif warehouse == "redshift":
            connector = RedshiftConnector(folder_path)
        elif warehouse == "bigquery":
            connector = BigQueryConnector(folder_path)
        else:
            raise Exception(f"Invalid warehouse {warehouse}")
        return connector
