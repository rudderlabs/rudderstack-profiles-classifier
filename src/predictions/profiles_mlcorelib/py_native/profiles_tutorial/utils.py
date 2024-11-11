from snowflake.snowpark.session import Session
from typing import Literal

# TODO: Use the Snowpark Connector from profiles_rudderstack


def remap_credentials(credentials: dict) -> dict:
    """Remaps credentials from profiles siteconfig to the expected format from snowflake session"""
    new_creds = {
        k if k != "dbname" else "database": v
        for k, v in credentials.items()
        if k != "type"
    }
    return new_creds


class SnowparkConnector:
    def __init__(self, wh_credentials: dict):
        self.wh_credentials = wh_credentials
        self.session = self.create_session()

    def create_session(self) -> Session:
        connection_parameters = remap_credentials(self.wh_credentials)
        return Session.builder.configs(connection_parameters).create()

    def run_query(self, query: str, output_type: Literal["pandas", "list"] = "list"):
        if output_type == "pandas":
            return self.session.sql(query).to_pandas()
        else:
            return self.session.sql(query).collect()
