import json
import pandas as pd
from typing import List, Tuple, Optional

import google.cloud
from google.cloud import bigquery
from google.oauth2 import service_account

from ..utils import constants
from .CommonWarehouseConnector import CommonWarehouseConnector


class BigQueryConnector(CommonWarehouseConnector):
    def __init__(self, folder_path: str) -> None:
        data_type_mapping = {
            "numeric": (
                "INT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "TINYINT",
                "BYTEINT",
                "DECIMAL",
                "BIGDECIMAL",
            ),
            "categorical": ("STRING", "JSON"),
            "timestamp": ("DATE", "TIME", "DATETIME", "TIMESTAMP", "INTERVAL"),
            "arraytype": ("ARRAY",),
        }
        super().__init__(folder_path, data_type_mapping)

    def build_session(self, credentials: dict) -> google.cloud.bigquery.client.Client:
        """Builds the BigQuery connection session with given credentials (creds)

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            session (google.cloud.bigquery.client.Client): BigQuery connection session
        """
        self.schema = credentials.get("schema", None)
        self.project_id = credentials.get("project_id", None)
        self.creds = credentials
        bq_credentials = service_account.Credentials.from_service_account_info(
            credentials["credentials"]
        )
        session = bigquery.Client(
            project=credentials["project_id"],
            credentials=bq_credentials,
            default_query_job_config=bigquery.QueryJobConfig(
                default_dataset=f"{credentials['project_id']}.{credentials['schema']}"
            ),
        )
        return session

    def run_query(
        self, session: google.cloud.bigquery.client.Client, query: str, response=True
    ) -> Optional[List]:
        """Runs the given query on the bigquery connection

        Args:
            session (google.cloud.bigquery.client.Client): BigQuery connection session for warehouse access
            query (str): Query to be executed on the BigQuery connection
            response (bool): Whether to fetch the results of the query or not | Defaults to True

        Returns:
            Results of the query run on the BigQuery connection
        """
        if response:
            return list(
                session.query_and_wait(query).to_dataframe().itertuples(index=False)
            )
        else:
            return session.query_and_wait(query)

    def get_table_as_dataframe(
        self, session: google.cloud.bigquery.client.Client, table_name: str, **kwargs
    ) -> pd.DataFrame:
        """Fetches the table with the given name from the BigQuery schema as a pandas Dataframe object

        Args:
            session (google.cloud.bigquery.client.Client): BigQuery connection cursor for warehouse access
            table_name (str): Name of the table to be fetched from the BigQuery schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        query = self._create_get_table_query(table_name, **kwargs)
        return session.query_and_wait(query).to_dataframe()

    def get_tablenames_from_schema(
        self, session: google.cloud.bigquery.client.Client
    ) -> pd.DataFrame:
        """
        Fetches the table names from the BigQuery schema.

        Args:
            session (google.cloud.bigquery.client.Client): BigQuery connection session for warehouse access

        Returns:
            pd.DataFrame: A pandas DataFrame containing the table names from the BigQuery schema.
        """
        query = f"SELECT DISTINCT table_name as tablename FROM `{self.project_id}.{self.schema}.INFORMATION_SCHEMA.TABLES`;"
        return session.query_and_wait(query).to_dataframe()

    def fetch_table_metadata(
        self, session: google.cloud.bigquery.client.Client, table_name: str
    ) -> List:
        """
        Fetches the schema of the given table from the BigQuery schema.

        Args:
            session (google.cloud.bigquery.client.Client): BigQuery connection session for warehouse access
            table_name (str): Name of the table to be fetched from the BigQuery schema

        Returns:
            List: A list containing the schema of the given table from the BigQuery schema.
        """
        schema = session.get_table(
            f"{self.project_id}.{self.schema}.{table_name}"
        ).schema
        return schema

    def fetch_create_metrics_table_query(
        self, metrics_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, str]:
        metrics_table = constants.METRICS_TABLE
        metrics_table_query = ""

        for col in metrics_df.columns:
            if (
                metrics_df[col].dtype == "object"
            ):  # can't find a str using "in" keyword as it's numpy dtype
                metrics_df[col] = metrics_df[col].apply(lambda x: json.dumps(x))
                metrics_table_query += f"{col} STRING,"
            elif (
                metrics_df[col].dtype == "float64"
                or metrics_df[col].dtype == "int64"
                or metrics_df[col].dtype == "Float64"
                or metrics_df[col].dtype == "Int64"
            ):
                metrics_table_query += f"{col} INTEGER,"
            elif metrics_df[col].dtype == "bool":
                metrics_table_query += f"{col} BOOL,"
            elif (
                metrics_df[col].dtype == "datetime64[ns]"
                or metrics_df[col].dtype == "datetime64[ns, UTC]"
            ):
                metrics_table_query += f"{col} TIMESTAMP,"

        metrics_table_query = metrics_table_query[:-1]
        create_metrics_table_query = f"CREATE TABLE IF NOT EXISTS {self.project_id}.{self.schema}.{metrics_table} ({metrics_table_query});"
        return metrics_df, create_metrics_table_query
