import pandas as pd
from typing import List, Optional, Dict

import google.cloud
from google.cloud import bigquery
from google.oauth2 import service_account

from src.connectors.CommonWarehouseConnector import CommonWarehouseConnector


class BigQueryConnector(CommonWarehouseConnector):
    def build_session(self, credentials: dict) -> google.cloud.bigquery.client.Client:
        """Builds the BigQuery connection client with given credentials (creds)

        Args:
            creds (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            client (google.cloud.bigquery.client.Client): BigQuery connection client
        """
        self.schema = credentials.get("schema", None)
        self.project_id = credentials.get("project_id", None)
        self.creds = credentials
        bq_credentials = service_account.Credentials.from_service_account_info(
            credentials["credentials"]
        )
        client = bigquery.Client(
            project=credentials["project_id"],
            credentials=bq_credentials,
            default_query_job_config=bigquery.QueryJobConfig(
                default_dataset=f"{credentials['project_id']}.{credentials['schema']}"
            ),
        )
        return client

    def run_query(
        self, client: google.cloud.bigquery.client.Client, query: str, response=True
    ) -> Optional[List]:
        """Runs the given query on the bigquery connection

        Args:
            client (google.cloud.bigquery.client.Client): BigQuery connection client for warehouse access
            query (str): Query to be executed on the BigQuery connection
            response (bool): Whether to fetch the results of the query or not | Defaults to True

        Returns:
            Results of the query run on the BigQuery connection
        """
        if response:
            return list(
                client.query_and_wait(query).to_dataframe().itertuples(index=False)
            )
        else:
            return client.query_and_wait(query)

    def get_table_as_dataframe(
        self, client: google.cloud.bigquery.client.Client, table_name: str, **kwargs
    ) -> pd.DataFrame:
        """Fetches the table with the given name from the BigQuery schema as a pandas Dataframe object

        Args:
            client (google.cloud.bigquery.client.Client): BigQuery connection cursor for warehouse access
            table_name (str): Name of the table to be fetched from the BigQuery schema

        Returns:
            table (pd.DataFrame): The table as a pandas Dataframe object
        """
        query = self._create_get_table_query(table_name, **kwargs)
        return client.query_and_wait(query).to_dataframe()

    def get_tablenames_from_schema(
        self, client: google.cloud.bigquery.client.Client
    ) -> pd.DataFrame:
        """
        Fetches the table names from the BigQuery schema.

        Args:
            client (google.cloud.bigquery.client.Client): BigQuery connection client for warehouse access

        Returns:
            pd.DataFrame: A pandas DataFrame containing the table names from the BigQuery schema.
        """
        query = f"SELECT DISTINCT table_name as tablename FROM `{self.project_id}.{self.schema}.INFORMATION_SCHEMA.TABLES`;"
        return client.query_and_wait(query).to_dataframe()
