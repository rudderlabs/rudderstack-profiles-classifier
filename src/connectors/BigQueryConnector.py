import pandas as pd
from typing import List, Optional, Dict

import google.cloud
from google.cloud import bigquery
from google.oauth2 import service_account

from src.connectors.CommonWarehouseConnector import CommonWarehouseConnector


class BigQueryConnector(CommonWarehouseConnector):
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

    def get_non_stringtype_features(
        self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        """
        Returns a list of strings representing the names of the Non-StringType(non-categorical) columns in the feature table.

        Args:
            feature_df (pd.DataFrame): A feature table dataframe
            label_column (str): A string representing the name of the label column.
            entity_column (str): A string representing the name of the entity column.

        Returns:
            List[str]: A list of strings representing the names of the non-StringType columns in the feature table.
        """
        non_stringtype_features = feature_df.select_dtypes(
            include=["number"]
        ).columns.to_list()
        return [
            col
            for col in non_stringtype_features
            if col.lower() not in (label_column.lower(), entity_column.lower())
        ]

    def get_stringtype_features(
        self, feature_df: pd.DataFrame, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        """
        Extracts the names of StringType(categorical) columns from a given feature table schema.

        Args:
            feature_df (pd.DataFrame): A feature table dataframe
            label_column (str): The name of the label column.
            entity_column (str): The name of the entity column.

        Returns:
            List[str]: A list of StringType(categorical) column names extracted from the feature table schema.
        """
        stringtype_features = feature_df.select_dtypes(
            include=["object", "category"]
        ).columns.to_list()
        return [
            col
            for col in stringtype_features
            if col.lower() not in (label_column.lower(), entity_column.lower())
        ]

    def get_timestamp_columns(
        self,
        session,
        table_name: str,
    ) -> List[str]:
        """
        Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the feature table from which to retrieve the timestamp columns.

        Returns:
            List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
        """
        schema = session.get_table(
            f"{self.project_id}.{self.schema}.{table_name}"
        ).schema
        timestamp_columns = [
            field.name
            for field in schema
            if field.field_type in ("DATE", "DATETIME", "TIMESTAMP", "INTERVAL")
        ]
        return timestamp_columns

    def get_arraytype_columns(self, session, table_name: str) -> List[str]:
        """Returns the list of features to be ignored from the feature table.

        Args:
            session : connection session for warehouse access
            table_name (str): Name of the table from which to retrieve the arraytype/super columns.

        Returns:
            list: The list of features to be ignored based column datatypes as ArrayType.
        """
        table_df = self.get_table(session, table_name)
        arraytype_columns = []
        arraytype_columns = [
            column
            for column in table_df.columns
            if all(isinstance(value, list) for value in table_df[column])
        ]
        return arraytype_columns
