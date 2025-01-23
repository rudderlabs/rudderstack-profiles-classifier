import json
import pandas as pd
from collections import namedtuple
from typing import List, Tuple, Optional, Union

import google.cloud
from google.cloud import bigquery
from google.oauth2 import service_account

from .CommonWarehouseConnector import CommonWarehouseConnector


class BigQueryConnector(CommonWarehouseConnector):
    def __init__(self, creds, folder_path: str) -> None:
        data_type_mapping = {
            "numeric": {
                "INT": float,
                "SMALLINT": float,
                "INTEGER": float,
                "BIGINT": float,
                "TINYINT": float,
                "BYTEINT": float,
                "DECIMAL": float,
                "BIGDECIMAL": float,
                "NUMERIC": float,
                "FLOAT": float,
                "BOOLEAN": float,
            },
            "categorical": {"STRING": str, "JSON": str},
            "timestamp": {
                "DATE": None,
                "TIME": None,
                "DATETIME": None,
                "TIMESTAMP": None,
                "INTERVAL": None,
            },
            "arraytype": {"ARRAY": None},
            "booleantype": {"BOOLEAN": None, "BOOL": None},
        }
        self.dtype_utils_mapping = {
            "numeric": "FLOAT",
            "categorical": "STRING",
            "timestamp": "TIMESTAMP",
        }
        super().__init__(creds, folder_path, data_type_mapping)

    def build_session(self, credentials: dict) -> google.cloud.bigquery.client.Client:
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

    def _run_query(self, query: str) -> google.cloud.bigquery.table.RowIterator:
        return self.session.query_and_wait(query)

    def run_query(self, query: str, **kwargs) -> Optional[Union[List, pd.DataFrame]]:
        """Runs the given query on the bigquery connection and returns a list with Named indices."""
        response = kwargs.get("response", True)
        return_type = kwargs.get("return_type", "sequence")

        try:
            query_run_obj = self._run_query(query)
            if response:
                if return_type == "dataframe":
                    return query_run_obj.to_dataframe()
                else:
                    return list(query_run_obj.to_dataframe().itertuples(index=False))
            else:
                return query_run_obj
        except Exception as e:
            raise Exception(f"Couldn't run the query: {query}. Error: {str(e)}")

    def get_entity_var_table_ref(self, table_name: str) -> str:
        return f"`{self.schema}`.`{table_name}`"

    def get_table_as_dataframe(
        self, _: google.cloud.bigquery.client.Client, table_name: str, **kwargs
    ) -> pd.DataFrame:
        query = self._create_get_table_query(table_name, **kwargs)
        result = self.run_query(query, response=False)

        try:
            return result.to_dataframe()
        except Exception as e1:
            try:
                schema = self._get_table_schema(table_name)
                return result.to_dataframe(
                    create_bqstorage_client=True,
                    dtypes=schema,
                    progress_bar_type=None,
                )
            except Exception as e2:
                raise Exception(
                    f"Failed to load table {table_name} from BigQuery. "
                    f"First attempt error: {str(e1)}. "
                    f"Second attempt error: {str(e2)}"
                )

    def _get_table_schema(self, table_name):
        schema_fields = self.fetch_table_metadata(table_name)
        schema = {}
        for field in schema_fields:
            if field.field_type in self.data_type_mapping["numeric"]:
                schema[field.name] = "float64"
            elif field.field_type in self.data_type_mapping["categorical"]:
                schema[field.name] = "string"
            elif field.field_type in self.data_type_mapping["timestamp"]:
                schema[field.name] = "datetime64[ns]"
            elif field.field_type in self.data_type_mapping["booleantype"]:
                schema[field.name] = "boolean"
            else:
                schema[field.name] = "string"  # Default to string for unknown types
        return schema

    def get_tablenames_from_schema(self) -> pd.DataFrame:
        query = f"SELECT DISTINCT table_name as tablename FROM `{self.project_id}.{self.schema}.INFORMATION_SCHEMA.TABLES`;"
        return self._run_query(query).to_dataframe()

    def fetch_table_metadata(self, table_name: str) -> List:
        """Fetches the schema fields of the given table from the BigQuery schema."""
        query = f"""SELECT column_name, data_type
                    FROM {self.schema}.INFORMATION_SCHEMA.COLUMNS
                    where table_schema='{self.schema}'
                        and table_name='{table_name}';"""
        schema_list = self.run_query(query)
        schema_fields = namedtuple("schema_field", ["name", "field_type"])
        named_schema_list = [schema_fields(*row) for row in schema_list]
        return named_schema_list

    def fetch_create_metrics_table_query(
        self, metrics_df: pd.DataFrame, table_name: str
    ) -> Tuple[pd.DataFrame, str]:
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
        create_metrics_table_query = f"CREATE TABLE IF NOT EXISTS `{self.project_id}.{self.schema}.{table_name}` ({metrics_table_query});"
        return metrics_df, create_metrics_table_query
