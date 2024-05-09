import json
import pandas as pd
import bigframes
import bigframes.pandas as bpd
from typing import List, Tuple, Optional

import google.cloud
from google.cloud import bigquery
from google.oauth2 import service_account

from ..utils import constants
from .CommonWarehouseConnector import CommonWarehouseConnector


class BigQueryConnector(CommonWarehouseConnector):
    def __init__(self, creds, folder_path: str) -> None:
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
                "NUMERIC",
                "FLOAT",
            ),
            "categorical": ("STRING", "JSON"),
            "timestamp": ("DATE", "TIME", "DATETIME", "TIMESTAMP", "INTERVAL"),
            "arraytype": ("ARRAY",),
            "booleantype": ("BOOLEAN", "BOOL"),
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
        bpd.options.bigquery.project = self.project_id
        return session

    def run_query(self, query: str, response=True) -> Optional[List]:
        """Runs the given query on the bigquery connection and returns a list with Named indices."""
        try:
            if response:
                return list(
                    self.session.query_and_wait(query)
                    .to_dataframe()
                    .itertuples(index=False)
                )
            else:
                return self.session.query_and_wait(query)
        except Exception as e:
            raise Exception(f"Couldn't run the query: {query}. Error: {str(e)}")

    ## Functions added by me
    def _create_get_table_query(self, table_name, **kwargs):
        filter_condition = kwargs.get("filter_condition", "")
        query = f"SELECT * FROM {self.project_id}.{self.schema}.{table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ";"
        return query

    def get_material_registry_table(
        self,
        material_registry_table_name: str,
    ) -> pd.DataFrame:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.
        """
        material_registry_table = self.get_table(material_registry_table_name)

        def safe_parse_json(entry):
            try:
                if isinstance(
                    entry, str
                ):  # If the entry is a string(for redshift), parse it as JSON
                    entry_dict = eval(entry)
                elif isinstance(
                    entry, dict
                ):  # If the entry is already a dictionary(for bigquery), use it directly
                    entry_dict = entry

                return entry_dict.get("complete", {}).get("status")
            except:
                return None

        material_registry_table["status"] = material_registry_table["metadata"].apply(
            safe_parse_json, by_row=False
        )
        return material_registry_table[material_registry_table["status"] == 2]

    def get_table(self, table_name: str, **kwargs) -> bigframes.dataframe.DataFrame:
        """Fetches the table with the given name from the schema as a pandas Dataframe object."""
        query = self._create_get_table_query(table_name, **kwargs)
        return bpd.read_gbq(query)

    ############################
    def get_table_as_dataframe(
        self, _: google.cloud.bigquery.client.Client, table_name: str, **kwargs
    ) -> pd.DataFrame:
        query = self._create_get_table_query(table_name, **kwargs)
        return self.session.query_and_wait(query).to_dataframe()

    def get_tablenames_from_schema(self) -> pd.DataFrame:
        query = f"SELECT DISTINCT table_name as tablename FROM `{self.project_id}.{self.schema}.INFORMATION_SCHEMA.TABLES`;"
        return self.session.query_and_wait(query).to_dataframe()

    def fetch_table_metadata(self, table_name: str) -> List:
        """Fetches the schema fields of the given table from the BigQuery schema."""
        schema = self.session.get_table(
            f"{self.project_id}.{self.schema}.{table_name}"
        ).schema
        return schema

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
