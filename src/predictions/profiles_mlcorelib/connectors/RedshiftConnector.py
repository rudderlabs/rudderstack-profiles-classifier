import json
import inspect
import pandas as pd
from collections import namedtuple
from typing import List, Tuple, Optional

import redshift_connector
import redshift_connector.cursor

from ..utils import constants
from .CommonWarehouseConnector import CommonWarehouseConnector


class RedshiftConnector(CommonWarehouseConnector):
    def __init__(self, creds: dict, folder_path: str) -> None:
        data_type_mapping = {
            "numeric": {
                "integer": int,
                "bigint": int,
                "float": int,
                "smallint": int,
                "decimal": int,
                "numeric": int,
                "real": int,
                "double precision": int,
            },
            "categorical": {"str": str, "character varying": str},
            "timestamp": {
                "timestamp without time zone": None,
                "date": None,
                "time without time zone": None,
            },
            "arraytype": {"super": None},
            "booleantype": {"boolean": None, "bool": None},
        }
        self.dtype_utils_mapping = {"numeric": "float", "categorical": "str"}
        super().__init__(creds, folder_path, data_type_mapping)

    def build_session(self, credentials: dict) -> redshift_connector.cursor.Cursor:
        self.schema = credentials.pop("schema")
        self.creds = credentials
        self.connection_parameters = self.remap_credentials(credentials)
        valid_params = inspect.signature(redshift_connector.connect).parameters
        conn_params = {
            k: v for k, v in self.connection_parameters.items() if k in valid_params
        }
        conn = redshift_connector.connect(**conn_params)
        self.creds["schema"] = self.schema
        conn.autocommit = True
        session = conn.cursor()
        session.execute(f"SET search_path TO {self.schema};")
        return session

    def run_query(self, query: str, response=True) -> Optional[List]:
        """Runs the given query on the redshift connection and returns a Named Tuple."""
        if response:
            query_run_obj = self.session.execute(query)
            if query_run_obj.description:
                column_names = [desc[0] for desc in query_run_obj.description]
                row_outputs = query_run_obj.fetchall()
                Row = namedtuple("Row", column_names)
                query_output = [Row(*row) for row in row_outputs]
                return query_output
            else:
                raise Exception(
                    "No result set is present for given query. Please check the query."
                )
        else:
            return self.session.execute(query)

    def get_entity_var_table_ref(self, table_name: str) -> str:
        return f'"{self.schema}"."{table_name.lower()}"'

    def get_entity_column_case_corrected(self, entity_column: str) -> str:
        return entity_column.lower()

    def get_table_as_dataframe(
        self, _: redshift_connector.cursor.Cursor, table_name: str, **kwargs
    ) -> pd.DataFrame:
        query = self._create_get_table_query(table_name, **kwargs)
        return self.session.execute(query).fetch_dataframe()

    def get_tablenames_from_schema(self) -> pd.DataFrame:
        query = f"SELECT DISTINCT tablename FROM PG_TABLE_DEF WHERE schemaname = '{self.schema}';"
        return self.session.execute(query).fetch_dataframe()

    def fetch_table_metadata(self, table_name: str) -> List:
        """Fetches the schema fields(column_name, data_type) tuple of the given table."""
        query = f"""SELECT column_name, data_type
                    FROM information_schema.columns
                    where table_schema='{self.schema}'
                        and table_name='{table_name.lower()}';"""
        schema_list = self.run_query(query)
        schema_fields = namedtuple("schema_field", ["name", "field_type"])
        named_schema_list = [schema_fields(*row) for row in schema_list]
        return named_schema_list

    def fetch_create_metrics_table_query(
        self,
        metrics_df: pd.DataFrame,
        table_name: str,
    ) -> Tuple[pd.DataFrame, str]:
        database_dtypes = json.loads(constants.rs_dtypes)
        metrics_table_query = ""

        for col in metrics_df.columns:
            if metrics_df[col].dtype == "object":
                metrics_df[col] = metrics_df[col].apply(lambda x: json.dumps(x))
                metrics_table_query += f"{col} {database_dtypes['text']},"
            elif metrics_df[col].dtype == "float64" or metrics_df[col].dtype == "int64":
                metrics_table_query += f"{col} {database_dtypes['num']},"
            elif metrics_df[col].dtype == "bool":
                metrics_table_query += f"{col} {database_dtypes['bool']},"
            elif metrics_df[col].dtype == "datetime64[ns]":
                metrics_table_query += f"{col} {database_dtypes['timestamp']},"

        metrics_table_query = metrics_table_query[:-1]
        create_metrics_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({metrics_table_query});"
        )
        return metrics_df, create_metrics_table_query
