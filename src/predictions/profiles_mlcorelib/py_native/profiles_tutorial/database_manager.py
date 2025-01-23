import os
import pandas as pd
import logging
from typing import List, Dict, Tuple, Any, Optional
from ruamel.yaml import YAML
import sys

from .io_handler import IOHandler
from profiles_rudderstack.material import WhtMaterial

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, material: WhtMaterial, io: IOHandler, fast_mode: bool):
        self.material = material
        self.client = material.wht_ctx.client
        self.schema = self.client.schema
        self.db = self.client.db
        self.io = io
        self.fast_mode = fast_mode

    def get_case_sensitive_name(self, table_name: str, respect_case=False) -> str:
        # Returns a table name that is case-sensitive according to the warehouse defaults
        # TODO: Use wht client methods instead
        if respect_case:
            return table_name
        if self.client.wh_type == "snowflake":
            return table_name.upper()
        else:
            return table_name

    def get_qualified_name(self, table: str, respect_case=False) -> str:
        """Returns the fully qualified name of the table"""
        # TODO: Use wht function here
        table_name = table
        if not respect_case and self.client.wh_type == "bigquery":
            table_name = table.lower().capitalize()

        return f"{self.db}.{self.schema}.{table_name}"

    def get_table_names(self) -> List[str]:
        # Ref taken from sqlconnect-go
        # TODO: Use wht client methods instead
        template = f"""
        {{% if warehouse.DatabaseType() == "snowflake" %}}
            SHOW TABLES IN SCHEMA {self.db}.{self.schema}
        {{% elif warehouse.DatabaseType() == "databricks" %}}
            SHOW TABLES IN {self.db}.{self.schema}
        {{% elif warehouse.DatabaseType() == "redshift" %}}
            SELECT table_name as name FROM {self.db}.information_schema.tables WHERE table_schema = '{self.schema}'
        {{% elif warehouse.DatabaseType() == "bigquery" %}}
            SELECT table_name as name FROM {self.db}.{self.schema}.INFORMATION_SCHEMA.TABLES
        {{% endif %}}"""
        sql = self.material.execute_text_template(template, skip_material_wrapper=True)
        res = self.client.query_sql_with_result(sql)
        table_name_col = "tableName" if "tableName" in res.columns else "name"
        tables = [
            self.get_case_sensitive_name(row[table_name_col].lower())
            for _, row in res.iterrows()
        ]
        return tables

    def upload_sample_data(self, sample_data_path: str, table_suffix: str) -> dict:
        new_table_names = {}
        to_upload = True
        existing_tables = self.get_table_names()

        for filename in os.listdir(sample_data_path):
            if not filename.endswith(".csv"):
                continue

            base_name = os.path.splitext(filename)[0]
            table_name = self.get_case_sensitive_name(
                f"{base_name}_{table_suffix}".lower()
            )
            new_table_names[filename] = table_name

            if not to_upload:
                continue

            if table_name in existing_tables:
                action = self.io.get_user_input(
                    f"Table {table_name} already exists. Do you want to skip uploading again, so we can reuse the tables? (yes/no) (yes - skips upload, no - uploads again): "
                )
                if action == "yes":
                    self.io.display_message("Skipping upload of all csv files.")
                    to_upload = False
                    continue

                self.client.query_template_without_result(
                    self.material,
                    f"""{{% set tbl = warehouse.NamedWhObject(name='{table_name}', type="TABLE") %}} {{% exec %}}{{{{warehouse.ForceDropTableStatement(tbl)}}}}{{% endexec %}}""",
                )

            df = pd.read_csv(os.path.join(sample_data_path, filename))
            self.io.display_message(
                f"Uploading file {filename} as table {table_name} with {df.shape[0]} rows and {df.shape[1]} columns"
            )
            self.client.write_df_to_table(
                df,
                table_name,
                schema=self.schema,
                append_if_exists=False,
            )
        return new_table_names

    def find_relevant_tables(self, new_table_names: dict) -> List[str]:
        tables = self.get_table_names()
        new_tables = [new_table.lower() for new_table in new_table_names.values()]
        relevant_tables = sorted(
            [table.lower() for table in tables if table.lower() in new_tables]
        )
        return relevant_tables

    def get_columns(self, table: str) -> List[str]:
        try:
            template = f"""
                {{% if warehouse.DatabaseType() == "snowflake" or warehouse.DatabaseType() == "databricks" %}}
                    DESCRIBE TABLE {self.db}.{self.schema}.{table}
                {{% elif warehouse.DatabaseType() == "bigquery" %}}
                    SELECT column_name as name, data_type FROM {self.db}.{self.schema}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}'
                {{% elif warehouse.DatabaseType() == "redshift" %}}
                    SELECT column_name as name, data_type FROM {self.db}.information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = '{table}'
                {{% endif %}}
            """
            sql = self.material.execute_text_template(
                template, skip_material_wrapper=True
            )
            result = self.client.query_sql_with_result(sql)
            columns = [row["name"] for _, row in result.iterrows()]
        except Exception as e:
            raise Exception(f"Error fetching columns for {table}: {e}")
        return columns

    def get_sample_data(
        self, table: str, column: str, num_samples: int = 5
    ) -> List[str]:
        try:
            query = f"SELECT {column} FROM {self.get_qualified_name(table.lower(), True)} where {column} is not null LIMIT {num_samples}"
            df: pd.DataFrame = self.client.query_sql_with_result(query)
            if df.empty:
                return []
            samples = df.iloc[:, 0].dropna().astype(str).tolist()
            return samples[:num_samples]
        except Exception as e:
            logger.error(
                f"Error fetching sample data for column '{column}' in table '{table}': {e}"
            )
            return []

    def map_columns_to_id_types(
        self, table: str, id_types: List[str], entity_name: str
    ) -> Tuple[Optional[List[Dict[Any, Any]]], str]:
        try:
            columns = self.get_columns(table)
        except Exception as e:
            raise Exception(f"Error fetching columns for {table}: {e}")

        id_type_mapping = {"anon_id": "anonymous_id"}
        # Shortlist columns based on regex matches with id_types
        shortlisted_columns = {}
        for id_type in id_types:
            # cleaned_id_type = re.sub(r'(id|_)', '', id_type, flags=re.IGNORECASE)
            # Create pattern that ignores 'id' and '_' in column names
            # pattern = re.compile(rf".*{re.escape(cleaned_id_type)}.*", re.IGNORECASE)
            # pattern = re.compile(rf".*{id_type}.*", re.IGNORECASE)
            # matched_columns = [col for col in columns if pattern.match(re.sub(r'(id|_)', '', col, flags=re.IGNORECASE))]
            matched_columns = [
                col
                for col in columns
                if col.lower() == id_type.lower()
                or col.lower() == id_type_mapping.get(id_type, id_type).lower()
            ]
            if matched_columns:
                shortlisted_columns[id_type] = matched_columns

        # Display table context
        self.io.display_message(f"\n{'-'*80}\n")
        self.io.display_message(
            f"The table `{table}` has the following columns, which look like id types:\n"
        )

        # Display shortlisted columns with sample data
        for id_type, cols in shortlisted_columns.items():
            for col in cols:
                sample_data = self.get_sample_data(table, col)
                self.io.display_message(f"id_type: {id_type}")
                self.io.display_message(f"column: {col} (sample data: {sample_data})\n")

        # Display all available id_types
        self.io.display_message(
            f"Following are all the id types defined earlier: \n\t{','.join(id_types)}"
        )
        shortlisted_id_types = ",".join(list(shortlisted_columns.keys()))
        applicable_id_types_input = self.io.get_user_input(
            f"Enter the comma-separated list of id_types applicable to the `{table}` table: \n",
            options=[shortlisted_id_types],
            default=shortlisted_id_types,
        )
        if applicable_id_types_input.lower() == "back":
            return None, "back"
        applicable_id_types = [
            id_type.strip()
            for id_type in applicable_id_types_input.split(",")
            if id_type.strip() in [id_type_.lower() for id_type_ in id_types]
        ]

        applicable_id_types = [
            it
            for it in id_types
            if it.lower() in [ait.lower() for ait in applicable_id_types]
        ]

        # Assert that all in shortlisted columns are in applicable_id_types
        for id_type in shortlisted_columns:
            if id_type not in applicable_id_types:
                self.io.display_message(
                    f"Please enter all id types applicable to the `{table}` table. The id type `{id_type}` is not found."
                )
                return None, "back"

        if not applicable_id_types:
            self.io.display_message(
                f"No valid id_types selected for `{table}` table. Skipping this table (it won't be part of id stitcher)"
            )
            return [], "next"

        self.io.display_message(
            f"\nNow let's map different id_types in table `{table}` to a column (you can also use SQL string operations on these columns: ex: LOWER(EMAIL_ID), in case you want to use email as an id_type while also treating them as case insensitive):\n"
        )
        table_mappings = []
        for id_type in applicable_id_types:
            while True:
                self.io.display_message(f"\nid type: {id_type}")
                # Suggest columns based on regex matches
                # suggested_cols = shortlisted_columns.get(id_type, [])
                # if suggested_cols:
                #     logger.info("Suggestions based on column names:")
                #     for col in suggested_cols:
                #         sample_data = self.get_sample_data(table, col)
                #         logger.info(f" - {col} (sample data: {sample_data})")
                # if self.fast_mode:
                default = id_type_mapping.get(id_type, id_type)
                # else:
                #     default = None
                user_input = self.io.get_user_input(
                    f"Enter the column(s) to map the id_type '{id_type}' in table `{table}`, or 'skip' to skip:\n",
                    default=default,
                    options=[default],
                )
                if user_input.lower() == "back":
                    return None, "back"
                if user_input.lower() == "skip":
                    self.io.display_message(
                        f"Skipping id_type '{id_type}' for table `{table}`"
                    )
                    break

                selected_columns = [col.strip() for col in user_input.split(",")]
                if not selected_columns:
                    self.io.display_message(
                        "No valid columns selected. Please try again.\n"
                    )
                    continue
                # Display selected columns with sample data for confirmation
                self.io.display_message(f"Selected columns for id_type '{id_type}':")
                for col in selected_columns:
                    sample_data = self.get_sample_data(table, col)
                    self.io.display_message(f"- {col} (sample data: {sample_data})")

                # confirm = self.input_handler.get_user_input("Is this correct? (yes/no): ", options=["yes", "no"])
                # if confirm.lower() == 'yes':
                for col in selected_columns:
                    mapping = {"select": col, "type": id_type, "entity": entity_name}
                    table_mappings.append(mapping)
                break
                #     break
                # else:
                #     logger.info("Let's try mapping again.\n")
        if table_mappings:
            self.io.display_message("Following is the summary of id types selected: \n")
            summary = {"table": table, "ids": table_mappings}
            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.preserve_quotes = True
            yaml.width = 4096  # Prevent line wrapping
            yaml.dump(summary, sys.stdout)
            self.io.display_message("\n")
            self.io.display_message(f"The above is the inputs yaml for table `{table}`")
        else:
            self.io.get_user_input(
                "No id_type mappings were selected for this table.\n"
            )
        return table_mappings, "next"
