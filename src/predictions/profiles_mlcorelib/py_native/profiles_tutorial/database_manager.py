import os
import pandas as pd
import logging
from typing import List, Dict, Tuple
from ruamel.yaml import YAML
import sys

from .input_handler import InputHandler
from .utils import SnowparkConnector

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config: dict, input_handler: InputHandler, fast_mode: bool):
        self.config = config
        logger.info(f"Connecting to snowflake account: {self.config['account']}")
        self.connector = self.connect_to_snowflake()
        self.input_handler = input_handler
        self.fast_mode = fast_mode

    def connect_to_snowflake(self):
        try:
            return SnowparkConnector(
                {
                    "account": self.config["account"],
                    "dbname": self.config["output_database"],
                    "schema": self.config["output_schema"],
                    "type": "snowflake",
                    "user": self.config["user"],
                    "password": self.config["password"],
                    "role": self.config["role"],
                    "warehouse": self.config["warehouse"],
                }
            )
        except Exception as e:
            logger.error(f"Failed to establish connection: {e}")
            raise

    def upload_sample_data(self, sample_data_dir: str, table_suffix: str) -> dict:
        new_table_names = {}
        to_upload = True
        res = self.connector.run_query(
            f"SHOW TABLES IN SCHEMA {self.config['input_database']}.{self.config['input_schema'].upper()}"
        )
        existing_tables = [row[1].lower() for row in res]

        for filename in os.listdir(sample_data_dir):
            if not filename.endswith(".csv"):
                continue

            base_name = os.path.splitext(filename)[0]
            table_name = f"{base_name}_{table_suffix}"
            new_table_names[filename] = table_name

            if not to_upload:
                continue

            if table_name.lower() in existing_tables:
                logger.info(f"Table {table_name} already exists.")
                action = self.input_handler.get_user_input(
                    "Do you want to skip uploading again, so we can reuse the tables? (yes/no) (yes - skips upload, no - uploads again): "
                )
                if action == "yes":
                    logger.info("Skipping upload of all csv files.")
                    to_upload = False
                    continue

                _ = self.connector.run_query(
                    f"DROP TABLE {self.config['input_database']}.{self.config['input_schema']}.{table_name}"
                )

            df = pd.read_csv(os.path.join(sample_data_dir, filename))
            logger.info(
                f"Uploading file {filename} as table {table_name} with {df.shape[0]} rows and {df.shape[1]} columns"
            )
            self.connector.session.write_pandas(
                df,
                table_name,
                database=self.config["input_database"].upper(),
                schema=self.config["input_schema"].upper(),
                auto_create_table=True,
                overwrite=True,
            )
        return new_table_names

    def find_relevant_tables(self, new_table_names: dict) -> List[str]:
        res = self.connector.run_query(
            f"SHOW TABLES IN SCHEMA {self.config['input_database']}.{self.config['input_schema']}"
        )
        tables = [row[1] for row in res]
        relevant_tables = [
            table for table in tables if table in new_table_names.values()
        ]
        return relevant_tables

    def get_columns(self, table: str) -> List[str]:
        try:
            query = f"DESCRIBE TABLE {self.config['input_database']}.{self.config['input_schema']}.{table}"
            result = self.connector.run_query(query, output_type="list")
            columns = [row["name"] for row in result]
        except Exception as e:
            raise Exception(f"Error fetching columns for {table}: {e}")
        return columns

    def get_sample_data(
        self, table: str, column: str, num_samples: int = 5
    ) -> List[str]:
        try:
            query = f"SELECT {column} FROM {self.config['input_database']}.{self.config['input_schema']}.{table} where {column} is not null LIMIT {num_samples}"
            df: pd.DataFrame = self.connector.run_query(query, output_type="pandas")
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
    ) -> Tuple[List[Dict], str]:
        try:
            columns = self.get_columns(table)
        except Exception as e:
            logger.error(f"Error fetching columns for {table}: {e}")
            return None, "back"

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
        logger.info(f"\n{'-'*80}\n")
        logger.info(
            f"The table `{table}` has the following columns, which look like id types:\n"
        )

        # Display shortlisted columns with sample data
        for id_type, cols in shortlisted_columns.items():
            for col in cols:
                sample_data = self.get_sample_data(table, col)
                logger.info(f"id_type: {id_type}")
                logger.info(f"column: {col} (sample data: {sample_data})\n")

        # Display all available id_types
        logger.info(
            f"Following are all the id types defined earlier: \n\t{','.join(id_types)}"
        )
        shortlisted_id_types = ",".join(list(shortlisted_columns.keys()))
        applicable_id_types_input = self.input_handler.get_user_input(
            f"Enter the comma-separated list of id_types applicable to the `{table}` table: \n>",
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
                logger.info(
                    f"Please enter all id types applicable to the `{table}` table. The id type `{id_type}` is not found."
                )
                return None, "back"

        if not applicable_id_types:
            logger.info(
                f"No valid id_types selected for `{table}` table. Skipping this table (it won't be part of id stitcher)"
            )
            return {}, "next"

        logger.info(
            f"\nNow let's map different id_types in table `{table}` to a column (you can also use SQL string operations on these columns: ex: LOWER(EMAIL_ID), in case you want to use email as an id_type while also treating them as case insensitive):\n"
        )
        table_mappings = []
        for id_type in applicable_id_types:
            while True:
                logger.info(f"\nid type: {id_type}")
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
                user_input = self.input_handler.get_user_input(
                    f"Enter the column(s) to map the id_type '{id_type}' in table `{table}`, or 'skip' to skip:\n> ",
                    default=default,
                    options=[default],
                )
                if user_input.lower() == "back":
                    return None, "back"
                if user_input.lower() == "skip":
                    logger.info(f"Skipping id_type '{id_type}' for table `{table}`")
                    break

                selected_columns = [col.strip() for col in user_input.split(",")]
                if not selected_columns:
                    logger.info("No valid columns selected. Please try again.\n")
                    continue
                # Display selected columns with sample data for confirmation
                logger.info(f"Selected columns for id_type '{id_type}':")
                for col in selected_columns:
                    sample_data = self.get_sample_data(table, col)
                    logger.info(f"- {col} (sample data: {sample_data})")

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
            logger.info("Following is the summary of id types selected: \n")
            summary = {"table": table, "ids": table_mappings}
            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.preserve_quotes = True
            yaml.width = 4096  # Prevent line wrapping
            yaml.dump(summary, sys.stdout)
            logger.info("\n")
            self.input_handler.get_user_input(
                f"The above is the inputs yaml for table `{table}`"
            )
        else:
            self.input_handler.get_user_input(
                "No id_type mappings were selected for this table.\n"
            )
        return table_mappings, "next"
