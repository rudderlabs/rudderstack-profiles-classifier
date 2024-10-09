from functools import reduce
import os
import gzip
import json
import uuid
import hashlib
import shutil
import pandas as pd
from datetime import datetime


from typing import Any, Iterable, List, Union, Sequence, Dict

import snowflake.snowpark
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, to_date
from snowflake.snowpark.session import Session

from ..utils import utils
from ..utils import constants
from ..utils.logger import logger
from ..connectors.Connector import Connector
from ..wht.rudderPB import MATERIAL_PREFIX
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend

local_folder = constants.SF_LOCAL_STORAGE_DIR


class SnowflakeConnector(Connector):
    def __init__(self, creds: dict) -> None:
        super().__init__(creds)
        self.data_type_mapping = {
            "numeric": {
                "DecimalType": T.DecimalType(),
                "DoubleType": T.DoubleType(),
                "FloatType": T.FloatType(),
                "IntegerType": T.IntegerType(),
                "LongType": T.LongType(),
                "ShortType": T.ShortType(),
            },
            "categorical": {
                "StringType": T.StringType(),
                "VariantType": T.VariantType(),
            },
            "timestamp": {
                "TimestampType": T.TimestampType(),
                "DateType": T.DateType(),
                "TimeType": T.TimeType(),
            },
            "arraytype": {
                "ArrayType": T.ArrayType(),
            },
            "booleantype": {
                "BooleanType": T.BooleanType(),
            },
        }
        self.dtype_utils_mapping = {
            "numeric": "DecimalType",
            "categorical": "StringType",
        }
        self.run_id = hashlib.md5(
            f"{str(datetime.now())}_{uuid.uuid4()}".encode()
        ).hexdigest()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        train_script_dir = os.path.dirname(current_dir)

        self.stage_name = f"@rs_{self.run_id}"
        self.stored_procedure_name = f"train_and_store_model_results_sf_{self.run_id}"
        self.delete_files = [train_script_dir]
        self.feature_table_name = f"features_{self.run_id}"
        self.udf_name = None
        return

    def build_session(self, credentials: dict) -> snowflake.snowpark.Session:
        self.schema = credentials.get("schema", None)
        self.connection_parameters = self.remap_credentials(credentials)
        if "privateKey" in credentials:
            private_key = load_pem_private_key(
                credentials["privateKey"].encode(),
                password=(
                    credentials["privateKeyPassphrase"].encode()
                    if credentials.get("privateKeyPassphrase")
                    else None
                ),
                backend=default_backend(),
            )
            self.connection_parameters["private_key"] = private_key
        session = Session.builder.configs(self.connection_parameters).create()
        # Removing the private key to prevent serialisation error in the snowflake stored procedure
        _ = self.connection_parameters.pop("private_key", None)
        return session

    def join_file_path(self, file_name: str) -> str:
        """Joins the given file name to the local temp folder path."""
        return os.path.join(local_folder, file_name)

    def run_query(self, query: str, **args) -> List:
        """Runs the given query on the snowpark session and returns a List with Named indices."""
        return self.session.sql(query).collect()

    def call_procedure(self, *args, **kwargs):
        """Calls the given procedure on the snowpark session and returns the results of the procedure call."""
        args = list(args)
        args.insert(2, self.feature_table_name)
        feature_table_df = args.pop(
            1
        )  # Snowflake stored procedure for training requires feature_table_name saved on warehouse instead of feature_table_df
        del feature_table_df

        return self.session.call(*args)

    def write_joined_input_table(self, query, table_name):
        create_temp_table_query = f"""
                                    CREATE OR REPLACE TABLE {self.schema}.{table_name} AS
                                    {query} ;
                                """

        self.run_query(create_temp_table_query, response=False)

    def get_entity_var_table_ref(self, table_name: str) -> str:
        return f'"{table_name.upper()}"'

    def get_merged_table(self, base_table, incoming_table):
        return (
            incoming_table
            if base_table is None
            else base_table.unionAllByName(incoming_table)
        )

    def fetch_processor_mode(
        self, user_preference_order_infra: List[str], is_rudder_backend: bool
    ) -> str:
        return constants.WAREHOUSE_MODE

    def compute_udf_name(self, model_path: str) -> None:
        with open(model_path, "r") as f:
            results = json.load(f)
        stage_name = results["model_info"]["file_location"]["stage"]
        self.udf_name = f"prediction_score_{stage_name.replace('@','')}"

    def is_valid_table(self, table_name: str) -> bool:
        try:
            self.session.sql(f"select * from {table_name} limit 1").collect()
            return True
        except:
            return False

    def check_table_entry_in_material_registry(
        self,
        registry_table_name: str,
        material: dict,
    ) -> bool:
        """
        Checks wether an entry is there in the material registry for the given
        material table name and wether its sucessfully materialised or not as well.
        Right now, we consider tables as materialised if the metadata status is 2.
        """
        material_registry_table = self.get_table(registry_table_name)
        num_rows = (
            material_registry_table.withColumn(
                "status", F.get_path("metadata", F.lit("complete.status"))
            )
            .filter(F.col("status") == 2)
            .filter(F.lower(col("model_name")) == material["model_name"].lower())
            .filter(F.lower(col("model_hash")) == material["model_hash"].lower())
            .filter(col("seq_no") == material["seq_no"])
            .count()
        )

        return num_rows != 0

    def get_table(self, table_name: str, **kwargs) -> snowflake.snowpark.Table:
        filter_condition = kwargs.get("filter_condition", None)
        if not self.is_valid_table(table_name):
            raise Exception(f"Table {table_name} does not exist or not authorized")
        table = self.session.table(table_name)
        if filter_condition and filter_condition != "*":
            table = self.filter_table(table, filter_condition)
        return table

    def get_table_as_dataframe(
        self, session: snowflake.snowpark.Session, table_name: str, **kwargs
    ) -> pd.DataFrame:
        # Duplicating "self.get_table()" function code here.
        # This is because "get_table()"" uses "self.session" which is not available in case of Snowpark
        # and I prefer duplicating 3 lines of code over changing the signature of multiple functions
        try:
            session.sql(f"select * from {table_name} limit 1").collect()
        except:
            raise Exception(f"Table {table_name} does not exist or not authorized")
        return session.table(table_name).toPandas()

    def send_table_to_train_env(self, table: snowflake.snowpark.Table, **kwargs) -> Any:
        """Sends the given snowpark table to the training env(ie. snowflake warehouse in this case) with the name as given"""
        self.write_table(table, self.feature_table_name, **kwargs)

    def write_table(
        self, table: snowflake.snowpark.Table, table_name_remote: str, **kwargs
    ) -> None:
        write_mode = kwargs.get("write_mode", "append")
        table.write.mode(write_mode).save_as_table(table_name_remote)

    def write_pandas(self, df: pd.DataFrame, table_name: str, **kwargs):
        """Writes the given pandas dataframe to the snowpark session with the given name

        Args:
            df (pd.DataFrame): Pandas dataframe to be written to the snowpark session
            table_name (str): Name with which the dataframe is to be written to the snowpark session
            From kwargs:
            - session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            - auto_create_table (bool): Flag to indicate whether to create the table if it does not exist
            - overwrite (bool): Flag to indicate whether to overwrite the table if it already exists in the snowpark session
        """
        session = kwargs.get("session", None)
        if session is None:
            raise Exception("Session object not found")
        auto_create_table = kwargs.get("auto_create_table", True)
        overwrite = kwargs.get("overwrite", False)
        session.write_pandas(
            df,
            table_name=table_name,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
        )

    def label_table(
        self,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ) -> snowflake.snowpark.Table:
        """Labels the given label_columns in the table as '1' or '0' if the value matches the label_value or not respectively."""
        if label_value is None:
            table = self.get_table(label_table_name).select(entity_column, label_column)
        else:
            table = (
                self.get_table(label_table_name)
                .withColumn(
                    label_column,
                    F.when(F.col(label_column) == label_value, F.lit(1)).otherwise(
                        F.lit(0)
                    ),
                )
                .select(entity_column, label_column)
            )
        return table

    def save_file(
        self,
        session: snowflake.snowpark.Session,
        file_name: str,
        stage_name: str,
        overwrite: bool,
    ):
        session.file.put(file_name, stage_name, overwrite=overwrite)

    def fetch_table_metadata(self, table_name: str) -> List:
        """Fetches a list containing the schema of the given table from the Snowflake schema."""
        table = self.get_table(table_name)
        return table.schema.fields

    def _field_datatype_to_string(self, datatype: object) -> str:
        """Converts field datatype to string by extracting from the first character till 'Type'"""
        datatype_str = str(datatype)
        index_of_type = datatype_str.find("Type")
        return datatype_str[: index_of_type + len("Type")]

    def _datatype_is_instance_of(self, datatype, data_type_str: str) -> bool:
        """Check if datatype is an instance of the class represented by data_type_str."""
        class_obj = getattr(T, data_type_str)
        return isinstance(datatype, class_obj)

    def fetch_given_data_type_columns(
        self,
        schema_fields: List,
        required_data_types: Dict[str, object],
        label_column: str,
        entity_column: str,
    ) -> Dict:
        """Fetches the column names from the given schema_fields based on the required data types (exclude label and entity columns)"""
        return {
            field.name: self._field_datatype_to_string(field.datatype)
            for field in schema_fields
            if any(
                self._datatype_is_instance_of(field.datatype, data_type_str)
                for data_type_str in required_data_types.keys()
            )
            and field.name.lower() not in (label_column.lower(), entity_column.lower())
        }

    def get_numeric_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["numeric"],
            label_column,
            entity_column,
        )

    def get_stringtype_features(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["categorical"],
            label_column,
            entity_column,
        )

    def get_arraytype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["arraytype"],
            label_column,
            entity_column,
        )

    def get_high_cardinal_features(
        self,
        feature_table: snowflake.snowpark.Table,
        categorical_columns: List[str],
        label_column: str,
        entity_column: str,
        cardinal_feature_threshold: float,
    ) -> List[str]:
        """
        Identify high cardinality features in the feature table based on condition that
        the sum of frequency of ten most popular categories is less than cardinal_feature_threshold fraction(0.01) of the total row count.
        """
        high_cardinal_features = list()
        lower_categorical_features = [col.lower() for col in categorical_columns]
        total_rows = feature_table.count()
        for field in feature_table.schema.fields:
            top_10_freq_sum = 0
            if (
                field.name.lower() in lower_categorical_features
                and field.name.lower()
                not in (
                    label_column.lower(),
                    entity_column.lower(),
                )
            ):
                feature_data = (
                    feature_table.filter(F.col(field.name) != "")
                    .group_by(F.col(field.name))
                    .count()
                    .sort(F.col("count").desc())
                    .first(10)
                )
                for row in feature_data:
                    top_10_freq_sum += row.COUNT
                if top_10_freq_sum < (cardinal_feature_threshold * total_rows):
                    high_cardinal_features.append(field.name)
        return high_cardinal_features

    def get_timestamp_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["timestamp"],
            label_column,
            entity_column,
        )

    def get_booleantype_columns(
        self,
        schema_fields: List,
        label_column: str,
        entity_column: str,
    ) -> List[str]:
        return self.fetch_given_data_type_columns(
            schema_fields,
            self.data_type_mapping["booleantype"],
            label_column,
            entity_column,
        )

    def generate_pivot_df(
        self,
        feature_table,
        array_column,
        top_k_array_categories,
        predict_arraytype_features,
        group_by_cols,
    ):
        # Explode the array and group by columns
        exploded_df = feature_table.select(
            *group_by_cols, F.explode(array_column).alias("ARRAY_VALUE")
        )
        grouped_df = exploded_df.groupBy(*exploded_df.columns).count()

        # Sum up the counts for each unique value
        total_counts = grouped_df.groupBy("ARRAY_VALUE").agg(
            F.sum("COUNT").alias("TOTAL_COUNT")
        )

        # Extract unique values and their respective total counts
        unique_values = [
            row["ARRAY_VALUE"].strip('"') for row in total_counts.collect()
        ]

        frequencies = {
            row["ARRAY_VALUE"].strip('"'): row["TOTAL_COUNT"]
            for row in total_counts.collect()
        }

        # Sort unique values based on frequency and select the top_k_col values
        sorted_values = sorted(
            unique_values, key=lambda x: frequencies[x], reverse=True
        )
        top_values = sorted_values[:top_k_array_categories]
        other_values = sorted_values[top_k_array_categories:]

        predict_top_values = predict_arraytype_features.get(array_column, [])
        if predict_top_values:
            top_values = [
                item[len(array_column) :].strip("_").lower()
                for item in predict_top_values
                if "OTHERS" not in item
            ]
            unique_values = list(set(unique_values) | set(top_values))
            other_values = [val for val in unique_values if val not in top_values]

        # Define columns to remove
        columns_to_remove = ["COUNT", "ARRAY_VALUE"]
        grouped_df_cols = [
            col for col in grouped_df.columns if col not in columns_to_remove
        ]

        # Pivot the DataFrame to create new columns for each unique value
        pivoted_df = (
            grouped_df.groupBy(grouped_df_cols)
            .pivot("ARRAY_VALUE", unique_values)
            .sum("COUNT")
            .na.fill(0)
        )

        for value in top_values:
            if value not in [col.strip("\"'") for col in pivoted_df.columns]:
                pivoted_df = pivoted_df.withColumn(value, F.lit(0))

        return pivoted_df, unique_values, top_values, other_values

    def rename_joined_df(
        self,
        array_column,
        top_values,
        other_values,
        joined_df,
        transformed_column_names,
        other_column_name,
    ):
        joined_df = joined_df.withColumn(
            other_column_name,
            (
                sum(joined_df[f"'{col}'"] for col in other_values)
                if len(other_values) != 0
                else F.lit(0)
            ),
        )
        transformed_column_names.append(other_column_name)

        # Clean up the column names by stripping quotes and remove columns that are in other_values
        cleaned_columns = set(
            col.strip("\"'")
            for col in joined_df.columns
            if col.strip("\"'") not in other_values
        )
        required_cols = list(
            cleaned_columns.union([other_column_name]).union(top_values)
        )

        # Filter DataFrame to only include columns that match top_values
        filtered_df = joined_df.select(
            *[col for col in joined_df.columns if col.strip("\"'") in required_cols]
        )

        # Generate new column names for the top values
        new_array_column_names = [
            f"{array_column}_{value}".upper().strip() for value in top_values
        ]

        # Rename columns
        for old_name, new_name in zip(top_values, new_array_column_names):
            transformed_column_names.append(new_name)
            filtered_df = filtered_df.withColumnRenamed(f"'{old_name}'", new_name)

        return filtered_df, transformed_column_names

    def transform_arraytype_features(
        self,
        feature_table: snowflake.snowpark.Table,
        arraytype_features: List[str],
        top_k_array_categories,
        **kwargs,
    ) -> Union[List[str], snowflake.snowpark.Table]:
        """Transforms arraytype features in a snowflake.snowpark.Table by expanding the arraytype features
        as {feature_name}_{unique_value} columns and perform numeric encoding based on their count in those cols.
        """

        predict_arraytype_features = kwargs.get("predict_arraytype_features", {})

        # Initialize lists to store transformed column names and DataFrames
        transformed_column_names = []
        transformed_tables = []

        # Initialize a variable to store the original feature table
        transformed_feature_table = feature_table

        # Identify columns to group by
        group_by_cols = [
            col for col in feature_table.columns if col not in arraytype_features
        ]

        # Loop through each array type feature
        for array_column in arraytype_features:
            # TODO : Find a fix to convert all the arraytype values to lower case
            # feature_table = feature_table.withColumn(
            #     array_column, F.expr(f"transform({array_column}, x -> lower(x))")
            # )

            other_column_name = f"{array_column}_OTHERS".upper()

            # Identify rows with empty or null arrays
            empty_array_rows = feature_table.filter(F.col(array_column) == [])
            null_array_value_rows = feature_table.filter(F.col(array_column).isNull())
            merged_empty_rows = empty_array_rows.join(
                null_array_value_rows, on=group_by_cols, how="full"
            ).select(*group_by_cols)

            # Skip to the next array type feature if all rows have empty or null arrays
            if merged_empty_rows.count() == feature_table.count():
                intermediate_transformed_table = feature_table
                if predict_arraytype_features.get(array_column):
                    (
                        pivoted_df,
                        unique_values,
                        top_values,
                        other_values,
                    ) = self.generate_pivot_df(
                        feature_table,
                        array_column,
                        top_k_array_categories,
                        predict_arraytype_features,
                        group_by_cols,
                    )

                    # Rename columns with top values
                    (
                        intermediate_transformed_table,
                        transformed_column_names,
                    ) = self.rename_joined_df(
                        array_column,
                        top_values,
                        other_values,
                        pivoted_df,
                        transformed_column_names,
                        other_column_name,
                    )

                intermediate_transformed_table = (
                    intermediate_transformed_table.withColumn(
                        other_column_name,
                        F.lit(0),
                    )
                )

                transformed_column_names.append(other_column_name)
                transformed_tables.append(intermediate_transformed_table)

                continue

            # Get the unique values, top values, and other values
            (
                pivoted_df,
                unique_values,
                top_values,
                other_values,
            ) = self.generate_pivot_df(
                feature_table,
                array_column,
                top_k_array_categories,
                predict_arraytype_features,
                group_by_cols,
            )

            # Join with rows having empty or null arrays, and fill NaN values with 0
            joined_df = pivoted_df.join(
                merged_empty_rows, on=group_by_cols, how="full"
            ).fillna(0)
            joined_df = self.drop_cols(joined_df, arraytype_features)

            # Rename columns with top values
            joined_df, transformed_column_names = self.rename_joined_df(
                array_column,
                top_values,
                other_values,
                joined_df,
                transformed_column_names,
                other_column_name,
            )

            # Append the transformed DataFrame to the list
            transformed_tables.append(joined_df)

        # If there are transformed DataFrames, join them together
        if transformed_tables:
            transformed_feature_table = reduce(
                lambda df1, df2: df1.join(df2, on=group_by_cols, how="left").fillna(0),
                transformed_tables,
            )

        # Drop the original array type features from the transformed table
        transformed_feature_table = self.drop_cols(
            transformed_feature_table, arraytype_features
        )

        return transformed_column_names, transformed_feature_table

    def transform_booleantype_features(
        self, feature_table: snowflake.snowpark.Table, booleantype_features: List[str]
    ) -> snowflake.snowpark.Table:
        """Transforms booleantype features in a snowflake.snowpark.Table"""

        # Initialize a variable to store the original feature table
        transformed_feature_table = feature_table

        for boolean_column in booleantype_features:
            transformed_feature_table = transformed_feature_table.withColumn(
                boolean_column, F.col(boolean_column).cast("integer")
            )
        return transformed_feature_table

    def get_default_label_value(
        self, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        label_value = list()
        table = self.get_table(table_name)
        distinct_labels = (
            table.select(F.col(label_column).alias("distinct_labels"))
            .distinct()
            .collect()
        )

        if len(distinct_labels) != 2:
            raise Exception("The feature to be predicted should be boolean")
        for row in distinct_labels:
            if row.DISTINCT_LABELS in positive_boolean_flags:
                label_value.append(row.DISTINCT_LABELS)

        if len(label_value) == 0:
            raise Exception(
                f"Label column {label_column} doesn't have any positive flags. Please provide custom label_value from label_column to bypass the error."
            )
        elif len(label_value) > 1:
            raise Exception(
                f"Label column {label_column} has multiple positive flags. Please provide custom label_value out of {label_value} to bypass the error."
            )
        return label_value[0]

    def fetch_filtered_table(
        self,
        df: snowflake.snowpark.Table,
        model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        columns: List[str],
    ) -> snowflake.snowpark.Table:
        filtered_snowpark_df = (
            df.filter(F.lower(col("model_name")) == model_name.lower())
            .filter(F.lower(col("model_hash")) == model_hash.lower())
            .filter(
                (to_date(col("end_ts")) >= start_time)
                & (to_date(col("end_ts")) <= end_time)
            )
            .select(columns)
        ).distinct()
        return filtered_snowpark_df

    def join_feature_label_tables(
        self,
        registry_table_name: str,
        model_name: str,
        model_hash: str,
        start_time: str,
        end_time: str,
        prediction_horizon_days: int,
    ) -> Iterable:
        snowpark_df = self.get_material_registry_table(registry_table_name)
        feature_snowpark_df = self.fetch_filtered_table(
            snowpark_df,
            model_name,
            model_hash,
            start_time,
            end_time,
            columns=["seq_no", "end_ts"],
        )
        label_snowpark_df = self.fetch_filtered_table(
            snowpark_df,
            model_name,
            model_hash,
            utils.date_add(start_time, prediction_horizon_days),
            utils.date_add(end_time, prediction_horizon_days),
            columns=["seq_no", "end_ts"],
        )

        return (
            feature_snowpark_df.join(
                label_snowpark_df,
                F.datediff("day", feature_snowpark_df.end_ts, label_snowpark_df.end_ts)
                == prediction_horizon_days,
                join_type="full",
            )
            .select(
                feature_snowpark_df.seq_no.alias("feature_seq_no"),
                feature_snowpark_df.end_ts.alias("feature_end_ts"),
                label_snowpark_df.seq_no.alias("label_seq_no"),
                label_snowpark_df.end_ts.alias("label_end_ts"),
            )
            .collect()
        )

    def get_old_prediction_table(
        self,
        lookahead_days: int,
        current_date: str,
        model_name: str,
        material_registry: str,
    ):
        past_predictions_end_date = utils.date_add(current_date, -lookahead_days)
        registry_df = self.get_material_registry_table(material_registry)

        try:
            past_predictions_info = (
                registry_df.filter(col("model_name") == model_name)
                .filter(col("model_type") == "python_model")
                .filter(to_date(col("end_ts")) == past_predictions_end_date)
                .sort(F.col("creation_ts").desc())
                .collect()[0]
            )
        except IndexError:
            raise Exception(
                f"No past predictions found for model {model_name} before {past_predictions_end_date}"
            )

        predictions_table_name = (
            f"{MATERIAL_PREFIX}{model_name}"
            + f"_{past_predictions_info.MODEL_HASH}"
            + f"_{past_predictions_info.SEQ_NO}"
        )
        return predictions_table_name

    def get_previous_predictions_info(
        self, prev_pred_ground_truth_table, score_column, label_column
    ):
        single_row = prev_pred_ground_truth_table.limit(1).collect()[0]
        model_id = single_row.MODEL_ID
        valid_at = single_row.VALID_AT
        score_and_ground_truth_df = prev_pred_ground_truth_table.select(
            score_column, label_column
        ).toPandas()
        return score_and_ground_truth_df, model_id, valid_at

    def get_tables_by_prefix(self, prefix: str):
        tables = list()
        registry_df = self.run_query(f"show tables starts with '{prefix}'")
        for row in registry_df:
            tables.append(row.name)
        return tables

    def get_creation_ts(
        self,
        material_table: str,
        model_hash: str,
        entity_key: str,
    ):
        """Retrieves the latest creation timestamp for a specific model hash, and entity key."""
        snowpark_df = self.get_material_registry_table(material_table)
        try:
            temp_hash_vector = (
                snowpark_df.filter(F.lower(col("model_hash")) == model_hash.lower())
                .filter(F.lower(col("entity_key")) == entity_key.lower())
                .sort(col("creation_ts"), ascending=False)
                .select(col("creation_ts"))
                .collect()[0]
            )

            creation_ts = temp_hash_vector.CREATION_TS

        except:
            raise Exception(
                f"Project is never materialzied with model hash {model_hash}."
            )
        return creation_ts

    def get_latest_seq_no_from_registry(
        self, material_table: str, model_hash: str, model_name: str
    ) -> int:
        snowpark_df = self.get_material_registry_table(material_table)
        try:
            temp_hash_vector = (
                snowpark_df.filter(F.lower(col("model_hash")) == model_hash.lower())
                .filter(F.lower(col("model_name")) == model_name.lower())
                .sort(col("creation_ts"), ascending=False)
                .select(col("seq_no"))
                .collect()[0]
            )
            seq_no = temp_hash_vector.SEQ_NO
        except:
            raise Exception(
                f"Error occured while fetching latest seq_no from registry table. Project is never materialzied with model hash {model_hash}."
            )
        return int(seq_no)

    def get_model_hash_from_registry(
        self, material_table, model_name: str, seq_no: int
    ) -> str:
        material_registry_df = self.get_material_registry_table(material_table)

        try:
            feature_table_info = (
                material_registry_df.filter(col("model_name") == model_name)
                .filter(col("seq_no") == seq_no)
                .select("model_hash")
                .collect()[0]
            )

            model_hash = feature_table_info.MODEL_HASH
        except:
            raise Exception(
                f"Error occurred while fetching model hash from registry table. \
                    Project is never materialzied with model name {model_name} and seq no {seq_no}."
            )

        return model_hash

    def add_index_timestamp_colum_for_predict_data(
        self, predict_data, index_timestamp: str, end_ts: str
    ) -> snowflake.snowpark.Table:
        predict_data = predict_data.withColumn(
            index_timestamp, F.to_timestamp(F.lit(end_ts))
        )
        return predict_data

    def fetch_staged_file(
        self,
        stage_name: str,
        file_name: str,
        target_folder: str,
    ) -> None:
        """Fetches a file from a Snowflake stage and saves it to a local target folder."""
        file_stage_path = f"{stage_name}/{file_name}"
        self.get_file(file_stage_path, target_folder)
        input_file_path = os.path.join(target_folder, f"{file_name}.gz")
        output_file_path = os.path.join(target_folder, file_name)

        with gzip.open(input_file_path, "rb") as gz_file:
            with open(output_file_path, "wb") as target_file:
                shutil.copyfileobj(gz_file, target_file)
        os.remove(input_file_path)

    def filter_table(
        self, table: snowflake.snowpark.Table, filter_condition: str
    ) -> snowflake.snowpark.Table:
        return table.filter(filter_condition)

    def drop_cols(
        self, table: snowflake.snowpark.Table, col_list: list
    ) -> snowflake.snowpark.Table:
        ignore_features_upper = [col.upper() for col in col_list]
        ignore_features_lower = [col.lower() for col in col_list]
        ignore_features_ = [
            col
            for col in table.columns
            if col in ignore_features_upper or col in ignore_features_lower
        ]
        return table.drop(ignore_features_)

    def filter_feature_table(
        self,
        feature_table: snowflake.snowpark.Table,
        max_row_count: int,
    ) -> snowflake.snowpark.Table:
        if feature_table.count() <= max_row_count:
            return feature_table
        else:
            return feature_table.sample(n=int(max_row_count))

    def check_for_classification_data_requirement(
        self,
        materials: List[constants.TrainTablesInfo],
        label_column: str,
        label_value: str,
        entity_column: str,
        filter_condition: str = None,
    ) -> bool:
        final_feature_table = None

        for m in materials:
            feature_table = self.get_table(
                m.feature_table_name, filter_condition=filter_condition
            )

            label_table = self.get_table(
                m.label_table_name, filter_condition=filter_condition
            )

            temp_table = self.join_feature_table_label_table(
                feature_table.select(entity_column), label_table, entity_column, "inner"
            )
            final_feature_table = self.get_merged_table(final_feature_table, temp_table)

        total_samples = final_feature_table.count()

        total_negative_samples = final_feature_table.filter(
            F.col(label_column) != label_value
        ).count()

        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES
        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        min_negative_label_count = min_label_proportion * total_samples

        if (
            total_samples < min_no_of_samples
            or total_negative_samples < min_negative_label_count
        ):
            logger.get().debug(
                "Total number of samples or number of negative samples are "
                "not meeting the minimum training requirement, "
                f"total samples - {total_samples}, minimum samples required - {min_no_of_samples}, "
                f"total negative samples - {total_negative_samples}, "
                f"minimum negative samples portion required - {min_label_proportion}"
            )
            return False
        return True

    def check_for_regression_data_requirement(
        self,
        materials: List[constants.TrainTablesInfo],
        filter_condition: str = None,
    ) -> bool:
        total_samples = 0
        for m in materials:
            feature_table = self.get_table(
                m.feature_table_name, filter_condition=filter_condition
            )

            total_samples += feature_table.count()

        min_no_of_samples = constants.MIN_NUM_OF_SAMPLES

        if total_samples < min_no_of_samples:
            logger.get().debug(
                "Number training samples are not meeting the minimum requirement, "
                f"total samples - {total_samples}, minimum samples required - {min_no_of_samples}"
            )
            return False

        return True

    def validate_columns_are_present(
        self, feature_table: snowflake.snowpark.Table, label_column: str
    ) -> bool:
        if label_column.upper() not in feature_table.columns:
            raise Exception(
                f"Label column {label_column} is not present in the feature table."
            )
        if len(feature_table.columns) < 3:
            raise Exception(
                f"Feature table must have at least one column apart from the label column {label_column} and entity_column."
            )
        return True

    def validate_row_count(
        self,
        feature_table: snowflake.snowpark.Table,
        min_sample_for_training: int,
        train_table_pairs,
    ) -> bool:
        row_count = feature_table.count()

        if row_count < min_sample_for_training:
            self.write_table(
                feature_table, self.feature_table_name, write_mode="overwrite"
            )

            log_message = (
                "Following are the table pairs used for creating the training data:\n"
            )
            log_message += " Feature table name, label table name:\n"
            log_message += "\n".join(
                f" {pair.feature_table_name}, {pair.label_table_name}"
                for pair in train_table_pairs
            )
            log_message += (
                f"\nThe table {self.feature_table_name} is built by joining the pairs using the entity-id, "
                "concatenating them, and applying eligible users flag. You can try different eligible users conditions to rerun the model to solve the data validation errors."
            )

            raise Exception(
                f"Insufficient data for training. Only {row_count} user records found, "
                f"while a minimum of {min_sample_for_training} user records is required.\n"
                f"For further information, you can check the table in the warehouse with the name: {self.feature_table_name}.\n"
                f"{log_message}"
            )

        return True

    def validate_class_proportions(
        self,
        feature_table: snowflake.snowpark.Table,
        label_column: str,
        train_table_pairs,
    ) -> bool:
        distinct_values_count = feature_table.groupBy(label_column).count()
        total_count = int(feature_table.count())
        result_table = distinct_values_count.withColumn(
            "NORMALIZED_COUNT", F.col("count") / total_count
        ).collect()

        min_label_proportion = constants.CLASSIFIER_MIN_LABEL_PROPORTION
        max_label_proportion = constants.CLASSIFIER_MAX_LABEL_PROPORTION

        no_invalid_rows = [
            row
            for row in result_table
            if row["NORMALIZED_COUNT"] < min_label_proportion
            or row["NORMALIZED_COUNT"] > max_label_proportion
        ]

        if len(no_invalid_rows) > 0:
            self.write_table(
                feature_table, self.feature_table_name, write_mode="overwrite"
            )
            error_msg = ""
            for row in result_table:
                error_msg += f"\t{row[label_column.upper()]} - user count:  {row['COUNT']} ({100 * row['NORMALIZED_COUNT']:.2f}%)\n"

            log_message = (
                "Following are the table pairs used for creating the training data:\n"
            )
            log_message += " Feature table name, label table name:\n"
            log_message += "\n".join(
                f" {pair.feature_table_name}, {pair.label_table_name}"
                for pair in train_table_pairs
            )
            log_message += (
                f"\nThe table {self.feature_table_name} is built by joining the pairs using the entity-id, "
                "concatenating them, and applying eligible users flag. You can try different eligible users conditions to rerun the model to solve the data validation errors."
            )

            raise Exception(
                f"Label column {label_column} exhibits significant class imbalance.\n"
                f"The model cannot be trained on such a highly imbalanced dataset.\n"
                f"You can select a subset of users where the class imbalance is not as severe, such as by excluding inactive users, etc.\n"
                f"Current class proportions are as follows:\n{error_msg}"
                f"You can look for the table {self.feature_table_name} in your warehouse where the eligible users data is stored, and this imbalance is found. You can try different combinations of eligible users to see how the imbalance changes."
                f"{log_message}"
            )

        return True

    def validate_label_distinct_values(
        self,
        feature_table: snowflake.snowpark.Table,
        label_column: str,
        train_table_pairs,
    ) -> bool:
        distinct_values_count = feature_table.groupBy(label_column).count()
        num_distinct_values = distinct_values_count.count()
        req_distinct_values = int(constants.REGRESSOR_MIN_LABEL_DISTINCT_VALUES)

        if num_distinct_values < req_distinct_values:
            self.write_table(
                feature_table, self.feature_table_name, write_mode="overwrite"
            )

            log_message = (
                "Following are the table pairs used for creating the training data:\n"
            )
            log_message += " Feature table name, label table name:\n"
            log_message += "\n".join(
                f" {pair.feature_table_name}, {pair.label_table_name}"
                for pair in train_table_pairs
            )
            log_message += (
                f"\nThe table {self.feature_table_name} is built by joining the pairs using the entity-id, "
                "concatenating them, and applying eligible users flag. You can try different eligible users conditions to rerun the model to solve the data validation errors."
            )

            raise Exception(
                f"Label column {label_column} has {num_distinct_values} distinct values while we expect a minimum of {req_distinct_values} values for a regression problem.\n"
                f"Please check your label column and consider modifying the task in your Python model to 'classification' if that's a better fit.\n"
                f"You can look for the table {self.feature_table_name} in your warehouse where the eligible users data is stored, for the distinct label count. You can try different combinations of eligible users to see how the label counts change."
                f"{log_message}"
            )

        return True

    def add_days_diff(
        self, table: snowflake.snowpark.Table, new_col, time_col, end_ts
    ) -> snowflake.snowpark.Table:
        """Adds a new column to the given table containing the difference in days between the given timestamp columns."""
        return table.withColumn(
            new_col, F.datediff("day", F.col(time_col), F.to_timestamp(F.lit(end_ts)))
        )

    def join_feature_table_label_table(
        self,
        feature_table: snowflake.snowpark.Table,
        label_table: snowflake.snowpark.Table,
        entity_column: str,
        join_type: str = "inner",
    ) -> snowflake.snowpark.Table:
        """Joins the given feature table and label table based on the given entity column."""
        return feature_table.join(label_table, [entity_column], join_type=join_type)

    def get_distinct_values_in_column(
        self, table: snowflake.snowpark.Table, column_name: str
    ) -> List:
        return table.select(column_name).distinct().collect()

    def get_material_registry_table(
        self, material_registry_table_name: str
    ) -> snowflake.snowpark.Table:
        """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
        Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.
        """
        material_registry_table = (
            self.get_table(material_registry_table_name)
            .sort(F.col("creation_ts").desc())
            .withColumn("status", F.get_path("metadata", F.lit("complete.status")))
            .filter(F.col("status") == 2)
        )
        return material_registry_table

    def generate_type_hint(
        self,
        df: snowflake.snowpark.Table,
    ):
        types = []
        schema_fields = df.schema.fields
        for field in schema_fields:
            types.append(field.datatype)
        return types

    def call_prediction_udf(
        self,
        predict_data: snowflake.snowpark.Table,
        prediction_udf: Any,
        entity_column: str,
        index_timestamp: str,
        score_column_name: str,
        percentile_column_name: str,
        output_label_column: str,
        train_model_id: str,
        input: snowflake.snowpark.Table,
        pred_output_df_columns: Dict,
    ) -> pd.DataFrame:
        """Calls the given function for prediction and returns results of the predict function."""

        pycaret_score_column_name = pred_output_df_columns["score"].upper()
        pycaret_label_column_name = pred_output_df_columns.get(
            "label", "prediction_score"
        ).upper()

        preds = predict_data.select(
            entity_column, index_timestamp, F.lit(train_model_id).alias("model_id")
        )

        # Apply prediction_udf and select columns
        prediction_df = input.select(
            prediction_udf(*input.columns).over(partition_by=[input.columns[0]])
        )
        extracted_df = prediction_df.select(
            F.col(pycaret_score_column_name).alias(score_column_name),
            F.col(pycaret_label_column_name).alias(output_label_column),
        )

        # Join dfs using monotonically_increasing_id
        w = Window.orderBy(F.monotonically_increasing_id())
        preds = preds.withColumn("columnindex", F.row_number().over(w))
        extracted_df = extracted_df.withColumn("columnindex", F.row_number().over(w))
        preds = preds.join(
            extracted_df,
            preds.columnindex == extracted_df.columnindex,
            "inner",
            lsuffix="_left",
            rsuffix="_right",
        ).drop("columnindex_left", "columnindex_right")

        # Remove the dummy label column in case of Regression
        if "label" not in pred_output_df_columns:
            preds.drop(output_label_column)

        preds_with_percentile = preds.withColumn(
            percentile_column_name,
            (F.percent_rank().over(Window.orderBy(F.col(score_column_name)))) * 100,
        )

        return preds_with_percentile

    def create_stage(self):
        self.run_query(
            f"create stage if not exists {self.stage_name.replace('@', '')}",
        )

    def _delete_import_files(
        self,
        stage_name: str,
        import_paths: List[str],
    ) -> None:
        all_stages = self.run_query(f"show stages like '{stage_name.replace('@', '')}'")
        if len(all_stages) == 0:
            logger.get().info(f"Stage {stage_name} does not exist. No files to delete.")
            return

        import_files = [element.split("/")[-1] for element in import_paths]
        files = self.run_query(f"list {stage_name}")
        for row in files:
            if any(substring in row.name for substring in import_files):
                self.run_query(f"remove @{row.name}")

    def _delete_procedures(self, procedure_name: str) -> None:
        procedures = self.run_query(f"show procedures like '{procedure_name}'")
        for row in procedures:
            try:
                words = row.arguments.split(" ")[:-2]
                procedure_arguments = " ".join(words)
                self.run_query(f"drop procedure if exists {procedure_arguments}")
            except Exception as e:
                raise Exception(f"Error while dropping procedure {e}")

    def _drop_fn_if_exists(self, fn_name: str) -> bool:
        """Snowflake caches the functions and it reuses these next time. To avoid the caching,
        we manually search for the same function name and drop it before we create the udf.
        """
        fn_list = self.session.sql(f"show user functions like '{fn_name}'").collect()
        if len(fn_list) == 0:
            logger.get().info(f"Function {fn_name} does not exist")
            return True
        else:
            logger.get().info(
                "Function name match found. Dropping all functions with the same name"
            )
            for fn in fn_list:
                fn_signature = fn["arguments"].split("RETURN")[0]
                drop = self.session.sql(f"DROP FUNCTION IF EXISTS {fn_signature}")
                logger.get().info(drop.collect()[0].status)
            logger.get().info("All functions with the same name dropped")
            return True

    def get_file(
        self,
        file_stage_path: str,
        target_folder: str,
    ):
        _ = self.session.file.get(file_stage_path, target_folder)

    def select_relevant_columns(
        self,
        table: snowflake.snowpark.Table,
        training_features_columns: Sequence[str],
    ) -> snowflake.snowpark.Table:
        uppercase_list = lambda features: [feature.upper() for feature in features]
        training_features_columns_upper_case = uppercase_list(training_features_columns)

        table_cols = [col.upper() for col in table.columns]
        for col in training_features_columns_upper_case:
            if col not in table_cols:
                raise Exception(
                    f"Expected feature column {col} not found in the predictions input table"
                )
        shortlisted_columns = []
        shortlisted_columns = [
            col for col in table_cols if col in training_features_columns_upper_case
        ]
        return table.select(*shortlisted_columns)

    def _job_cleanup(self):
        if self.stored_procedure_name:
            self._delete_procedures(self.stored_procedure_name)
        if self.udf_name:
            self._drop_fn_if_exists(self.udf_name)
        if self.delete_files:
            self._delete_import_files(self.stage_name, self.delete_files)

    def pre_job_cleanup(self):
        self._job_cleanup()

    def post_job_cleanup(self):
        self._job_cleanup()
        if self.feature_table_name:
            self.run_query(f"drop table if exists {self.feature_table_name}")
        self.session.close()
