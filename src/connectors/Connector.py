import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union, Sequence, Optional, Dict

import snowflake.snowpark
import src.utils.utils as utils
from src.utils import constants
from src.utils.logger import logger
from src.utils.constants import TrainTablesInfo


class Connector(ABC):
    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format for connection to warehouses

        Args:
            credentials (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            dict: Data warehouse creadentials remapped in format that is required to create a connection to warehouse
        """
        new_creds = {
            k if k != "dbname" else "database": v
            for k, v in credentials.items()
            if k != "type"
        }
        return new_creds

    def get_material_names(
        self,
        session,
        material_table: str,
        start_date: str,
        end_date: str,
        features_profiles_model: str,
        model_hash: str,
        material_table_prefix: str,
        prediction_horizon_days: int,
        output_filename: str,
        site_config_path: str,
        project_folder: str,
        input_models: List[str],
        inputs: List[str],
    ) -> List[TrainTablesInfo]:
        """
        Retrieves the names of the feature and label tables, as well as their corresponding training dates, based on the provided inputs.
        If no materialized data is found within the specified date range, the function attempts to materialize the feature and label data using the `materialise_past_data` function.
        If no materialized data is found even after materialization, an exception is raised.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            material_table (str): The name of the material table (present in constants.py file).
            start_date (str): The start date for training data.
            end_date (str): The end date for training data.
            features_profiles_model (str): The name of the model.
            model_hash (str): The latest model hash.
            material_table_prefix (str): A constant.
            prediction_horizon_days (int): The period of days for prediction horizon.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
            input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model.
            inputs (List[str]): List of input material queries

        Returns:
            List[Tuple[Dict[str, str], Dict[str, str]]]: EXAMPLE:
            materials = [({"name": feature_table_name, "end_dt": feature_table_dt}, {"name": label_table_name, "end_dt": label_table_dt})....]
        """
        (
            material_names,
            training_dates,
        ) = self.get_material_names_(
            session,
            material_table,
            start_date,
            end_date,
            features_profiles_model,
            model_hash,
            material_table_prefix,
            prediction_horizon_days,
            inputs,
        )
        if self._count_complete_sequences(material_names) == 0:
            try:
                _ = self.generate_training_materials(
                    session,
                    material_names,
                    training_dates,
                    start_date,
                    features_profiles_model,
                    model_hash,
                    prediction_horizon_days,
                    output_filename,
                    site_config_path,
                    project_folder,
                    input_models,
                )
                (
                    material_names,
                    training_dates,
                ) = self.get_material_names_(
                    session,
                    material_table,
                    start_date,
                    end_date,
                    features_profiles_model,
                    model_hash,
                    material_table_prefix,
                    prediction_horizon_days,
                    inputs,
                )
            except Exception as e:
                raise Exception(
                    f"Following exception occured while generating past materials with hash {model_hash} for {features_profiles_model} between dates {start_date} and {end_date}: {e}"
                )
        if self._count_complete_sequences(material_names) == 0:
            raise Exception(
                f"Tried to materialise past data but no materialized data found for {features_profiles_model} between dates {start_date} and {end_date}"
            )
        materials = []
        for material_pair, date_pair in zip(material_names, training_dates):
            if material_pair[0] and material_pair[1]:
                train_table_info = TrainTablesInfo(
                    feature_table_name=material_pair[0],
                    feature_table_date=date_pair[0],
                    label_table_name=material_pair[1],
                    label_table_date=date_pair[1],
                )
                materials.append(train_table_info)
        return materials

    def generate_training_materials(
        self,
        session,
        material_names: List[Tuple[str, str]],
        training_dates: List[Tuple[str, str]],
        start_date: str,
        features_profiles_model: str,
        model_hash: str,
        prediction_horizon_days: int,
        output_filename: str,
        site_config_path: str,
        project_folder: str,
        input_models: List[str],
    ) -> Tuple[str, str]:
        """
        Generates training dataset from start_date and end_date, and fetches the resultant table names from the material_table.
        Args:
            session : warehouse session
            material_names (List[Tuple[str, str]]): Name of materials
            training_dates (List[Tuple[str, str]]): training dates corresponding to material_names
            start_date (str): Start date for training data.
            features_profiles_model (str): The name of the model.
            model_hash (str): The latest model hash.
            prediction_horizon_days (int): The period of days for prediction horizon.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
            input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model.

        Returns:
            Tuple[str, str]: A tuple containing feature table date and label table date strings
        """
        feature_package_path = utils.get_feature_package_path(input_models)
        feature_date, label_date = self.get_valid_feature_label_dates(
            session,
            material_names,
            training_dates,
            start_date,
            features_profiles_model,
            model_hash,
            prediction_horizon_days,
        )

        materialise_data = True
        for date in [feature_date, label_date]:
            if date is not None:
                materialise_data = materialise_data and utils.materialise_past_data(
                    date,
                    feature_package_path,
                    output_filename,
                    site_config_path,
                    project_folder,
                )
                logger.info(f"Materialised data successfully, for date {date}")

        if not materialise_data:
            logger.warning(
                "Failed to materialise feature and label data. Will attempt to fetch materialised data from warehouse registry table"
            )
        return feature_date, label_date

    def get_valid_feature_label_dates(
        self,
        session,
        material_names,
        training_dates,
        start_date,
        features_profiles_model,
        model_hash,
        prediction_horizon_days,
    ):
        for material_pair, date_pair in zip(material_names, training_dates):
            if material_pair[0] is not None and material_pair[1] is None:
                feature_table_name_ = material_pair[0]
                if self.is_valid_table(session, feature_table_name_):
                    feature_date = None
                    label_date = utils.date_add(
                        date_pair[0].split()[0], prediction_horizon_days
                    )
            elif material_pair[0] is None and material_pair[1] is not None:
                label_table_name_ = material_pair[1]
                if self.is_valid_table(session, label_table_name_):
                    feature_date = utils.date_add(
                        date_pair[1].split()[0],
                        -prediction_horizon_days,
                    )
                    label_date = None
            elif material_pair[0] is None and material_pair[1] is None:
                feature_date = utils.date_add(start_date, prediction_horizon_days)
                label_date = utils.date_add(feature_date, prediction_horizon_days)
            else:
                logger.exception(
                    f"Unexpected material names: {material_pair} and corresponding training dates: {date_pair} for {features_profiles_model} and {model_hash}"
                )
        return feature_date, label_date

    def fetch_training_material_names(
        self,
        session,
        feature_date,
        label_date,
        material_registry_table,
        features_profiles_model,
        material_table_prefix,
    ) -> Tuple[str, str]:
        """Fetches the materialised table names of feature table and label table from warehouse registry table, based on the end_ts field in the registry table.
        If there are more than one materialised table names with the same end_ts, it returns the most recently created one, using creation_ts.
        Imp Note: It does not do any check on model hash, so it is possible that the materialised tables are not consistent.
        Currently this is not being in use. This was created as a back up for pb versions inconsistency on rudder-sources
        Args:
            session (_type_): _description_
            feature_date (_type_): _description_
            label_date (_type_): _description_
            material_registry_table (_type_): _description_
            features_profiles_model (_type_): _description_
            material_table_prefix (_type_): _description_

        Returns:
            Tuple[str, str]: _description_
        """
        for dt in [feature_date, label_date]:
            query = f"select seq_no, model_hash from {material_registry_table} where DATE(end_ts) = '{dt}' and model_name = '{features_profiles_model}' order by creation_ts desc"
            table_instance_details = self.run_query(session, query)
            for row in table_instance_details:
                seq_no = row[0]
                model_hash = row[1]
                table_name = utils.generate_material_name(
                    material_table_prefix,
                    features_profiles_model,
                    model_hash,
                    str(seq_no),
                )
                if self.is_valid_table(session, table_name):
                    if dt == feature_date:
                        feature_table_name = table_name
                    else:
                        label_table_name = table_name
                    break
        return feature_table_name, label_table_name

    def get_latest_material_hash(
        self,
        entity_key: str,
        var_table_suffix: List[str],
        output_filename: str,
        site_config_path: str = None,
        project_folder: str = None,
    ) -> Tuple[str, str]:
        project_folder = utils.get_project_folder(project_folder, output_filename)
        pb = utils.get_pb_path()
        args = [
            pb,
            "compile",
            "-p",
            project_folder,
            "--migrate_on_load=True",
        ]
        if site_config_path is not None:
            args.extend(["-c", site_config_path])
        logger.info(f"Fetching latest model hash by running command: {' '.join(args)}")
        pb_compile_output_response = utils.subprocess_run(args)
        pb_compile_output = (pb_compile_output_response.stdout).lower()
        logger.info(f"pb compile output: {pb_compile_output}")

        features_profiles_model = None
        for var_table in var_table_suffix:
            if entity_key + var_table in pb_compile_output:
                features_profiles_model = entity_key + var_table
                break
        if features_profiles_model is None:
            raise Exception(
                f"Could not find any matching var table in the output of pb compile command"
            )

        material_file_prefix = (
            constants.MATERIAL_TABLE_PREFIX + features_profiles_model + "_"
        ).lower()

        try:
            model_hash = pb_compile_output[
                pb_compile_output.index(material_file_prefix)
                + len(material_file_prefix) :
            ].split("_")[0]
        except ValueError:
            raise Exception(
                f"Could not find material file prefix {material_file_prefix} in the output of pb compile command: {pb_compile_output}"
            )
        return model_hash, features_profiles_model

    def validate_historical_materials_hash(
        self,
        session,
        material_table_query: str,
        feature_material_seq_no: Optional[int],
        label_material_seq_no: Optional[int],
    ) -> bool:
        """
        entity_var_table hash doesn't necessarily change if the underlying entity vars change.
        So, we need to validate if the historic entity var tables were generated by the same model
        that is generating current entity var table.
        We do that by checking the input tables that generate entity var table, by replacing
        the current seq no with historic seq nos and verify those tables exist.
        This relies on the input sql queries sent by profiles which point to the current materialised tables.

        Args:
            session: WH connector session/cursor for data warehouse access
            material_table_query (str): Query to fetch the material table names
            feature_material_seq_no (int): feature material seq no
            label_material_seq_no (int): label material seq no
        Returns:
            bool: True if the material table exists with given seq no else False
        """
        try:
            # Replace the last seq_no with the current seq_no
            # and prepare sql statement to check for the table existence
            # Ex. select * from material_shopify_user_features_fa138b1a_785 limit 1
            if feature_material_seq_no is not None:
                feature_table_query = (
                    utils.replace_seq_no_in_query(
                        material_table_query, int(feature_material_seq_no)
                    )
                    + " limit 1"
                )
                result = self.run_query(session, feature_table_query, response=True)
                assert len(result) != 0

            if label_material_seq_no is not None:
                label_table_query = (
                    utils.replace_seq_no_in_query(
                        material_table_query, int(label_material_seq_no)
                    )
                    + " limit 1"
                )
                result = self.run_query(session, label_table_query, response=True)
                assert len(result) != 0

            return True
        except:
            logger.info(
                f"{material_table_query} is not materialized for one of the \
                        seq nos '{feature_material_seq_no}', '{label_material_seq_no}'"
            )
            return False

    def _count_complete_sequences(self, sequences: List[Sequence]) -> int:
        """
        A sequence is said to be complete if it does not have any Nones.
        """
        complete_sequences = [
            sequence
            for sequence in sequences
            if all(element is not None for element in sequence)
        ]
        return len(complete_sequences)

    @abstractmethod
    def build_session(self, credentials: dict):
        pass

    @abstractmethod
    def join_file_path(self, file_name: str) -> str:
        pass

    @abstractmethod
    def run_query(self, session, query: str, response: bool) -> Optional[Sequence]:
        pass

    @abstractmethod
    def call_procedure(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_merged_table(self, feature_table, feature_table_instance):
        pass

    @abstractmethod
    def fetch_processor_mode(
        self, user_preference_order_infra: List[str], is_rudder_backend: bool
    ) -> str:
        pass

    @abstractmethod
    def get_table(self, session, table_name: str, **kwargs):
        pass

    @abstractmethod
    def get_table_as_dataframe(
        self, session, table_name: str, **kwargs
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def is_valid_table(self, session, table_name) -> bool:
        pass

    @abstractmethod
    def write_pandas(
        self,
        df: pd.DataFrame,
        table_name: str,
        auto_create_table: bool,
        overwrite: bool,
    ) -> Any:
        pass

    @abstractmethod
    def label_table(
        self,
        session,
        label_table_name: str,
        label_column: str,
        entity_column: str,
        label_value: Union[str, int, float],
    ):
        pass

    @abstractmethod
    def save_file(
        self, session, file_name: str, stage_name: str, overwrite: bool
    ) -> Any:
        pass

    @abstractmethod
    def get_non_stringtype_features(
        self, feature_table, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(
        self, feature_table, label_column: str, entity_column: str, **kwargs
    ) -> List[str]:
        pass

    @abstractmethod
    def get_arraytype_columns(self, session, table_name: str) -> List[str]:
        pass

    @abstractmethod
    def get_arraytype_columns_from_table(self, table: Any, **kwargs) -> list:
        pass

    @abstractmethod
    def get_high_cardinal_features(
        self, feature_table, label_column, entity_column, cardinal_feature_threshold
    ) -> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(self, session, table_name: str) -> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns_from_table(
        self, session, table_name: str, **kwargs
    ) -> List[str]:
        pass

    @abstractmethod
    def get_default_label_value(
        self, session, table_name: str, label_column: str, positive_boolean_flags: list
    ):
        pass

    @abstractmethod
    def get_material_names_(
        self,
        session,
        material_table: str,
        start_time: str,
        end_time: str,
        model_name: str,
        model_hash: str,
        material_table_prefix: str,
        prediction_horizon_days: int,
        inputs: List[str],
    ) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]],]:
        pass

    @abstractmethod
    def get_material_registry_name(self, session, table_prefix: str) -> str:
        pass

    @abstractmethod
    def get_creation_ts(
        self,
        session,
        material_table: str,
        model_name: str,
        model_hash: str,
        entity_key: str,
    ):
        pass

    @abstractmethod
    def get_end_ts(
        self,
        session,
        material_table: str,
        model_name: str,
        model_hash: str,
        seq_no: int,
    ):
        pass

    @abstractmethod
    def add_index_timestamp_colum_for_predict_data(
        self, predict_data, index_timestamp: str, end_ts: str
    ):
        pass

    @abstractmethod
    def fetch_staged_file(
        self, session, stage_name: str, file_name: str, target_folder: str
    ) -> None:
        pass

    @abstractmethod
    def drop_cols(self, table, col_list: list):
        pass

    @abstractmethod
    def filter_feature_table(
        self,
        feature_table,
        entity_column: str,
        max_row_count: int,
        min_sample_for_training: int,
    ):
        pass

    @abstractmethod
    def validate_columns_are_present(self, feature_table, label_column):
        pass

    @abstractmethod
    def validate_class_proportions(self, feature_table, label_column):
        pass

    @abstractmethod
    def validate_label_distinct_values(self, feature_table, label_column):
        pass

    @abstractmethod
    def add_days_diff(self, table, new_col, time_col, end_ts):
        pass

    @abstractmethod
    def join_feature_table_label_table(
        self, feature_table, label_table, entity_column: str
    ):
        pass

    @abstractmethod
    def get_distinct_values_in_column(self, table, column_name: str) -> List:
        pass

    @abstractmethod
    def get_material_registry_table(
        self, session: Any, material_registry_table_name: str
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def generate_type_hint(self, df: Any):
        pass

    @abstractmethod
    def call_prediction_udf(
        self,
        predict_data: Any,
        prediction_udf: Any,
        entity_column: str,
        index_timestamp: str,
        score_column_name: str,
        percentile_column_name: str,
        output_label_column: str,
        train_model_id: str,
        prob_th: float,
        input: Any,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def select_relevant_columns(self, table, training_features_columns):
        pass

    @abstractmethod
    def cleanup(self, *args, **kwargs) -> None:
        pass
