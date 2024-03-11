import pandas as pd

from datetime import datetime
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union, Sequence, Optional

from ..utils import utils
from ..utils.logger import logger
from ..utils.constants import TrainTablesInfo
from ..wht.pb import getPB


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
            prediction_horizon_days (int): The period of days for prediction horizon.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
            input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model.
            inputs (List[str]): List of input material queries

        Returns:
            List[TrainTablesInfo]: A list of TrainTablesInfo objects, each containing the names of the feature and label tables, as well as their corresponding training dates.
        """
        logger.info("getting material names")
        (materials) = self.get_material_names_(
            session,
            material_table,
            start_date,
            end_date,
            features_profiles_model,
            model_hash,
            prediction_horizon_days,
            inputs,
        )

        if len(self._get_complete_sequences(materials)) == 0:
            try:
                _ = self.generate_training_materials(
                    session,
                    materials,
                    start_date,
                    features_profiles_model,
                    model_hash,
                    prediction_horizon_days,
                    output_filename,
                    site_config_path,
                    project_folder,
                    input_models,
                )
                (materials) = self.get_material_names_(
                    session,
                    material_table,
                    start_date,
                    end_date,
                    features_profiles_model,
                    model_hash,
                    prediction_horizon_days,
                    inputs,
                )
            except Exception as e:
                raise Exception(
                    f"Following exception occured while generating past materials with hash {model_hash} for {features_profiles_model} between dates {start_date} and {end_date}: {e}"
                )

        complete_sequences_materials = self._get_complete_sequences(materials)
        if len(complete_sequences_materials) == 0:
            raise Exception(
                f"Tried to materialise past data but no materialized data found for {features_profiles_model} between dates {start_date} and {end_date}"
            )

        return complete_sequences_materials

    def check_and_generate_more_materials(
        self,
        session,
        get_material_func: callable,
        data_requirement_check_func: callable,
        strategy: str,
        feature_data_min_date_diff: int,
        materials: List[TrainTablesInfo],
        max_num_of_materialisations: int,
        materialisation_dates: list,
        prediction_horizon_days: int,
        input_models: str,
        output_filename: str,
        site_config_path: str,
        project_folder: str,
    ):
        met_data_requirement = data_requirement_check_func(self, session, materials)

        logger.debug(f"Min data requirement satisfied: {met_data_requirement}")
        logger.debug(f"New material generation strategy : {strategy}")
        if met_data_requirement or strategy == "":
            return materials

        feature_package_path = utils.get_feature_package_path(input_models)
        max_materializations = (
            max_num_of_materialisations
            if strategy == "auto"
            else len(materialisation_dates)
        )

        for i in range(max_materializations):
            feature_date = None
            label_date = None

            if strategy == "auto":
                training_dates = [
                    utils.datetime_to_date_string(m.feature_table_date)
                    for m in materials
                ]
                training_dates = [
                    date_str for date_str in training_dates if len(date_str) != 0
                ]
                logger.info(f"training_dates : {training_dates}")
                training_dates = sorted(
                    training_dates,
                    key=lambda x: datetime.strptime(x, "%Y-%m-%d"),
                    reverse=True,
                )

                max_feature_date = training_dates[0]
                min_feature_date = training_dates[-1]

                feature_date, label_date = utils.generate_new_training_dates(
                    max_feature_date,
                    min_feature_date,
                    training_dates,
                    prediction_horizon_days,
                    feature_data_min_date_diff,
                )
                logger.info(
                    f"new generated dates for feature: {feature_date}, label: {label_date}"
                )
            elif strategy == "manual":
                dates = materialisation_dates[i].split(",")
                if len(dates) >= 2:
                    feature_date = dates[0]
                    label_date = dates[1]

                if feature_date is None or label_date is None:
                    continue

            try:
                for date in [feature_date, label_date]:
                    args = {
                        "feature_package_path": feature_package_path,
                        "features_valid_time": date,
                        "output_path": output_filename,
                        "site_config_path": site_config_path,
                        "project_folder": project_folder,
                    }
                    getPB().run(args)
            except Exception as e:
                logger.warning(str(e))
                logger.warning("Stopped generating new material dates.")
                break

            logger.info(
                "Materialised feature and label data successfully, "
                f"for dates {feature_date} and {label_date}"
            )

            # Get materials with new feature start date
            # and validate min data requirement again
            materials = get_material_func(start_date=feature_date)
            logger.debug(
                f"new feature tables: {[m.feature_table_name for m in materials]}"
            )
            logger.debug(f"new label tables: {[m.label_table_name for m in materials]}")
            met_data_requirement = data_requirement_check_func(self, session, materials)

            if met_data_requirement:
                break

        return materials

    def generate_training_materials(
        self,
        session,
        materials: List[TrainTablesInfo],
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
            materials (List[TrainTablesInfo]): materials info
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
            materials,
            start_date,
            features_profiles_model,
            model_hash,
            prediction_horizon_days,
        )

        materialise_data = True
        for date in [feature_date, label_date]:
            if date is not None:
                args = {
                    "feature_package_path": feature_package_path,
                    "features_valid_time": date,
                    "output_path": output_filename,
                    "site_config_path": site_config_path,
                    "project_folder": project_folder,
                }
                getPB().run(args)
                logger.info(f"Materialised data successfully, for date {date}")

        if not materialise_data:
            logger.warning(
                "Failed to materialise feature and label data. Will attempt to fetch materialised data from warehouse registry table"
            )
        return feature_date, label_date

    def get_valid_feature_label_dates(
        self,
        session,
        materials,
        start_date,
        features_profiles_model,
        model_hash,
        prediction_horizon_days,
    ):
        if len(materials) == 0:
            feature_date = utils.date_add(start_date, prediction_horizon_days)
            label_date = utils.date_add(feature_date, prediction_horizon_days)
            return feature_date, label_date

        feature_date, label_date = None, None
        for material_info in materials:
            if (
                material_info.feature_table_name is not None
                and material_info.label_table_name is None
            ):
                feature_table_name_ = material_info.feature_table_name
                assert (
                    self.is_valid_table(session, feature_table_name_) is True
                ), f"Failed to fetch \
                    valid feature_date and label_date because table {feature_table_name_} does not exist in the warehouse"
                label_date = utils.date_add(
                    material_info.feature_table_date.split()[0],
                    prediction_horizon_days,
                )
            elif (
                material_info.feature_table_name is None
                and material_info.label_table_name is not None
            ):
                label_table_name_ = material_info.label_table_name
                assert (
                    self.is_valid_table(session, label_table_name_) is True
                ), f"Failed to fetch \
                    valid feature_date and label_date because table {label_table_name_} does not exist in the warehouse"
                feature_date = utils.date_add(
                    material_info.label_table_date.split()[0],
                    -1 * prediction_horizon_days,
                )
            elif (
                material_info.feature_table_name is None
                and material_info.label_table_name is None
            ):
                feature_date = utils.date_add(start_date, prediction_horizon_days)
                label_date = utils.date_add(feature_date, prediction_horizon_days)
            else:
                logger.exception(
                    f"We don't need to fetch feature_date and label_date to materialise new datasets because Tables {material_info.feature_table_name} and {material_info.label_table_name} already exist. Please check generated materials for discrepancies."
                )
                raise Exception(
                    f"We don't need to fetch feature_date and label_date to materialise new datasets because Tables {material_info.feature_table_name} and {material_info.label_table_name} already exist. Please check generated materials for discrepancies."
                )
        return feature_date, label_date

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
                feature_table_query = utils.replace_seq_no_in_query(
                    material_table_query, int(feature_material_seq_no)
                )
                assert self.check_table_entry_in_material_registry(
                    session, feature_table_query
                ), f"Material table {feature_table_query} does not exist"

            if label_material_seq_no is not None:
                label_table_query = utils.replace_seq_no_in_query(
                    material_table_query, int(label_material_seq_no)
                )
                assert self.check_table_entry_in_material_registry(
                    session, label_table_query
                ), f"Material table {label_table_query} does not exist"

            return True
        except AssertionError as e:
            logger.debug(
                f"{e}. Skipping the sequence tuple {feature_material_seq_no, label_material_seq_no} as it is not valid"
            )
            return False

    def _fetch_valid_historic_materials(
        self,
        session,
        row,
        features_profiles_model,
        model_hash,
        inputs,
        materials,
    ):
        feature_table_name_, label_table_name_ = None, None
        if row.FEATURE_SEQ_NO is not None:
            feature_table_name_ = getPB().get_material_name(
                features_profiles_model,
                model_hash,
                row.FEATURE_SEQ_NO,
            )
        if row.LABEL_SEQ_NO is not None:
            label_table_name_ = getPB().get_material_name(
                features_profiles_model,
                model_hash,
                row.LABEL_SEQ_NO,
            )

        if (
            feature_table_name_ is not None
            and not self.is_valid_table(session, feature_table_name_)
        ) or (
            label_table_name_ is not None
            and not self.is_valid_table(session, label_table_name_)
        ):
            return

        # Iterate over inputs and validate meterial names
        validation_flag = True
        for input_material_query in inputs:
            if not self.validate_historical_materials_hash(
                session,
                input_material_query,
                row.FEATURE_SEQ_NO,
                row.LABEL_SEQ_NO,
            ):
                validation_flag = False
                break

        if validation_flag:
            train_table_info = TrainTablesInfo(
                feature_table_name=feature_table_name_,
                feature_table_date=str(row.FEATURE_END_TS),
                label_table_name=label_table_name_,
                label_table_date=str(row.LABEL_END_TS),
            )
            materials.append(train_table_info)

    def _get_complete_sequences(self, sequences: List[Sequence]) -> int:
        """
        A sequence is said to be complete if it does not have any Nones.
        """
        complete_sequences = [
            sequence
            for sequence in sequences
            if all(element is not None for element in sequence)
        ]
        return complete_sequences

    @abstractmethod
    def fetch_filtered_table(
        self,
        df,
        features_profiles_model,
        model_hash,
        start_time,
        end_time,
        columns,
    ):
        pass

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
    def send_to_train_env(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def is_valid_table(self, session, table_name) -> bool:
        pass

    @abstractmethod
    def check_table_entry_in_material_registry(
        self, session, material_table_name: str
    ) -> bool:
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
        prediction_horizon_days: int,
        inputs: List[str],
    ) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]],]:
        pass

    @abstractmethod
    def get_tables_by_prefix(self, session, prefix: str) -> str:
        pass

    @abstractmethod
    def get_creation_ts(
        self,
        session,
        material_table: str,
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
    def check_for_classification_data_requirement(
        self, session, meterials, label_column, label_value
    ) -> bool:
        pass

    @abstractmethod
    def check_for_regression_data_requirement(self, session, meterials) -> bool:
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
    def pre_job_cleanup(self, session) -> None:
        pass

    @abstractmethod
    def post_job_cleanup(self, session) -> None:
        pass
