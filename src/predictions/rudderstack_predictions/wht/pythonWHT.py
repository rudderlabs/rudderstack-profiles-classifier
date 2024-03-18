from typing import List, Optional, Sequence, Tuple, Dict

from .rudderPB import MATERIAL_PREFIX

from ..utils import utils

from ..utils.constants import TrainTablesInfo
from ..utils.logger import logger
from ..utils import constants
from ..connectors.Connector import Connector
from .rudderPB import RudderPB
from .mockPB import MockPB


def split_key(item):
    parts = item.split("_")
    if len(parts) > 1 and parts[-1].isdigit():
        return int(parts[-1])
    return 0


class PythonWHT:
    def init(
        self,
        connector: Connector,
        session,
        site_config_path: str,
        project_folder_path: str,
    ) -> None:
        self.connector = connector
        self.session = session
        self.site_config_path = site_config_path
        self.project_folder_path = project_folder_path
        self.cached_registry_table_name = ""

    def _getPB(self):
        mock = False
        if mock:
            return MockPB()
        return RudderPB()

    def get_registry_table_name(self):
        if self.cached_registry_table_name == "":
            material_registry_tables = self.connector.get_tables_by_prefix(
                self.session, "MATERIAL_REGISTRY"
            )

            sorted_material_registry_tables = sorted(
                material_registry_tables, key=split_key, reverse=True
            )
            self.cached_registry_table_name = sorted_material_registry_tables[0]
        return self.cached_registry_table_name

    def get_latest_entity_var_table(self, entity_key: str) -> Tuple[str, str, str]:
        model_hash, model_name = self._getPB().get_latest_material_hash(
            entity_key,
            self.site_config_path,
            self.project_folder_path,
        )
        creation_ts = self.connector.get_creation_ts(
            self.session,
            self.get_registry_table_name(),
            model_hash,
            entity_key,
        )
        return model_hash, model_name, creation_ts

    def latest_existing_entity_var_table_from_registry(
        self, model_hash: str, feature_model_name: str
    ) -> str:
        seq_no = self.connector.get_latest_seq_no_from_registry(
            self.session, self.get_registry_table_name(), model_hash, feature_model_name
        )
        return self.compute_material_name(feature_model_name, model_hash, seq_no)

    def get_latest_entity_var_table_name(
        self, model_hash: str, entity_var_model: str, inputs: list
    ) -> str:
        try:
            input = inputs[0]
            seq_no = int(input.split("_")[-1])
            return (
                MATERIAL_PREFIX
                + entity_var_model
                + "_"
                + model_hash
                + "_"
                + f"{seq_no:.0f}"
            )
        except IndexError:
            raise Exception(
                "Error while getting feature table name using model "
                f"hash {model_hash}, feature profile model {entity_var_model} and input {inputs}"
            )

    def _validate_historical_materials_hash(
        self,
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
                assert self.connector.check_table_entry_in_material_registry(
                    self.session,
                    self.get_registry_table_name(),
                    self._split_material_name(feature_table_query),
                ), f"Material table {feature_table_query} does not exist"

            if label_material_seq_no is not None:
                label_table_query = utils.replace_seq_no_in_query(
                    material_table_query, int(label_material_seq_no)
                )
                assert self.connector.check_table_entry_in_material_registry(
                    self.session,
                    self.get_registry_table_name(),
                    self._split_material_name(label_table_query),
                ), f"Material table {label_table_query} does not exist"

            return True
        except AssertionError as e:
            logger.debug(
                f"{e}. Skipping the sequence tuple {feature_material_seq_no, label_material_seq_no} as it is not valid"
            )
            return False

    def _fetch_valid_historic_materials(
        self,
        table_row,
        feature_model_name,
        model_hash,
        inputs,
        materials,
    ):
        feature_material_name, label_material_name = None, None
        if table_row.FEATURE_SEQ_NO is not None:
            feature_material_name = self.compute_material_name(
                feature_model_name, model_hash, table_row.FEATURE_SEQ_NO
            )
        if table_row.LABEL_SEQ_NO is not None:
            label_material_name = self.compute_material_name(
                feature_model_name, model_hash, table_row.LABEL_SEQ_NO
            )

        if (
            feature_material_name is not None
            and not self.connector.is_valid_table(self.session, feature_material_name)
        ) or (
            label_material_name is not None
            and not self.connector.is_valid_table(self.session, label_material_name)
        ):
            return

        # Iterate over inputs and validate meterial names
        validation_flag = True
        for input_material_query in inputs:
            if not self._validate_historical_materials_hash(
                input_material_query,
                table_row.FEATURE_SEQ_NO,
                table_row.LABEL_SEQ_NO,
            ):
                validation_flag = False
                break

        if validation_flag:
            train_table_info = TrainTablesInfo(
                feature_table_name=feature_material_name,
                feature_table_date=str(table_row.FEATURE_END_TS),
                label_table_name=label_material_name,
                label_table_date=str(table_row.LABEL_END_TS),
            )
            materials.append(train_table_info)

    def _get_material_names(
        self,
        start_time: str,
        end_time: str,
        features_model_name: str,
        model_hash: str,
        prediction_horizon_days: int,
        inputs: List[str],
    ) -> List[TrainTablesInfo]:
        """Generates material names as list containing feature table name and label table name required to create the training model and their corresponding training dates.

        Args:
            session : connection session for warehouse access
            material_table (str): Name of the material table(present in constants.py file)
            start_time (str): train_start_dt
            end_time (str): train_end_dt
            model_name (str): Present in model_configs file
            model_hash (str) : latest model hash
            prediction_horizon_days (int): period of days

        Returns:
            List[TrainTablesInfo]: A list of TrainTablesInfo objects, each containing the names of the feature and label tables, as well as their corresponding training dates.
        """
        feature_label_df = self.connector.join_feature_label_tables(
            self.session,
            self.get_registry_table_name(),
            features_model_name,
            model_hash,
            start_time,
            end_time,
            prediction_horizon_days,
        )
        materials = list()
        for row in feature_label_df:
            self._fetch_valid_historic_materials(
                row,
                features_model_name,
                model_hash,
                inputs,
                materials,
            )
        return materials

    def _generate_training_materials(
        self,
        materials: List[TrainTablesInfo],
        start_date: str,
        prediction_horizon_days: int,
        input_models: List[str],
    ) -> None:
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
        feature_date, label_date = self._get_valid_feature_label_dates(
            materials,
            start_date,
            prediction_horizon_days,
        )

        materialise_data = True
        for date in [feature_date, label_date]:
            if date is not None:
                self.run(feature_package_path, date)

        if not materialise_data:
            logger.warning(
                "Failed to materialise feature and label data. Will attempt to fetch materialised data from warehouse registry table"
            )

    def run(self, feature_package_path: str, date: str):
        args = {
            "feature_package_path": feature_package_path,
            "features_valid_time": date,
            "site_config_path": self.site_config_path,
            "project_folder": self.project_folder_path,
        }
        self._getPB().run(args)
        logger.info(f"Materialised data successfully, for date {date}")

    def _get_valid_feature_label_dates(
        self,
        materials,
        start_date,
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
                    self.connector.is_valid_table(self.session, feature_table_name_)
                    is True
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
                    self.connector.is_valid_table(self.session, label_table_name_)
                    is True
                ), f"Failed to fetch \
                    valid feature_date and label_date because table {label_table_name_} does not exist in the warehouse"
                feature_date = utils.date_add(
                    material_info.label_table_date.split()[0],
                    -prediction_horizon_days,
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

    def _split_material_name(self, name: str) -> dict:
        """
        Splits given material table into model_name, model_hash and seq_no
        Ex. Splits "Material_user_var_table_54ddc22a_383" into (user_var_table, 54ddc22a, 383)
        """
        mlower = name.lower()
        # TODO - Move this logic to bigquery conenctor
        if "`" in mlower:  # BigQuery case table name
            table_name = mlower.split("`")[-2]
        else:
            table_name = mlower.split()[-1]
        table_suffix = table_name.split(MATERIAL_PREFIX.lower())[-1]
        split_parts = table_suffix.split("_")
        try:
            seq_no = int(split_parts[-1])
        except ValueError:
            raise Exception(f"Unable to extract seq_no from material name {name}")
        model_hash = split_parts[-2]
        model_name = "_".join(split_parts[0:-2])
        return {
            "model_name": model_name,
            "model_hash": model_hash,
            "seq_no": seq_no,
        }

    def get_material_names(
        self,
        start_date: str,
        end_date: str,
        features_model_name: str,
        model_hash: str,
        prediction_horizon_days: int,
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

        def get_complete_sequences(sequences: List[Sequence]) -> int:
            """
            A sequence is said to be complete if it does not have any Nones.
            """
            complete_sequences = [
                sequence
                for sequence in sequences
                if all(element is not None for element in sequence)
            ]
            return complete_sequences

        logger.info("getting material names")
        (materials) = self._get_material_names(
            start_date,
            end_date,
            features_model_name,
            model_hash,
            prediction_horizon_days,
            inputs,
        )
        if len(get_complete_sequences(materials)) == 0:
            self._generate_training_materials(
                materials,
                start_date,
                prediction_horizon_days,
                input_models,
            )
            (materials) = self._get_material_names(
                start_date,
                end_date,
                features_model_name,
                model_hash,
                prediction_horizon_days,
                inputs,
            )

        complete_sequences_materials = get_complete_sequences(materials)
        if len(complete_sequences_materials) == 0:
            raise Exception(
                f"Tried to materialise past data but no materialized data found for {features_model_name} between dates {start_date} and {end_date}"
            )
        return complete_sequences_materials

    def compute_material_name(
        self, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        return f"{MATERIAL_PREFIX}{model_name}_{model_hash}_{seq_no:.0f}"

    def get_input_models(
        self,
        original_input_models: List[str],
    ) -> List[str]:
        """Find matches for input models in the JSON data. If no matches are found, an exception is raised.
        Args:
            original_input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model.
            train_summary_output_file_name (str): output filename
            project_folder (str): project folder path to pb_project.yaml file
            site_config_path (str): path to the siteconfig.yaml file
        Returns:
            List[str]: List of input models - full paths in the profiles project for models that are required to generate the current model.
        """
        args = {
            "site_config_path": self.site_config_path,
            "project_folder": self.project_folder,
        }

        json_data = self._getPB().show_models(args)

        # Find matches for input models in the JSON data
        new_input_models = []

        for model in original_input_models:
            model_key = model.split("/")[-1]
            found_unique_match = False
            for key in json_data:
                if key.endswith(model_key):
                    if found_unique_match:
                        raise ValueError(
                            f"Multiple unique occurrences found for {model_key}"
                        )
                    model_path = key.split("/", 1)[-1]
                    new_input_models.append(model_path)
                    found_unique_match = True

        return new_input_models

    def get_input_column_types(
        self,
        trainer_obj,
        table_name: str,
        label_column: str,
        entity_column: str,
    ) -> Dict:
        numeric_columns = utils.merge_lists_to_unique(
            self.connector.get_non_stringtype_features(
                self.session, table_name, label_column, entity_column
            ),
            trainer_obj.prep.numeric_pipeline["numeric_columns"],
        )
        categorical_columns = utils.merge_lists_to_unique(
            self.connector.get_stringtype_features(
                self.session, table_name, label_column, entity_column
            ),
            trainer_obj.prep.categorical_pipeline["categorical_columns"],
        )
        arraytype_columns = self.connector.get_arraytype_columns(
            self.session, table_name, label_column, entity_column
        )
        timestamp_columns = (
            self.connector.get_timestamp_columns(
                self.session, table_name, label_column, entity_column
            )
            if len(trainer_obj.prep.timestamp_columns) == 0
            else trainer_obj.prep.timestamp_columns
        )
        return {
            "numeric": numeric_columns,
            "categorical": categorical_columns,
            "arraytype": arraytype_columns,
            "timestamp": timestamp_columns,
        }
