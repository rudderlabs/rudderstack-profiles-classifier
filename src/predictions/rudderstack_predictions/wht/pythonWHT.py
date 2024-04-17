from typing import List, Optional, Sequence, Tuple

from .rudderPB import MATERIAL_PREFIX

from ..utils import utils

from ..utils.constants import TrainTablesInfo, MATERIAL_DATE_FORMAT
from ..utils.logger import logger
from ..connectors.Connector import Connector
from .rudderPB import RudderPB
from .mockPB import MockPB

import json


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
        mock = True
        if mock:
            return MockPB()
        return RudderPB()

    def get_input_models(
        self,
        inputs: List[str],
    ) -> List[str]:
        """Returns List of input models - full paths in the profiles project for models
        that are required to generate the current model.
        """
        original_input_models = [
            self.split_material_name(input_)["model_name"] for input_ in inputs
        ]

        args = {
            "site_config_path": self.site_config_path,
            "project_folder": self.project_folder_path,
        }

        # Fetch models information from the project
        pb_show_models_response_output = self._getPB().show_models(args)
        models_info = self._getPB().extract_json_from_stdout(
            pb_show_models_response_output
        )

        # Find matching models in the project
        absolute_input_models = []

        for model_name in original_input_models:
            matching_models = [
                key.split("/", 1)[-1] for key in models_info if key.endswith(model_name)
            ]

            if len(matching_models) == 1:
                absolute_input_models.append(matching_models[0])
            elif len(matching_models) > 1:
                raise ValueError(
                    f"Multiple models with name {model_name} are found. Please ensure the models added in inputs are named uniquely and retry"
                )

        return absolute_input_models

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
        model_hash, entity_var_model_name = self._getPB().get_latest_material_hash(
            entity_key,
            self.site_config_path,
            self.project_folder_path,
        )
        creation_ts = self.get_model_creation_ts(model_hash, entity_key)
        return model_hash, entity_var_model_name, creation_ts

    def get_model_creation_ts(self, model_hash: str, entity_key: str):
        return self.connector.get_creation_ts(
            self.session,
            self.get_registry_table_name(),
            model_hash,
            entity_key,
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
                    self.split_material_name(feature_table_query),
                ), f"Material table {feature_table_query} does not exist"

            if label_material_seq_no is not None:
                label_table_query = utils.replace_seq_no_in_query(
                    material_table_query, int(label_material_seq_no)
                )
                assert self.connector.check_table_entry_in_material_registry(
                    self.session,
                    self.get_registry_table_name(),
                    self.split_material_name(label_table_query),
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
        entity_var_model_name,
        model_hash,
        inputs,
        materials,
    ):
        feature_material_name, label_material_name = None, None
        if table_row.FEATURE_SEQ_NO is not None:
            feature_material_name = self.compute_material_name(
                entity_var_model_name, model_hash, table_row.FEATURE_SEQ_NO
            )
        if table_row.LABEL_SEQ_NO is not None:
            label_material_name = self.compute_material_name(
                entity_var_model_name, model_hash, table_row.LABEL_SEQ_NO
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
            feature_table_date = (
                "None"
                if table_row.FEATURE_END_TS is None
                else table_row.FEATURE_END_TS.strftime(MATERIAL_DATE_FORMAT)
            )

            label_table_date = (
                "None"
                if table_row.LABEL_END_TS is None
                else table_row.LABEL_END_TS.strftime(MATERIAL_DATE_FORMAT)
            )

            train_table_info = TrainTablesInfo(
                feature_table_name=feature_material_name,
                feature_table_date=feature_table_date,
                label_table_name=label_material_name,
                label_table_date=label_table_date,
            )
            materials.append(train_table_info)

    def _get_material_names(
        self,
        start_time: str,
        end_time: str,
        entity_var_model_name: str,
        model_hash: str,
        prediction_horizon_days: int,
        inputs: List[str],
    ) -> List[TrainTablesInfo]:
        """Generates material names as list containing feature table name and label table name
            required to create the training model and their corresponding training dates.

        Returns:
            List[TrainTablesInfo]: A list of TrainTablesInfo objects,
            each containing the names of the feature and label tables, as well as their corresponding training dates.
        """
        feature_label_df = self.connector.join_feature_label_tables(
            self.session,
            self.get_registry_table_name(),
            entity_var_model_name,
            model_hash,
            start_time,
            end_time,
            prediction_horizon_days,
        )
        materials = list()
        for row in feature_label_df:
            self._fetch_valid_historic_materials(
                row,
                entity_var_model_name,
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

    def split_material_name(self, name: str) -> dict:
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
        entity_var_model_name: str,
        model_hash: str,
        prediction_horizon_days: int,
        input_models: List[str],
        inputs: List[str],
    ) -> List[TrainTablesInfo]:
        """
        Retrieves the names of the feature and label tables, as well as their corresponding training dates, based on the provided inputs.
        If no materialized data is found within the specified date range, the function attempts to materialize the feature and label data using the `materialise_past_data` function.
        If no materialized data is found even after materialization, an exception is raised.

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
            entity_var_model_name,
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
                entity_var_model_name,
                model_hash,
                prediction_horizon_days,
                inputs,
            )

        complete_sequences_materials = get_complete_sequences(materials)
        if len(complete_sequences_materials) == 0:
            raise Exception(
                f"Tried to materialise past data but no materialized data found for {entity_var_model_name} between dates {start_date} and {end_date}"
            )
        return complete_sequences_materials

    def compute_material_name(
        self, model_name: str, model_hash: str, seq_no: int
    ) -> str:
        return f"{MATERIAL_PREFIX}{model_name}_{model_hash}_{seq_no:.0f}"
