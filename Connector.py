import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union, Sequence, Optional

import utils
import constants
from logger import logger

class Connector(ABC):
    def remap_credentials(self, credentials: dict) -> dict:
        """Remaps credentials from profiles siteconfig to the expected format for connection to warehouses

        Args:
            credentials (dict): Data warehouse credentials from profiles siteconfig

        Returns:
            dict: Data warehouse creadentials remapped in format that is required to create a connection to warehouse
        """
        new_creds = {k if k != 'dbname' else 'database': v for k, v in credentials.items() if k != 'type'}
        return new_creds
 
    def get_material_names(self, session, material_table: str, start_date: str, end_date: str, 
                        package_name: str, features_profiles_model: str, model_hash: str, material_table_prefix: str, prediction_horizon_days: int, 
                        output_filename: str, site_config_path: str, project_folder: str, input_models: List[str])-> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """
        Retrieves the names of the feature and label tables, as well as their corresponding training dates, based on the provided inputs.
        If no materialized data is found within the specified date range, the function attempts to materialize the feature and label data using the `materialise_past_data` function.
        If no materialized data is found even after materialization, an exception is raised.

        Args:
            session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
            material_table (str): The name of the material table (present in constants.py file).
            start_date (str): The start date for training data.
            end_date (str): The end date for training data.
            package_name (str): The name of the package.
            features_profiles_model (str): The name of the model.
            model_hash (str): The latest model hash.
            material_table_prefix (str): A constant.
            prediction_horizon_days (int): The period of days for prediction horizon.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
            input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model. If this is empty, we infer this frmo the package_name and features_profiles_model - for backward compatibility

        Returns:
            Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]: A tuple containing two lists:
                - material_names: A list of tuples containing the names of the feature and label tables.
                - training_dates: A list of tuples containing the corresponding training dates.
        """
        try:
            material_names, training_dates = self.get_material_names_(session, material_table, start_date, end_date, features_profiles_model, model_hash, material_table_prefix, prediction_horizon_days)

            if len(material_names) == 0:
                try:
                    _ = self.generate_training_materials(start_date, package_name, features_profiles_model, prediction_horizon_days, output_filename, site_config_path, project_folder, input_models)
                    material_names, training_dates = self.get_material_names_(session, material_table, start_date, end_date, features_profiles_model, model_hash, material_table_prefix, prediction_horizon_days)
                    if len(material_names) == 0:
                        raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {features_profiles_model} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}. This error means the model is unable to find historic data for training. In the python_model spec, ensure to give the paths to the feature table model correctly in train/inputs and point the same in train/config/data")
                except Exception as e:
                    raise Exception(f"No materialised data found with model_hash {model_hash} in the given date range. Generate {features_profiles_model} for atleast two dates separated by {prediction_horizon_days} days, where the first date is between {start_date} and {end_date}. This error means the model is unable to find historic data for training. In the python_model spec, ensure to give the paths to the feature table model correctly in train/inputs and point the same in train/config/data")
            return material_names, training_dates
        except Exception as e:
            logger.error(e)
            raise Exception("Exception occured while retrieving material names. Please check the logs for more details")
        
    def generate_training_materials(self, 
                                    start_date: str, 
                                    package_name: str, 
                                    features_profiles_model: str, 
                                    prediction_horizon_days: int, 
                                    output_filename: str, 
                                    site_config_path: str, 
                                    project_folder: str, 
                                    input_models: List[str])-> Tuple[str, str]:
        """
        Generates training dataset from start_date and end_date, and fetches the resultant table names from the material_table. 
        Args:
            start_date (str): Start date for training data.
            package_name (str): The name of the package.
            features_profiles_model (str): The name of the model.
            prediction_horizon_days (int): The period of days for prediction horizon.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
            input_models (List[str]): List of input models - relative paths in the profiles project for models that are required to generate the current model. If this is empty, we infer this frmo the package_name and features_profiles_model - for backward compatibility

        Returns:
            Tuple[str, str]: A tuple containing feature table date and label table date strings
        """
        feature_package_path = utils.get_feature_package_path(package_name, features_profiles_model, input_models)
        feature_date = utils.date_add(start_date, prediction_horizon_days)
        label_date = utils.date_add(feature_date, prediction_horizon_days)
        materialise_feature_data = utils.materialise_past_data(feature_date, feature_package_path, output_filename, site_config_path, project_folder)
        materialise_label_data = utils.materialise_past_data(label_date, feature_package_path, output_filename, site_config_path, project_folder)
        if materialise_feature_data and materialise_label_data:
            logger.info(f"Materialised feature and label data successfully, for dates {feature_date} and {label_date}")
        else:
            logger.warning("Failed to materialise feature and label data. Will attempt to fetch materialised data from warehouse registry table")
        return feature_date, label_date
        
    def fetch_training_material_names(self, 
                                      session, 
                                      feature_date,
                                      label_date,
                                      material_registry_table, 
                                      features_profiles_model,
                                      material_table_prefix) -> Tuple[str, str]:
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
                table_name = utils.generate_material_name(material_table_prefix, features_profiles_model, model_hash, str(seq_no))
                if utils.is_valid_table(session, table_name):
                    if dt == feature_date:
                        feature_table_name = table_name
                    else:
                        label_table_name = table_name
                    break
        return feature_table_name, label_table_name                    
        
    def get_latest_material_hash(self, package_name:str, features_profiles_model: str, output_filename:str, 
                                        site_config_path: str = None, inputs: List[str] = None, project_folder: str = None) -> str:
        project_folder = utils.get_project_folder(project_folder, output_filename)
        feature_package_path = utils.get_feature_package_path(package_name, features_profiles_model, inputs)
        pb = utils.get_pb_path()
        args = [pb, "compile", "-p", project_folder, "-m", feature_package_path, "--migrate_on_load=True"]
        if site_config_path is not None:
            args.extend(['-c', site_config_path])
        logger.info(f"Fetching latest model hash by running command: {' '.join(args)}")
        pb_compile_output_response = utils.subprocess_run(args)
        pb_compile_output = (pb_compile_output_response.stdout).lower()
        material_file_prefix = (constants.MATERIAL_TABLE_PREFIX + features_profiles_model + '_').lower()
        model_hash = pb_compile_output[pb_compile_output.index(material_file_prefix) + len(material_file_prefix):].split('_')[0]
        return model_hash

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
    def get_table(self, session, table_name: str, **kwargs):
        pass
    
    @abstractmethod
    def get_table_as_dataframe(self, session, table_name: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_table(self, table, table_name_remote: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def write_pandas(self, df: pd.DataFrame, table_name: str, auto_create_table: bool, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def label_table(self, session,
                    label_table_name: str, label_column: str, entity_column: str, index_timestamp: str,
                    label_value: Union[str,int,float], label_ts_col: str):
        pass

    @abstractmethod
    def save_file(self, session, file_name: str, stage_name: str, overwrite: bool) -> Any:
        pass

    @abstractmethod
    def call_procedure(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def get_non_stringtype_features(self, feature_table, label_column: str, entity_column: str, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def get_stringtype_features(self, feature_table, label_column: str, entity_column: str, **kwargs)-> List[str]:
        pass

    @abstractmethod
    def get_arraytype_features(self, session, table_name: str)-> List[str]:
        pass

    @abstractmethod
    def get_arraytype_features_from_table(self, table: Any, **kwargs)-> list:
        pass

    @abstractmethod
    def get_high_cardinal_features(self, feature_table, label_column, entity_column, cardinal_feature_threshold) -> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns(self, session, table_name: str, index_timestamp: str)-> List[str]:
        pass

    @abstractmethod
    def get_timestamp_columns_from_table(self, session, table_name: str, index_timestamp: str, **kwargs)-> List[str]:
        pass

    @abstractmethod
    def get_default_label_value(self, session, table_name: str, label_column: str, positive_boolean_flags: list):
        pass

    @abstractmethod
    def get_material_names_(self, session, material_table: str, start_time: str, end_time: str, model_name:str, model_hash: str,
                        material_table_prefix:str, prediction_horizon_days: int) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        pass
    
    @abstractmethod
    def get_material_registry_name(self, session, table_prefix: str) -> str:
        pass
    
    @abstractmethod
    def get_creation_ts(self, session, material_table: str, model_name:str, model_hash:str):
        pass

    @abstractmethod
    def fetch_staged_file(self, session, stage_name: str, file_name: str, target_folder: str)-> None:
        pass

    @abstractmethod
    def drop_cols(self, table, col_list: list):
        pass

    @abstractmethod
    def filter_feature_table(self, feature_table, entity_column: str, index_timestamp: str, max_row_count: int, min_sample_for_training: int):
        pass

    @abstractmethod
    def add_days_diff(self, table, new_col, time_col_1, time_col_2):
        pass

    @abstractmethod
    def join_feature_table_label_table(self, feature_table, label_table, entity_column: str):
        pass

    @abstractmethod
    def get_distinct_values_in_column(self, table, column_name: str) -> List:
        pass

    @abstractmethod
    def get_material_registry_table(self, session: Any, material_registry_table_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def generate_type_hint(self, df: Any):        
        pass

    @abstractmethod
    def call_prediction_udf(self, predict_data: Any, prediction_udf: Any, entity_column: str, index_timestamp: str,
                                  score_column_name: str, percentile_column_name: str, output_label_column: str, train_model_id: str,
                                  column_names_path: str, prob_th: float, input: Any) -> pd.DataFrame:
        pass

    @abstractmethod
    def clean_up(self) -> None:
        pass