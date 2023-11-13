import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, f1_score, precision_score, recall_score, accuracy_score, average_precision_score,average_precision_score, PrecisionRecallDisplay, RocCurveDisplay, auc, roc_curve, precision_recall_curve , mean_absolute_error, mean_squared_error,r2_score
from sklearn.model_selection import train_test_split
import numpy as np 
import pandas as pd
from typing import Tuple, List, Union, Dict
from scipy.optimize import minimize_scalar

import snowflake.snowpark
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark.functions import col

import yaml

from datetime import datetime, timedelta, timezone

import os
import gzip
import shutil
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import OneHotEncoder
import shap
import constants as constants
import joblib
import json
import subprocess

from dataclasses import dataclass
from logger import logger

@dataclass
class PreprocessorConfig:
    """PreprocessorConfig class is used to store the preprocessor configuration parameters
    """
    timestamp_columns: List[str]
    ignore_features: List[str]
    numeric_pipeline: dict
    categorical_pipeline: dict
    feature_selectors: dict
    train_size: float
    test_size: float
    val_size: float
            
    
class TrainerUtils:

    evalution_metrics_map_regressor = {
        metric.__name__: metric
        for metric in [mean_absolute_error, mean_squared_error,r2_score]
    }
    

    def get_classification_metrics(self,
                                   y_true: pd.DataFrame, 
                                   y_pred_proba: np.array, 
                                   th: float =0.5) -> dict:
        """Generates classification metrics

        Args:
            y_true (pd.DataFrame): Array of 1s and 0s. True labels
            y_pred_proba (np.array): Array of predicted probabilities
            th (float, optional): thresold for classification. Defaults to 0.5.

        Returns:
            dict: Returns classification metrics in form of a dict for the given thresold
        """
        precision, recall, f1, _ = precision_recall_fscore_support(y_true, np.where(y_pred_proba>th,1,0))
        precision = precision[1]
        recall = recall[1]
        f1 = f1[1]
        roc_auc = roc_auc_score(y_true, y_pred_proba)
        pr_auc = average_precision_score(y_true, y_pred_proba)
        user_count = y_true.shape[0]
        metrics = {"precision": precision, "recall": recall, "f1_score": f1, "roc_auc": roc_auc, 'pr_auc': pr_auc, 'users': user_count}
        return metrics
        
    def get_best_th(self, y_true: pd.DataFrame, y_pred_proba: np.array, metric_to_optimize: str) -> Tuple:
        """This function calculates the thresold that maximizes f1 score based on y_true and y_pred_proba and classication metrics on basis of that.

        Args:
            y_true (pd.DataFrame): Array of 1s and 0s. True labels
            y_pred_proba (np.array): Array of predicted probabilities

        Returns:
            Tuple: Returns the metrics at the threshold and that threshold that maximizes f1 score based on y_true and y_pred_proba
        """
        
        
        metric_functions = {
            'f1_score': f1_score,
            'precision': precision_score,
            'recall': recall_score,
            'accuracy': accuracy_score
        }

        if metric_to_optimize not in metric_functions:
            raise ValueError(f"Unsupported metric: {metric_to_optimize}")

        objective_function = metric_functions[metric_to_optimize]
        objective = lambda th: -objective_function(y_true, np.where(y_pred_proba > th, 1, 0))

        result = minimize_scalar(objective, bounds=(0, 1), method='bounded')
        best_th = result.x                
        best_metrics = self.get_classification_metrics(y_true, y_pred_proba, best_th)
        return best_metrics, best_th
    
    def get_metrics_classifier(self,clf,
                X_train: pd.DataFrame, 
                y_train: pd.DataFrame,
                X_test: pd.DataFrame, 
                y_test: pd.DataFrame,
                X_val: pd.DataFrame, 
                y_val: pd.DataFrame,
                train_config: dict) -> Tuple:
        """Generates classification metrics and predictions for train, validation and test data along with the best probability thresold

        Args:
            clf (_type_): classifier to calculate the classification metrics
            X_train (pd.DataFrame): X_train dataframe
            y_train (pd.DataFrame): y_train dataframe
            X_test (pd.DataFrame): X_test dataframe
            y_test (pd.DataFrame): y_test dataframe
            X_val (pd.DataFrame): X_val dataframe
            y_val (pd.DataFrame): y_val dataframe

        Returns:
            Tuple: Returns the classification metrics and predictions for train, validation and test data along with the best probability thresold.
        """
        train_preds = clf.predict_proba(X_train)[:,1]
        metric_to_optimize = train_config["model_params"]["validation_on"]
        train_metrics, prob_threshold = self.get_best_th(y_train, train_preds,metric_to_optimize)

        test_preds = clf.predict_proba(X_test)[:,1]
        test_metrics = self.get_classification_metrics(y_test, test_preds, prob_threshold)

        val_preds = clf.predict_proba(X_val)[:,1]
        val_metrics = self.get_classification_metrics(y_val, val_preds, prob_threshold)

        metrics = {"train": train_metrics, "val": val_metrics, "test": test_metrics}
        predictions = {"train": train_preds, "val": val_preds, "test": test_preds}
        
        return metrics, predictions, round(prob_threshold, 2)
    
    def get_metrics_regressor(self, model, train_x, train_y, test_x, test_y, val_x, val_y):
        """
        Calculate and return regression metrics for the trained model.

        Args:
            model: The trained regression model.
            train_x (pd.DataFrame): Training data features.
            train_y (pd.DataFrame): Training data labels.
            test_x (pd.DataFrame): Test data features.
            test_y (pd.DataFrame): Test data labels.
            val_x (pd.DataFrame): Validation data features.
            val_y (pd.DataFrame): Validation data labels.
            train_config (dict): Configuration for training.

        Returns:
            result_dict (dict): Dictionary containing regression metrics.
        """
        train_pred = model.predict(train_x)
        test_pred = model.predict(test_x)
        val_pred = model.predict(val_x)

        train_metrics = {}
        test_metrics = {}
        val_metrics = {}

        for metric_name, metric_func in self.evalution_metrics_map_regressor.items():
            train_metrics[metric_name] = float(metric_func(train_y, train_pred))
            test_metrics[metric_name] = float(metric_func(test_y, test_pred))
            val_metrics[metric_name] = float(metric_func(val_y, val_pred))

        metrics = {"train": train_metrics, "val": val_metrics, "test": test_metrics}

        return metrics
    
def split_train_test(feature_df: pd.DataFrame, 
                    label_column: str, 
                    entity_column: str,
                    train_size:float, 
                    val_size: float, 
                    test_size: float,
                    isStratify : bool) -> Tuple:
    """Splits the data in train test and validation according to the their given partition factions.

    Args:
        feature_df (pd.DataFrame): feature table dataframe from the retrieved material_names tuple
        label_column (str): name of label column from feature table
        entity_column (str): name of entity column from feature table
        output_profiles_ml_model (str): output ml model from model_configs file
        train_size (float): partition fraction for train data
        val_size (float): partition fraction for validation data
        test_size (float): partition fraction for test data

    Returns:
        Tuple: returns the train_x, train_y, test_x, test_y, val_x, val_y in form of pd.DataFrame
    """
    feature_df.columns = feature_df.columns.str.upper()
    X_train, X_temp = train_test_split(feature_df, train_size=train_size, random_state=42,stratify=feature_df[label_column.upper()].values if isStratify else None)
    X_val, X_test = train_test_split(X_temp, train_size=val_size/(val_size + test_size), random_state=42,stratify=X_temp[label_column.upper()].values if isStratify else None)
    train_x = X_train.drop([entity_column.upper(), label_column.upper()], axis=1)
    train_y = X_train[[label_column.upper()]]
    val_x = X_val.drop([entity_column.upper(), label_column.upper()], axis=1)
    val_y = X_val[[label_column.upper()]]
    test_x = X_test.drop([entity_column.upper(), label_column.upper()], axis=1)
    test_y = X_test[[label_column.upper()]]
    return train_x, train_y, test_x, test_y, val_x, val_y
    
def remap_credentials(credentials: dict) -> dict:
    """Remaps credentials from profiles siteconfig to the expected format from snowflake session

    Args:
        credentials (dict): Data warehouse credentials from profiles siteconfig

    Returns:
        dict: Data warehouse creadentials remapped in format that is required to create a snowpark session
    """
    new_creds = {k if k != 'dbname' else 'database': v for k, v in credentials.items() if k != 'type'}
    return new_creds

def load_yaml(file_path: str) -> dict:
    """Loads the yaml file for any given filename

    Args:
        file_path (str): Path of the .yaml file that is to be read

    Returns:
        dict: dictionary as key-value pairs from given .yaml file
    """
    with open(file_path, "r") as f:
        data = yaml.safe_load(f)
    return data

def combine_config(notebook_config: dict, profiles_config:dict= None) -> dict:
    """Combine the configs after overwriting values of profiles.yaml in model_configs.yaml

    Args:
        notebook_config (dict): configs from model_configs.yaml file
        profiles_config (dict, optional): configs from profiles.yaml file that should overwrite corresponding values from notebook_config. Defaults to None.

    Returns:
        dict: final merged config
    """
    if not isinstance(profiles_config, dict):
        return notebook_config
    
    merged_config = dict()
    for key in profiles_config:
        if key in notebook_config:
            if isinstance(profiles_config[key], dict) and isinstance(notebook_config[key], dict):
                merged_config[key] = combine_config(notebook_config[key], profiles_config[key])
            elif profiles_config[key] is None:
                merged_config[key] = notebook_config[key]
            else:
                merged_config[key] = profiles_config[key]
        else:
            merged_config[key] = profiles_config[key]

    for key in notebook_config:
        if key not in profiles_config:
            merged_config[key] = notebook_config[key]
    return merged_config

def generate_type_hint(sp_df: snowflake.snowpark.Table):        
    """Returns the type hints for given snowpark DataFrame's fields

    Args:
        sp_df (snowflake.snowpark.Table): snowpark DataFrame

    Returns:
        _type_: Returns the type hints for given snowpark DataFrame's fields
    """
    type_map = {
        T.BooleanType(): float,
        T.DoubleType(): float,
        T.DecimalType(36,6): float,
        T.LongType(): float,
        T.StringType(): str
    }
    types = [type_map[d.datatype] for d in sp_df.schema.fields]
    return T.PandasDataFrame[tuple(types)]

def drop_columns_if_exists(df: snowflake.snowpark.Table, 
                          ignore_features: list) -> snowflake.snowpark.Table:
    """Returns the snowpark DataFrame after dropping the features that are to be ignored.

    Args:
        df (snowflake.snowpark.Table): snowpark DataFrame
        ignore_features (list): list of features that we want to drop from the dataframe

    Returns:
        snowflake.snowpark.Table: snowpark DataFrame after dropping the ignored features
    """
    ignore_features_upper = [col.upper() for col in ignore_features]
    ignore_features_lower = [col.lower() for col in ignore_features]
    ignore_features_ = [col for col in df.columns if col in ignore_features_upper or col in ignore_features_lower]
    return df.drop(ignore_features_)

def delete_import_files(session: snowflake.snowpark.Session, 
                        stage_name: str, 
                        import_paths: List[str]) -> None:
    """
    Deletes files from the specified Snowflake stage that match the filenames extracted from the import paths.
    Args:
        session (snowflake.snowpark.Session): A Snowflake session object.
        stage_name (str): The name of the Snowflake stage.
        import_paths (List[str]): The paths of the files to be deleted from the stage.
    Returns:
        None: The function does not return any value.
    """
    import_files = [element.split('/')[-1] for element in import_paths]
    files = session.sql(f"list {stage_name}").collect()
    for row in files:
        if any(substring in row.name for substring in import_files):
            session.sql(f"remove @{row.name}").collect()

def delete_procedures(session: snowflake.snowpark.Session, train_procedure: str) -> None:
    """
    Deletes Snowflake train procedures based on a given name.

    Args:
        session (snowflake.snowpark.Session): A Snowflake session object.
        train_procedure (str): The name of the train procedures to be deleted.

    Returns:
        None

    Example:
        session = snowflake.snowpark.Session(...)
        delete_procedures(session, 'train_model')

    This function retrieves a list of procedures that match the given train procedure name using a SQL query. 
    It then iterates over each procedure and attempts to drop it using another SQL query. If an error occurs during the drop operation, it throws an exception.
    """
    procedures = session.sql(f"show procedures like '{train_procedure}'").collect()
    for row in procedures:
        try:
            procedure_arguments = row.arguments.split('RETURN')[0].strip()
            session.sql(f"drop procedure if exists {procedure_arguments}").collect()
        except:
            raise Exception(f"Error while deleting procedure {row.name}")

def get_column_names(onehot_encoder: OneHotEncoder, 
                     col_names: List[str]) -> List[str]:
    """Assigning new column names for the one-hot encoded columns.

    Args:
        onehot_encoder (OneHotEncoder): OneHotEncoder object.
        col_names (List[str]): List of categorical column names before applying onehot transformation

    Returns:
        List[str]: List of categorical column names of the output dataframe including categories after applying onehot transformation.
    """
    category_names = []
    for col_id, col in enumerate(col_names):
        for value in onehot_encoder.categories_[col_id]:
            category_names.append(f"{col}_{value}")
    return category_names

def get_non_stringtype_features(feature_table: snowflake.snowpark.Table, label_column: str, entity_column: str) -> List[str]:
    """
    Returns a list of strings representing the names of the Non-StringType(non-categorical) columns in the feature table.

    Args:
        feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
        label_column (str): A string representing the name of the label column.
        entity_column (str): A string representing the name of the entity column.

    Returns:
        List[str]: A list of strings representing the names of the non-StringType columns in the feature table.
    """
    non_stringtype_features = []
    for field in feature_table.schema.fields:
        if field.datatype != T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
            non_stringtype_features.append(field.name)
    return non_stringtype_features

def get_stringtype_features(feature_table: snowflake.snowpark.Table, label_column: str, entity_column: str)-> List[str]:
    """
    Extracts the names of StringType(categorical) columns from a given feature table schema.

    Args:
        feature_table (snowflake.snowpark.Table): A feature table object from the `snowflake.snowpark.Table` class.
        label_column (str): The name of the label column.
        entity_column (str): The name of the entity column.

    Returns:
        List[str]: A list of StringType(categorical) column names extracted from the feature table schema.
    """
    stringtype_features = []
    for field in feature_table.schema.fields:
        if field.datatype == T.StringType() and field.name.lower() not in (label_column.lower(), entity_column.lower()):
            stringtype_features.append(field.name)
    return stringtype_features

def get_arraytype_features(table: snowflake.snowpark.Table)-> list:
    """Returns the list of features to be ignored from the feature table.
    Args:
        table (snowflake.snowpark.Table): snowpark table.
    Returns:
        list: The list of features to be ignored based column datatypes as ArrayType.
    """
    arraytype_features = [row.name for row in table.schema.fields if row.datatype == T.ArrayType()]
    return arraytype_features

def transform_null(df: pd.DataFrame, numeric_columns: List[str], categorical_columns: List[str])-> pd.DataFrame:
    """
    Replaces the pd.NA values in the numeric and categorical columns of a pandas DataFrame with np.nan and None, respectively.

    Args:
        df (pd.DataFrame): The pandas DataFrame.
        numeric_columns (List[str]): A list of column names that contain numeric values.
        categorical_columns (List[str]): A list of column names that contain categorical values.

    Returns:
        pd.DataFrame: The transformed DataFrame with pd.NA values replaced by np.nan in numeric columns and None in categorical columns.
    """
    df[numeric_columns] = df[numeric_columns].replace({pd.NA: np.nan})
    df[categorical_columns] = df[categorical_columns].replace({pd.NA: None})
    return df

def get_material_registry_name(session: snowflake.snowpark.Session, table_prefix: str='MATERIAL_REGISTRY') -> str:
    """This function will return the latest material registry table name
    Args:
        session (snowflake.snowpark.Session): snowpark session
        table_name (str): name of the material registry table prefix
    Returns:
        str: latest material registry table name
    """
    material_registry_tables = list()

    def split_key(item):
        parts = item.split('_')
        if len(parts) > 1 and parts[-1].isdigit():
            return int(parts[-1])
        return 0

    registry_df = session.sql(f"show tables starts with '{table_prefix}'")
    for row in registry_df.collect():
        material_registry_tables.append(row.name)
    material_registry_tables.sort(reverse=True)
    sorted_material_registry_tables = sorted(material_registry_tables, key=split_key, reverse=True)
    return sorted_material_registry_tables[0]

def get_output_directory(folder_path: str)-> str:
    """This function will return the output directory path

    Args:
        folder_path (str): path of the folder where output directory will be created

    Returns:
        str: output directory path
    """
    file_list = [file for file in os.listdir(folder_path) if file.endswith('.py')]
    if file_list == []:
        latest_filename = "train"
    else:
        files_creation_ts = [os.path.getctime(os.path.join(folder_path, file)) for file in file_list]
        latest_filename = file_list[int(np.array(files_creation_ts).argmax())]

    materialized_folder = os.path.splitext(latest_filename)[0]
    target_path = Path(os.path.join(folder_path, f"{materialized_folder}_reports"))
    target_path.mkdir(parents=True, exist_ok=True)
    return str(target_path)

def get_date_range(creation_ts: datetime, 
                   prediction_horizon_days: int) -> Tuple:
    """This function will return the start_date and end_date on basis of latest hash

    Args:
        end_date (datetime): creation timestamp of latest hash
        prediction_horizon_days (int): period of days

    Returns:
        Tuple: start_date and end_date on basis of latest hash and period of days
    """
    start_date = creation_ts - timedelta(days = 2*prediction_horizon_days)
    end_date = creation_ts - timedelta(days = prediction_horizon_days)
    if isinstance(start_date, datetime):
        start_date = start_date.date()
        end_date = end_date.date()
    return str(start_date), str(end_date)

def date_add(reference_date: str, add_days: int) -> str:
    """
    Adds the horizon days to the reference date and returns the new date as a string.

    Args:
        reference_date (str): The Reference date in the format "YYYY-MM-DD".
        add_days (int): The number of days to add to the reference date.

    Returns:
        str: The new date is returned as a string in the format "YYYY-MM-DD".
    """
    new_timestamp = datetime.strptime(reference_date, "%Y-%m-%d") + timedelta(days=add_days)
    new_date = new_timestamp.strftime("%Y-%m-%d")
    return new_date

def get_timestamp_columns(table: snowflake.snowpark.Table, index_timestamp: str)-> List[str]:
    """
    Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.
    Args:
        session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
        feature_table (snowflake.snowpark.Table): The feature table from which to retrieve the timestamp columns.
        index_timestamp (str): The name of the column containing the index timestamp information.
    Returns:
        List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
    """
    timestamp_columns = []
    for field in table.schema.fields:
        if field.datatype in [T.TimestampType(), T.DateType(), T.TimeType()] and field.name.lower() != index_timestamp.lower():
            timestamp_columns.append(field.name)
    return timestamp_columns

def get_latest_material_hash(session: snowflake.snowpark.Session,
                       material_table: str,
                       features_profiles_model:str) -> Tuple:
    """This function will return the model hash that is latest for given model name in material table

    Args:
        session (snowflake.snowpark.Session): snowpark session
        material_table (str): name of material registry table
        features_profiles_model (str): feature profiles model name from model_configs file

    Returns:
        Tuple: latest model hash and it's creation timestamp
    """
    snowpark_df = get_material_registry_table(session, material_table)
    temp_hash_vector = snowpark_df.filter(col("model_name") == features_profiles_model).sort(col("creation_ts"), ascending=False).select(col("model_hash"), col("creation_ts")).collect()[0]
    model_hash = temp_hash_vector.MODEL_HASH
    creation_ts = temp_hash_vector.CREATION_TS
    return model_hash, creation_ts

def merge_lists_to_unique(l1: list, l2: list)-> list:
    """Merges two lists and returns a unique list of elements.

    Args:
        l1 (list): The first list.
        l2 (list): The second list.

    Returns:
        list: A unique list of elements from both the lists.
    """
    return list(set(l1 + l2))

def get_pb_path() -> str:
    """In Rudder-sources check if pb command works. Else, it returns the exact location where pb installable is present.

    Returns:
        str: _description_
    """
    try:
        _ = subprocess.check_output(["which", "pb"])
        return "pb"
    except:
        logger.warning("pb command not found in the path. Using the default rudder-sources path /venv/bin/pb")
        return constants.PB
    

def subprocess_run(args):
    response = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if response.returncode == 0:
        return True
    else:
        logger.warning("Error occurred. Exit code:", response.returncode)
        logger.warning("Standard Output:\n", response.stdout)
        logger.warning("Standard Error:\n", response.stderr)
        return False
    
def materialise_past_data(features_valid_time: str, feature_package_path: str, output_path: str, site_config_path: str, project_folder: str)-> None:
    """
    Materializes past data for a given date using the 'pb' command-line tool.

    Args:
        features_valid_time (str): The date for which the past data needs to be materialized.
        feature_package_path (str): The path to the feature package.
        site_config_path (str): path to the siteconfig.yaml file
        project_folder (str): project folder path to pb_project.yaml file

    Returns:
        None.

    Example Usage:
        materialise_past_data("2022-01-01", "packages/feature_table/models/shopify_user_features", "output/path")
    """
    try:
        features_valid_time_unix = int(datetime.strptime(features_valid_time, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        if project_folder is None:
            path_components = output_path.split(os.path.sep)
            output_index = path_components.index('output')
            project_folder = os.path.sep.join(path_components[:output_index])
        pb = get_pb_path()
        args = [pb, "run", "-p", project_folder, "-m", feature_package_path, "--migrate_on_load=True", "--end_time", str(features_valid_time_unix)]
        if site_config_path is not None:
            args.extend(['-c', site_config_path])
        logger.info(f"Materialising historic data for {features_valid_time} using pb: {' '.join(args)} ")
        pb_run_for_past_data = subprocess_run(args)
        if pb_run_for_past_data:
            logger.info(f"Successfully materialised data for date {features_valid_time} ")
        else:
            raise Exception(f"Error occurred while materialising data for date {features_valid_time} ")
    except Exception as e:
        logger.error(f"Exception occured while materialising data for date {features_valid_time} ")
        print(e)

def is_valid_table(session: snowflake.snowpark.Session, table_name: str) -> bool:
    """
    Checks whether a table exists in the data warehouse.

    Args:
        session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
        table_name (str): The name of the table to be checked.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        session.sql(f"select * from {table_name} limit 1").collect()
        return True
    except:
        return False
    
def get_material_registry_table(session: snowflake.snowpark.Session, material_registry_table_name: str) -> snowflake.snowpark.Table:
    """Fetches and filters the material registry table to get only the successful runs. It assumes that the successful runs have a status of 2.
    Currently profiles creates a row at the start of a run with status 1 and creates a new row with status to 2 at the end of the run.

    Args:
        session (snowflake.snowpark.Session): A Snowpark session for data warehouse access.
        material_registry_table_name (str): The material registry table name.

    Returns:
        snowflake.snowpark.Table: The filtered material registry table containing only the successfully materialized data.
    """
    material_registry_table = (session.table(material_registry_table_name)
                               .withColumn("status", F.get_path("metadata", F.lit("complete.status")))
                               .filter(F.col("status")==2)
                               )
    return material_registry_table


def generate_material_name(material_table_prefix: str, model_name: str, model_hash: str, seq_no: str) -> str:
    """Generates a valid table name from the model hash, model name, and seq no. 

    Args:
        material_table_prefix (str): a standard prefix defined in constants.py, common for all the material tables
        model_name (str): name of the profiles model, defined in profiles project
        model_hash (str): hash of the model, generated by profiles
        seq_no (str): sequence number of the material table - determines the timestamp of the material table

    Returns:
        str: name of the material table in warehouse 
    """
    return f'{material_table_prefix}{model_name}_{model_hash}_{seq_no}'


def plot_roc_auc_curve(pipe, test_x, test_y, roc_auc_file, label_column)-> None:
    """
    Plots the ROC curve and calculates the Area Under the Curve (AUC) for a given classifier model.

    Parameters:
    session (object): The session object that provides access to the file storage.
    pipe (object): The trained pipeline model.
    stage_name (str): The name of the stage.
    test_x (array-like): The test data features.
    test_y (array-like): The test data labels.
    chart_name (str): The name of the chart.
    label_column (str): The name of the label column in the test data.

    Returns:
    None. The function does not return any value. The generated ROC curve plot is saved as an image file and uploaded to the session's file storage.
    """
    fpr, tpr, _ = roc_curve(test_y[label_column.upper()].values, pipe.predict_proba(test_x)[:,1])
    roc_auc = auc(fpr, tpr)
    sns.set(style="ticks",  context='notebook')
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, color="b", label=f"ROC AUC = {roc_auc:.2f}", linewidth=2)
    plt.plot([0, 1], [0, 1], color="gray", linestyle="--", linewidth=2)
    plt.title("ROC Curve (Test Data)")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.legend(loc="lower right")
    sns.despine()
    plt.grid(True)
    plt.savefig(roc_auc_file)
    plt.clf()

def plot_pr_auc_curve(pipe, test_x, test_y, pr_auc_file, label_column)-> None:
    """
    Plots a precision-recall curve and saves it as a file.

    Args:
        session (object): A session object used to upload the plot file.
        pipe (object): A pipeline object used to predict probabilities.
        stage_name (str): The name of the stage where the plot file will be uploaded.
        test_x (array-like): The test data features.
        test_y (array-like): The test data labels.
        chart_name (str): The name of the plot file.
        label_column (str): The column name of the label in the test data.

    Returns:
        None. The function only saves the precision-recall curve plot as a file.
    """
    precision, recall, _ = precision_recall_curve(test_y[label_column.upper()].values, pipe.predict_proba(test_x)[:,1])
    pr_auc = auc(recall, precision)
    sns.set(style="ticks",  context='notebook')
    plt.figure(figsize=(8, 6))
    plt.plot(recall, precision, color="b", label=f"PR AUC = {pr_auc:.2f}", linewidth=2)
    plt.ylim([int(min(precision)*20)/20, 1.0])
    plt.xlim([int(min(recall)*20)/20, 1.0])
    plt.title("Precision-Recall Curve (Test data)")
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.legend(loc="lower left")
    sns.despine()
    plt.grid(True)
    plt.savefig(pr_auc_file)
    plt.clf()

def plot_lift_chart(pipe, test_x, test_y, lift_chart_file, label_column)-> None:
    """
    Generates a lift chart for a binary classification model.

    Args:
        session (object): The session object used to save the chart file.
        pipe (object): The trained model pipeline.
        stage_name (string): The name of the stage where the chart will be saved.
        test_x (DataFrame): The test data features.
        test_y (DataFrame): The test data labels.
        chart_name (string): The name of the chart file.
        label_column (string): The column name of the label in the test data.

    Returns:
        None. The function does not return any value, but it saves the lift chart as an image file in the specified location.
    """
    data = pd.DataFrame()
    data['label'] = test_y[label_column.upper()].values
    data['pred'] = pipe.predict_proba(test_x)[:,1]

    sorted_indices = np.argsort(data["pred"].values, kind="heapsort")[::-1]
    cumulative_actual = np.cumsum(data["label"][sorted_indices].values)
    cumulative_percentage = np.linspace(0, 1, len(cumulative_actual)+1)

    sns.set(style="ticks", context='notebook')
    plt.figure(figsize=(8, 6))
    sns.lineplot(x=cumulative_percentage*100, 
                y=np.array([0] + list(100*cumulative_actual / cumulative_actual[-1])), 
                linewidth=2, color="b", label="Model Lift curve")
    sns.despine()
    plt.plot([0, 100*data["label"].mean()], [0, 100], color="red", linestyle="--", label="Best Case", linewidth=1.5)
    plt.plot([0, 100], [0, 100], color="black", linestyle="--", label="Baseline", linewidth=1.5)

    plt.title("Cumulative Gain Curve")
    plt.xlabel("Percentage of Data Targeted")
    plt.ylabel("Percent of target users covered")
    plt.ylim([0, 100])
    plt.xlim([0, 100])
    plt.legend()
    plt.grid(True)
    plt.savefig(lift_chart_file)
    plt.clf()

def plot_top_k_feature_importance(pipe, train_x, numeric_columns, categorical_columns, figure_file, top_k_features=5)-> pd.DataFrame:
    """
    Generates a bar chart to visualize the top k important features in a machine learning model.

    Args:
        session (object): The session object used for writing the feature importance values and saving the chart image.
        pipe (object): The pipeline object containing the preprocessor and model.
        stage_name (str): The name of the stage where the chart image will be saved.
        train_x (array-like): The input data used for calculating the feature importance values.
        numeric_columns (list): The list of column names for numeric features.
        categorical_columns (list): The list of column names for categorical features.
        chart_name (str): The name of the chart image file.
        top_k_features (int, optional): The number of top important features to display in the chart. Default is 5.

    Returns:
        None. The function generates a bar chart and writes the feature importance values to a table in the session.
    """
    try:
        train_x_processed = pipe['preprocessor'].transform(train_x)
        train_x_processed = train_x_processed.astype(np.int_)
        shap_values = shap.TreeExplainer(pipe['model']).shap_values(train_x_processed)
        x_label = "Importance scores"
        if isinstance(shap_values, list):
            print("Got List output, suggesting that the model is a multi-output model. Using the second output for plotting feature importance")
            x_label = "Importance scores of positive label"
            shap_values = shap_values[1]
        onehot_encoder_columns = get_column_names(dict(pipe.steps)["preprocessor"].transformers_[1][1].named_steps["encoder"], categorical_columns)
        col_names_ = numeric_columns + onehot_encoder_columns + [col for col in list(train_x) if col not in numeric_columns and col not in categorical_columns]

        shap_df = pd.DataFrame(shap_values, columns=col_names_)
        vals = np.abs(shap_df.values).mean(0)
        feature_names = shap_df.columns
        shap_importance = pd.DataFrame(data = vals, index = feature_names, columns = ["feature_importance_vals"])
        shap_importance.sort_values(by=['feature_importance_vals'],  ascending=False, inplace=True)
        
        ax = shap_importance[:top_k_features][::-1].plot(kind='barh', figsize=(8, 6), color='#86bf91', width=0.3)
        ax.set_xlabel(x_label)
        ax.set_ylabel("Feature Name")
        plt.title(f"Top {top_k_features} Important Features")
        plt.savefig(figure_file, bbox_inches="tight")
        plt.clf()
        return shap_importance
    except Exception as e:
        print("Exception occured while plotting feature importance")
        print(e)

def fetch_staged_file(session: snowflake.snowpark.Session, 
                        stage_name: str, 
                        file_name: str, 
                        target_folder: str)-> None:
    """
    Fetches a file from a Snowflake stage and saves it to a local target folder.

    Args:
        session (snowflake.snowpark.Session): The Snowflake session object used to connect to the Snowflake account.
        stage_name (str): The name of the Snowflake stage where the file is located.
        file_name (str): The name of the file to fetch from the stage.
        target_folder (str): The local folder where the fetched file will be saved.

    Returns:
        None
    """
    file_stage_path = f"{stage_name}/{file_name}"
    _ = session.file.get(file_stage_path, target_folder)
    input_file_path = os.path.join(target_folder, f"{file_name}.gz")
    output_file_path = os.path.join(target_folder, file_name)

    with gzip.open(input_file_path, 'rb') as gz_file:
        with open(output_file_path, 'wb') as target_file:
            shutil.copyfileobj(gz_file, target_file)
    os.remove(input_file_path)

def load_stage_file_from_local(session: snowflake.snowpark.Session, stage_name: str, 
                               file_name: str, target_folder: str, filetype: str):
    """
    Fetches a file from a Snowflake stage to local, loads it into memory and delete that file from local.

    Args:
        session (snowflake.snowpark.Session): The Snowflake session object used to connect to the Snowflake account.
        stage_name (str): The name of the Snowflake stage where the file is located.
        file_name (str): The name of the file to fetch from the stage.
        target_folder (str): The local folder where the fetched file will be saved.
        filetype (str): The type of the file to load ('json' or 'joblib').

    Returns:
        The loaded file object, either as a JSON object or a joblib object, depending on the `filetype`.
    """
    fetch_staged_file(session, stage_name, file_name, target_folder)
    if filetype == 'json':
        f = open(os.path.join(target_folder, file_name), "r")
        output_file = json.load(f)
    elif filetype == 'joblib':
        output_file = joblib.load(os.path.join(target_folder, file_name))
    os.remove(os.path.join(target_folder, file_name))
    return output_file
