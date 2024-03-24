from functools import reduce
import math
import re
import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)

from sklearn.metrics import (
    precision_recall_fscore_support,
    roc_auc_score,
    f1_score,
    precision_score,
    recall_score,
    accuracy_score,
    average_precision_score,
    average_precision_score,
    auc,
    roc_curve,
    precision_recall_curve,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)

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
from ..utils import constants
import joblib
import json
import subprocess

from dataclasses import dataclass
from ..utils.logger import logger


@dataclass
class PreprocessorConfig:
    """PreprocessorConfig class is used to store the preprocessor configuration parameters"""

    timestamp_columns: List[str]
    ignore_features: List[str]
    numeric_pipeline: dict
    categorical_pipeline: dict
    feature_selectors: dict
    train_size: float
    test_size: float
    val_size: float


@dataclass
class OutputsConfig:
    """OutputsConfig class is used to store the outputs configuration parameters"""

    column_names: dict
    feature_meta_data: List[dict]


class TrainerUtils:
    evalution_metrics_map_regressor = {
        metric.__name__: metric
        for metric in [mean_absolute_error, mean_squared_error, r2_score]
    }

    def get_classification_metrics(
        self,
        y_true: pd.DataFrame,
        y_pred_proba: np.array,
        th: float = 0.5,
        recall_to_precision_importance: float = 1.0,
    ) -> dict:
        """Generates classification metrics

        Args:
            y_true (pd.DataFrame): Array of 1s and 0s. True labels
            y_pred_proba (np.array): Array of predicted probabilities
            th (float, optional): thresold for classification. Defaults to 0.5.
            recall_to_precision_importance (float, optional): Importance of recall to precision. Defaults to 1.0

        Returns:
            dict: Returns classification metrics in form of a dict for the given thresold
        """
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true,
            np.where(y_pred_proba > th, 1, 0),
            beta=recall_to_precision_importance,
        )
        precision = precision[1]
        recall = recall[1]
        f1 = f1[1]
        roc_auc = roc_auc_score(y_true, y_pred_proba)
        pr_auc = average_precision_score(y_true, y_pred_proba)
        user_count = y_true.shape[0]
        metrics = {
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "roc_auc": roc_auc,
            "pr_auc": pr_auc,
            "users": user_count,
        }
        return metrics

    def get_best_th(
        self,
        y_true: pd.DataFrame,
        y_pred_proba: np.array,
        metric_to_optimize: str,
        recall_to_precision_importance: float = 1.0,
    ) -> Tuple:
        """This function calculates the thresold that maximizes f1 score based on y_true and y_pred_proba
        and classication metrics on basis of that.

        Args:
            y_true (pd.DataFrame): Array of 1s and 0s. True labels
            y_pred_proba (np.array): Array of predicted probabilities
            recall_to_precision_importance (float, optional): Importance of recall to precision.

        Returns:
            Tuple: Returns the metrics at the threshold and that threshold that maximizes f1 score
                based on y_true and y_pred_proba
        """

        metric_functions = {
            "f1_score": f1_score,
            "precision": precision_score,
            "recall": recall_score,
            "accuracy": accuracy_score,
        }

        if metric_to_optimize not in metric_functions:
            raise ValueError(f"Unsupported metric: {metric_to_optimize}")

        objective_function = metric_functions[metric_to_optimize]
        objective = lambda th: -objective_function(
            y_true, np.where(y_pred_proba > th, 1, 0)
        )

        result = minimize_scalar(objective, bounds=(0, 1), method="bounded")
        best_th = result.x
        best_metrics = self.get_classification_metrics(
            y_true, y_pred_proba, best_th, recall_to_precision_importance
        )
        return best_metrics, best_th

    def get_metrics_classifier(
        self,
        clf,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_test: pd.DataFrame,
        y_test: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        train_config: dict,
        recall_to_precision_importance: float = 1.0,
    ) -> Tuple:
        """Generates classification metrics and predictions for train, \
            validation and test data along with the best probability thresold

        Args:
            clf (_type_): classifier to calculate the classification metrics
            X_train (pd.DataFrame): X_train dataframe
            y_train (pd.DataFrame): y_train dataframe
            X_test (pd.DataFrame): X_test dataframe
            y_test (pd.DataFrame): y_test dataframe
            X_val (pd.DataFrame): X_val dataframe
            y_val (pd.DataFrame): y_val dataframe
            recall_to_precision_importance (float, optional): Importance of recall to precision. Defaults to 1.0

        Returns:
            Tuple: Returns the classification metrics and predictions for train, \
                validation and test data along with the best probability thresold.
        """
        train_preds = clf.predict_proba(X_train)[:, 1]
        metric_to_optimize = train_config["model_params"]["validation_on"]
        train_metrics, prob_threshold = self.get_best_th(
            y_train, train_preds, metric_to_optimize, recall_to_precision_importance
        )

        test_preds = clf.predict_proba(X_test)[:, 1]
        test_metrics = self.get_classification_metrics(
            y_test, test_preds, prob_threshold, recall_to_precision_importance
        )

        val_preds = clf.predict_proba(X_val)[:, 1]
        val_metrics = self.get_classification_metrics(
            y_val, val_preds, prob_threshold, recall_to_precision_importance
        )

        metrics = {"train": train_metrics, "val": val_metrics, "test": test_metrics}
        predictions = {"train": train_preds, "val": val_preds, "test": test_preds}

        return metrics, predictions, round(prob_threshold, 2)

    def get_metrics_regressor(
        self, model, train_x, train_y, test_x, test_y, val_x, val_y
    ):
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


def split_train_test(
    feature_df: pd.DataFrame,
    label_column: str,
    entity_column: str,
    train_size: float,
    val_size: float,
    test_size: float,
    isStratify: bool,
) -> Tuple:
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
    X_train, X_temp = train_test_split(
        feature_df,
        train_size=train_size,
        random_state=42,
        stratify=feature_df[label_column.upper()].values if isStratify else None,
    )
    X_val, X_test = train_test_split(
        X_temp,
        train_size=val_size / (val_size + test_size),
        random_state=42,
        stratify=X_temp[label_column.upper()].values if isStratify else None,
    )
    train_x = X_train.drop([entity_column.upper(), label_column.upper()], axis=1)
    train_y = X_train[[label_column.upper()]]
    val_x = X_val.drop([entity_column.upper(), label_column.upper()], axis=1)
    val_y = X_val[[label_column.upper()]]
    test_x = X_test.drop([entity_column.upper(), label_column.upper()], axis=1)
    test_y = X_test[[label_column.upper()]]
    return train_x, train_y, test_x, test_y, val_x, val_y


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


def combine_config(default_config: dict, profiles_config: dict = None) -> dict:
    """Combine the configs after overwriting values of profiles.yaml in model_configs.yaml

    Args:
        default_config (dict): configs from model_configs.yaml file
        profiles_config (dict, optional): configs from profiles.yaml file that should overwrite corresponding values from notebook_config. Defaults to None.

    Returns:
        dict: final merged config
    """
    if not isinstance(profiles_config, dict):
        return default_config

    merged_config = dict()
    for key in profiles_config:
        if key in default_config:
            if isinstance(profiles_config[key], dict) and isinstance(
                default_config[key], dict
            ):
                merged_config[key] = combine_config(
                    default_config[key], profiles_config[key]
                )
            elif profiles_config[key] is None:
                merged_config[key] = default_config[key]
            else:
                merged_config[key] = profiles_config[key]
        else:
            merged_config[key] = profiles_config[key]

    for key in default_config:
        if key not in profiles_config:
            merged_config[key] = default_config[key]
    return merged_config


def get_feature_table_column_types(
    feature_table,
    input_column_types: dict,
    label_column: str,
    entity_column: str,
):
    feature_table_column_types = {"numeric": [], "categorical": [], "arraytype": []}
    uppercase_columns = lambda columns: [col.upper() for col in columns]

    upper_numeric_input_cols = uppercase_columns(input_column_types["numeric"])
    upper_timestamp_input_cols = uppercase_columns(input_column_types["timestamp"])
    upper_feature_table_numeric_cols = merge_lists_to_unique(
        upper_numeric_input_cols, upper_timestamp_input_cols
    )

    for col in feature_table.columns:
        if col.upper() in (label_column.upper(), entity_column.upper()):
            continue
        elif col.upper() in upper_feature_table_numeric_cols:
            feature_table_column_types["numeric"].append(col.upper())
        elif col.upper() in uppercase_columns(input_column_types["categorical"]):
            feature_table_column_types["categorical"].append(col.upper())
        elif col.upper() in uppercase_columns(input_column_types["arraytype"]):
            feature_table_column_types["arraytype"].append(col.upper())
        else:
            raise Exception(
                f"Column {col.upper()} in feature table is not numeric or categorical"
            )
    return feature_table_column_types


def transform_arraytype_features(feature_table, arraytype_features):
    transformed_feature_table = feature_table
    group_by_cols = [
        col for col in feature_table.columns if col.lower() not in arraytype_features
    ]

    transformed_dfs = []

    for array_col_name in arraytype_features:
        exploded_df = feature_table.select(
            *group_by_cols, F.explode(array_col_name).alias("ARRAY_VALUE")
        )
        grouped_df = exploded_df.groupBy(*exploded_df.columns).count()

        unique_values = [
            row["ARRAY_VALUE"].strip('"')
            for row in grouped_df.select("ARRAY_VALUE").distinct().collect()
        ]
        new_array_feature_col_names = [
            f"{array_col_name}_{value}" for value in unique_values
        ]

        columns_to_remove = ["COUNT", "ARRAY_VALUE"]
        grouped_df_cols = [
            col for col in grouped_df.columns if col not in columns_to_remove
        ]
        pivoted_df = (
            grouped_df.groupBy(grouped_df_cols)
            .pivot("ARRAY_VALUE", unique_values)
            .sum("COUNT")
            .na.fill(0)
        )

        for old_name, new_name in zip(unique_values, new_array_feature_col_names):
            pivoted_df = pivoted_df.withColumnRenamed(f"'{old_name}'", new_name)

        transformed_dfs.append(pivoted_df)

    if transformed_dfs:
        # Apply reduce to join DataFrames
        transformed_feature_table = reduce(
            lambda df1, df2: df1.join(df2, on=group_by_cols, how="inner"),
            transformed_dfs,
        )

    return transformed_feature_table


def get_all_ignore_features(
    feature_table, input_column_types, config_ignore_features, high_cardinal_features
):
    ignore_features_ = merge_lists_to_unique(
        high_cardinal_features, config_ignore_features
    )

    uppercase_list = lambda names: [name.upper() for name in names]
    lowercase_list = lambda names: [name.lower() for name in names]

    ignore_features = [
        col
        for col in feature_table.columns
        if col in uppercase_list(ignore_features_)
        or col in lowercase_list(ignore_features_)
    ]
    return ignore_features


def parse_warehouse_creds(creds: dict, mode: str) -> dict:
    if mode == constants.K8S_MODE:
        wh_creds_str = os.environ[constants.K8S_WH_CREDS_KEY]
        wh_creds = json.loads(wh_creds_str)
    else:
        wh_creds = creds
    return wh_creds


def convert_ts_str_to_dt_str(timestamp_str: str) -> str:
    try:
        if "+" in timestamp_str:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S%z")
        elif " " in timestamp_str:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        else:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d")
        string_date = timestamp.strftime("%Y-%m-%d")
        return string_date
    except Exception as e:
        logger.error(f"Error occurred while converting timestamp to date string: {e}")
        raise Exception(
            f"Error occurred while converting timestamp to date string: {e}"
        )


def get_column_names(onehot_encoder: OneHotEncoder, col_names: List[str]) -> List[str]:
    """Assigning new column names for the one-hot encoded columns.

    Args:
        onehot_encoder (OneHotEncoder): OneHotEncoder object.
        col_names (List[str]): List of categorical column names before applying onehot transformation

    Returns:
        List[str]: List of categorical column names of the output dataframe \
            including categories after applying onehot transformation.
    """
    category_names = []
    for col_id, col in enumerate(col_names):
        for value in onehot_encoder.categories_[col_id]:
            category_names.append(f"{col}_{value}")
    return category_names


def transform_null(
    df: pd.DataFrame, numeric_columns: List[str], categorical_columns: List[str]
) -> pd.DataFrame:
    """
    Replaces the pd.NA values in the numeric and categorical columns of a pandas DataFrame with np.nan and None, respectively.

    Args:
        df (pd.DataFrame): The pandas DataFrame.
        numeric_columns (List[str]): A list of column names that contain numeric values.
        categorical_columns (List[str]): A list of column names that contain categorical values.

    Returns:
        pd.DataFrame: The transformed DataFrame with pd.NA values replaced by np.nan in numeric columns and \
            None in categorical columns.
    """
    df[numeric_columns] = df[numeric_columns].replace({pd.NA: np.nan})
    df[categorical_columns] = df[categorical_columns].replace({pd.NA: None})
    return df


def get_output_directory(folder_path: str) -> str:
    """This function will return the output directory path

    Args:
        folder_path (str): path of the folder where output directory will be created

    Returns:
        str: output directory path
    """
    file_list = [file for file in os.listdir(folder_path) if file.endswith(".py")]
    if file_list == []:
        latest_filename = "train"
    else:
        files_creation_ts = [
            os.path.getctime(os.path.join(folder_path, file)) for file in file_list
        ]
        latest_filename = file_list[int(np.array(files_creation_ts).argmax())]

    materialized_folder = os.path.splitext(latest_filename)[0]
    target_path = Path(os.path.join(folder_path, f"{materialized_folder}_reports"))
    target_path.mkdir(parents=True, exist_ok=True)
    return str(target_path)


def delete_file(file_path: str) -> None:
    """
    Delete a file.
    """
    try:
        os.remove(file_path)
        logger.info(f"File '{file_path}' deleted successfully from local.")
    except FileNotFoundError:
        logger.error(f"Error: File '{file_path}' not found.")
    except PermissionError:
        logger.error(f"Error: Permission denied. Unable to delete '{file_path}'.")
    except OSError as e:
        logger.error(f"Error occurred while deleting file '{file_path}': {e}")


def delete_folder(folder_path: str) -> None:
    """
    Delete a folder and its contents recursively.
    Parameters:
        folder_path (str): The path of the folder to be deleted.
    Returns:
        None: The function does not return any value.
    Raises:
        FileNotFoundError: If the specified folder does not exist.
        PermissionError: If there are permission issues while deleting the folder.
        OSError: If an error occurs during the deletion process.
    Example:
        delete_folder("path/to/your/folder")
    """
    try:
        shutil.rmtree(folder_path)
        logger.info(f"Folder '{folder_path}' deleted successfully from local.")
    except FileNotFoundError:
        logger.error(f"Error: Folder '{folder_path}' not found.")
    except PermissionError:
        logger.error(f"Error: Permission denied. Unable to delete '{folder_path}'.")
    except OSError as e:
        logger.error(f"Error occurred while deleting folder '{folder_path}': {e}")


def get_date_range(creation_ts: datetime, prediction_horizon_days: int) -> Tuple:
    """This function will return the start_date and end_date on basis of latest hash

    Args:
        end_date (datetime): creation timestamp of latest hash
        prediction_horizon_days (int): period of days

    Returns:
        Tuple: start_date and end_date on basis of latest hash and period of days
    """
    start_date = creation_ts - timedelta(days=2 * prediction_horizon_days)
    end_date = creation_ts - timedelta(days=prediction_horizon_days)
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
    new_timestamp = datetime.strptime(reference_date, "%Y-%m-%d") + timedelta(
        days=add_days
    )
    new_date = new_timestamp.strftime("%Y-%m-%d")
    return new_date


def get_abs_date_diff(ref_date1: str, ref_date2: str) -> int:
    """
    For given two dates in string format, it will retrun the difference in days
    Args:
        ref_date1: Reference date1
        ref_date2: Reference date2
    Returns:
        int: Difference in number of dates
    """
    d1 = datetime.strptime(ref_date1, "%Y-%m-%d")
    d2 = datetime.strptime(ref_date2, "%Y-%m-%d")
    diff = d2 - d1
    return abs(diff.days)


def dates_proximity_check(reference_date: str, dates: list, distance: int) -> bool:
    """
    For given reference date and list of training dates, it will check
    whether a given date(reference_date) is farther from every date in the dates list, by minimum "distance" days

    Args:
        reference_date: Given reference date
        dates: List of dates to be checked with
        distance: Difference distance
    Returns:
        bool: Wether given date is passing the proximity check
    """
    for d in dates:
        if get_abs_date_diff(reference_date, d) < distance:
            return False
    return True


def datetime_to_date_string(datetime_str: str) -> str:
    """
    Converts datetime sting to date string

    Args:
        datetime_str: Datetime in string format
    Returns:
        str: Date in string format
    """
    try:
        if "+" in datetime_str:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S%z")
        elif " " in datetime_str:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        else:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    except ValueError:
        # Value error will be raised if its not able
        # to match datetime string with given format
        # In this case datetime sting is in "%Y-%m-%d" format
        # and returning empty string
        logger.warning(
            f"Not able to extract date string from datetime string {datetime_str}"
        )
        return ""

    date = datetime_obj.date()
    return str(date)


def generate_new_training_dates(
    max_feature_date: str,
    min_feature_date: str,
    training_dates: list,
    prediction_horizon_days: int,
    feature_data_min_date_diff: int,
) -> Tuple:
    # Find next valid feature date
    # It is guaranteed that new training date will be found within 'max_num_of_tries' iterations
    num_days_diff = get_abs_date_diff(max_feature_date, min_feature_date)
    max_num_of_tries = math.ceil(num_days_diff / feature_data_min_date_diff) + 1

    for idx in range(max_num_of_tries):
        # d3 = d2 - t
        generated_feature_date = date_add(
            max_feature_date, -1 * (idx + 1) * feature_data_min_date_diff
        )

        found = dates_proximity_check(
            generated_feature_date, training_dates, feature_data_min_date_diff
        )

        if found:
            break

    if not found:
        logger.warning(
            "Couldn't find a date honouring proximity check. "
            f"Proceeding with {generated_feature_date} as feature_date"
        )

    feature_date = generated_feature_date
    label_date = date_add(feature_date, prediction_horizon_days)

    return (feature_date, label_date)


def merge_lists_to_unique(l1: list, l2: list) -> list:
    """Merges two lists and returns a unique list of elements.

    Args:
        l1 (list): The first list.
        l2 (list): The second list.

    Returns:
        list: A unique list of elements from both the lists.
    """
    return list(set(l1 + l2))


def fetch_key_from_dict(dictionary, key, default_value=None):
    if not dictionary:
        dictionary = dict()
    return dictionary.get(key, default_value)


def get_feature_package_path(input_models: List[str]) -> str:
    """Returns the feature package path

    Args:
        input_models (List[str]): list of input models

    Returns:
        str: feature package path
    """
    assert (
        len(input_models) > 0
    ), "No input models provided in the config. Path to profiles input models (ex: models/<entity_var_name>) must be specified in the train data config."
    return ",".join(input_models)


def subprocess_run(args):
    response = subprocess.run(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if response.returncode != 0:
        logger.error(f"Error occurred. Exit code:{response.returncode}")
        logger.error(f"Subprocess Output: {response.stdout}")
        raise Exception(f"Subprocess Error: {response.stderr}")
    return response


def plot_regression_deciles(y_pred, y_true, deciles_file, label_column):
    """
    Plots y-actual vs y-predicted using deciles and saves it as a file.
    Args:
        y_pred (pd.Series): Predicted labels.
        y_true (pd.Series): Actual labels.
        deciles_file (str): File path to save the deciles plot.
        label_column (str): Name of the label column.
    Returns:
        None. The function only saves the deciles plot as a file.
    """
    deciles = pd.qcut(y_pred, q=10, labels=False, duplicates="drop")
    deciles_df = pd.DataFrame(
        {"Actual": y_true, "Predicted": y_pred, "Deciles": deciles}
    )
    deciles_agg = (
        deciles_df.groupby("Deciles")
        .agg({"Actual": "mean", "Predicted": "mean"})
        .reset_index()
    )

    sns.set(style="ticks", context="notebook")
    plt.figure(figsize=(8, 6))
    plt.scatter(deciles_agg["Predicted"], deciles_agg["Actual"], color="b", alpha=0.5)
    plt.plot(
        [deciles_agg["Predicted"].min(), deciles_agg["Predicted"].max()],
        [deciles_agg["Predicted"].min(), deciles_agg["Predicted"].max()],
        color="r",
        linestyle="--",
        linewidth=2,
    )
    plt.title(f"Y-Actual vs Y-Predicted (Deciles) for {label_column}")
    plt.xlabel(f"Mean Predicted {label_column}")
    plt.ylabel(f"Mean Actual {label_column}")
    sns.despine()
    plt.grid(True)
    plt.savefig(deciles_file)
    plt.clf()


def plot_regression_residuals(y_pred, y_true, residuals_file):
    """
    Plots regression residuals and saves it as a file.

    Args:
        y_true (array-like): The test data true values.
        y_pred (array-like): The predicted data values.
        chart_name (str): The name of the plot file.

    Returns:
        None. The function only saves the residuals plot as a file.
    """
    residuals = y_true - y_pred
    sns.set(style="ticks", context="notebook")
    plt.figure(figsize=(8, 6))
    plt.scatter(y_pred, residuals, color="b", alpha=0.5)
    plt.axhline(y=0, color="r", linestyle="--", linewidth=2)
    plt.title("Residuals Plot (Test data)")
    plt.xlabel("Predicted Values")
    plt.ylabel("Residuals")
    sns.despine()
    plt.grid(True)
    plt.savefig(residuals_file)
    plt.clf()


def regression_evaluation_plot(y_pred, y_true, regression_chart_file, num_bins=10):
    """
    Create a plot between the percentage of targeted data and the percentage of actual labels covered.

    Parameters:
    - y_pred (array-like): Predicted values.
    - y_true (array-like): True values.
    - regression_chart_file (str): The file path to save the regression chart.
    - num_bins (int, optional): Number of bins for dividing the continuous labels. Default is 10.

    Returns:
    - None
    """
    # Calculate deciles for y_true
    deciles = np.percentile(y_true, np.arange(0, 100, 100 / num_bins))

    # Calculate the percentage of targeted data
    percentage_targeted = [
        np.sum(y_pred <= decile) / len(y_pred) * 100 for decile in deciles
    ]

    # Calculate the percentage of actual labels covered
    percentage_covered = [
        np.sum(y_true[y_pred <= decile] <= decile) / len(y_true) * 100
        for decile in deciles
    ]

    # Plot the results
    plt.plot(
        percentage_targeted,
        percentage_covered,
        marker="o",
        linestyle="-",
        label="Regression Evaluation",
    )

    # Plot the base case line
    plt.plot([0, 100], [0, 100], linestyle="--", color="gray", label="Base Case")

    plt.xlabel("Percentage of Data Targeted")
    plt.ylabel("Percentage of Target Data Covered")
    plt.title("Regression Evaluation Plot")
    plt.legend()
    plt.savefig(regression_chart_file)
    plt.clf()


def plot_roc_auc_curve(y_pred, y_true, roc_auc_file) -> None:
    """
    Plots the ROC curve and calculates the Area Under the Curve (AUC) for a given classifier model.

    Parameters:
    y_true (array-like): The test data true labels.
    y_pred (array-like): The predicted data labels.
    chart_name (str): The name of the plot file.

    Returns:
    None. The function does not return any value. The generated ROC curve plot is \
        saved as an image file and uploaded to the session's file storage.
    """
    fpr, tpr, _ = roc_curve(y_true, y_pred)
    roc_auc = auc(fpr, tpr)
    sns.set(style="ticks", context="notebook")
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


def plot_pr_auc_curve(y_pred, y_true, pr_auc_file) -> None:
    """
    Plots a precision-recall curve and saves it as a file.

    Args:
        y_true (array-like): The test data true labels.
        y_pred (array-like): The predicted data labels.
        chart_name (str): The name of the plot file.

    Returns:
        None. The function only saves the precision-recall curve plot as a file.
    """
    precision, recall, _ = precision_recall_curve(y_true, y_pred)
    pr_auc = auc(recall, precision)
    sns.set(style="ticks", context="notebook")
    plt.figure(figsize=(8, 6))
    plt.plot(recall, precision, color="b", label=f"PR AUC = {pr_auc:.2f}", linewidth=2)
    plt.ylim([int(min(precision) * 20) / 20, 1.0])
    plt.xlim([int(min(recall) * 20) / 20, 1.0])
    plt.title("Precision-Recall Curve (Test data)")
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.legend(loc="lower left")
    sns.despine()
    plt.grid(True)
    plt.savefig(pr_auc_file)
    plt.clf()


def plot_lift_chart(y_pred, y_true, lift_chart_file) -> None:
    """
    Generates a lift chart for a binary classification model.

    Args:
        y_true (array-like): The test data true labels.
        y_pred (array-like): The predicted data labels.
        chart_name (str): The name of the plot file.

    Returns:
        None. The function does not return any value, but it saves the lift chart as an image file \
            in the specified location.
    """
    data = pd.DataFrame()
    data["label"] = y_true
    data["pred"] = y_pred

    sorted_indices = np.argsort(data["pred"].values, kind="heapsort")[::-1]
    cumulative_actual = np.cumsum(data["label"][sorted_indices].values)
    cumulative_percentage = np.linspace(0, 1, len(cumulative_actual) + 1)

    sns.set(style="ticks", context="notebook")
    plt.figure(figsize=(8, 6))
    sns.lineplot(
        x=cumulative_percentage * 100,
        y=np.array([0] + list(100 * cumulative_actual / cumulative_actual[-1])),
        linewidth=2,
        color="b",
        label="Model Lift curve",
    )
    sns.despine()
    plt.plot(
        [0, 100 * data["label"].mean()],
        [0, 100],
        color="red",
        linestyle="--",
        label="Best Case",
        linewidth=1.5,
    )
    plt.plot(
        [0, 100],
        [0, 100],
        color="black",
        linestyle="--",
        label="Baseline",
        linewidth=1.5,
    )

    plt.title("Cumulative Gain Curve")
    plt.xlabel("Percentage of Predicted Target Users")
    plt.ylabel("Percent of Actual Target Users")
    plt.ylim([0, 100])
    plt.xlim([0, 100])
    plt.legend()
    plt.grid(True)
    plt.savefig(lift_chart_file)
    plt.clf()


def plot_top_k_feature_importance(
    pipe, train_x, numeric_columns, categorical_columns, figure_file, top_k_features=5
) -> pd.DataFrame:
    train_x_processed = pipe["preprocessor"].transform(train_x)
    train_x_processed = train_x_processed.astype(np.int_)

    try:
        shap_values = shap.TreeExplainer(pipe["model"]).shap_values(train_x_processed)
    except Exception as e:
        logger.warning(
            f"Exception occured while calculating shap values {e}, using KernelExplainer"
        )
        shap_values = shap.KernelExplainer(
            pipe["model"].predict_proba, data=train_x_processed
        ).shap_values(train_x_processed)

    x_label = "Importance scores"
    if isinstance(shap_values, list):
        logger.debug(
            "Got List output, suggesting that the model is a multi-output model. \
                Using the second output for plotting feature importance"
        )
        x_label = "Importance scores of positive label"
        shap_values = shap_values[1]
    elif (
        isinstance(shap_values, np.ndarray)
        and len(shap_values.shape) == 3
        and shap_values.shape[2] == 2
    ):
        logger.debug(
            "Got 3D numpy array with last dimension having depth of 2. Taking the second output for plotting feature importance"
        )
        shap_values = shap_values[:, :, 1]
    onehot_encoder_columns = get_column_names(
        dict(pipe.steps)["preprocessor"].transformers_[1][1].named_steps["encoder"],
        categorical_columns,
    )
    col_names_ = (
        numeric_columns
        + onehot_encoder_columns
        + [
            col
            for col in list(train_x)
            if col not in numeric_columns and col not in categorical_columns
        ]
    )
    shap_df = pd.DataFrame(shap_values, columns=col_names_)
    vals = np.abs(shap_df.values).mean(0)
    feature_names = shap_df.columns
    shap_importance = pd.DataFrame(
        data=vals, index=feature_names, columns=["feature_importance_vals"]
    )
    shap_importance.sort_values(
        by=["feature_importance_vals"], ascending=False, inplace=True
    )

    ax = shap_importance[:top_k_features][::-1].plot(
        kind="barh", figsize=(8, 6), color="#86bf91", width=0.3
    )
    ax.set_xlabel(x_label)
    ax.set_ylabel("Feature Name")
    plt.title(f"Top {top_k_features} Important Features")
    plt.savefig(figure_file, bbox_inches="tight")
    plt.clf()
    return shap_importance


def fetch_staged_file(
    session: snowflake.snowpark.Session,
    stage_name: str,
    file_name: str,
    target_folder: str,
) -> None:
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

    with gzip.open(input_file_path, "rb") as gz_file:
        with open(output_file_path, "wb") as target_file:
            shutil.copyfileobj(gz_file, target_file)
    os.remove(input_file_path)


def load_stage_file_from_local(
    session: snowflake.snowpark.Session,
    stage_name: str,
    file_name: str,
    target_folder: str,
    filetype: str,
):
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
    if filetype == "json":
        f = open(os.path.join(target_folder, file_name), "r")
        output_file = json.load(f)
    elif filetype == "joblib":
        output_file = joblib.load(os.path.join(target_folder, file_name))
    os.remove(os.path.join(target_folder, file_name))
    return output_file


#### Kept here for explain_prediction function ####
def remap_credentials(credentials: dict) -> dict:
    """Remaps credentials from profiles siteconfig to the expected format from snowflake session

    Args:
        credentials (dict): Data warehouse credentials from profiles siteconfig

    Returns:
        dict: Data warehouse creadentials remapped in format that is required to create a snowpark session
    """
    new_creds = {
        k if k != "dbname" else "database": v
        for k, v in credentials.items()
        if k != "type"
    }
    return new_creds


def get_timestamp_columns(table: snowflake.snowpark.Table) -> List[str]:
    """
    Retrieve the names of timestamp columns from a given table schema, excluding the index timestamp column.
    Args:
        session (snowflake.snowpark.Session): The Snowpark session for data warehouse access.
        feature_table (snowflake.snowpark.Table): The feature table from which to retrieve the timestamp columns.
    Returns:
        List[str]: A list of names of timestamp columns from the given table schema, excluding the index timestamp column.
    """
    timestamp_columns = []
    for field in table.schema.fields:
        if field.datatype in [T.TimestampType(), T.DateType(), T.TimeType()]:
            timestamp_columns.append(field.name)
    return timestamp_columns


# Not being called currently. Functions for saving feature-importance score for top_k and
# bottom_k users as per their prediction scores. ####
def explain_prediction(
    creds: dict,
    user_main_id: str,
    predictions_table_name: str,
    feature_table_name: str,
    predict_config: dict,
) -> None:
    """
    Function to generate user-specific feature-importance data.

    Args:
        creds (dict): A dictionary containing the data warehouse credentials.
        user_main_id (str): The main ID of the user for whom the feature importance data is generated.
        predictions_table_name (str): The name of the table containing the prediction results.
        feature_table_name (str): The name of the table containing the feature data.
        predict_config (dict): A dictionary containing the configuration settings for the prediction.

    Returns:
        None
    """
    # Existing code remains unchanged
    connection_parameters = remap_credentials(creds)
    session = Session.builder.configs(connection_parameters).create()

    current_dir = os.getcwd()
    config_path = os.path.join(current_dir, "config", "model_configs.yaml")
    notebook_config = load_yaml(config_path)
    merged_config = combine_config(notebook_config, predict_config)

    stage_name = constants.STAGE_NAME
    model_file_name = constants.MODEL_FILE_NAME

    prediction_table = session.table(predictions_table_name)
    model_id = prediction_table.select(F.col("model_id")).limit(1).collect()[0].MODEL_ID
    output_profiles_ml_model = merged_config["data"]["output_profiles_ml_model"]
    entity_column = merged_config["data"]["entity_column"]
    timestamp_columns = merged_config["preprocessing"]["timestamp_columns"]
    index_timestamp = merged_config["data"]["index_timestamp"]
    score_column_name = merged_config["outputs"]["column_names"]["score"]
    top_k = merged_config["data"]["top_k"]
    bottom_k = merged_config["data"]["bottom_k"]

    column_dict = load_stage_file_from_local(
        session,
        stage_name,
        f"{output_profiles_ml_model}_{model_id}_column_names.json",
        ".",
        "json",
    )
    numeric_columns = column_dict["numeric_columns"]
    categorical_columns = column_dict["categorical_columns"]

    prediction_table = prediction_table.select(
        F.col(entity_column), F.col(score_column_name)
    )
    feature_table = session.table(feature_table_name)
    if len(timestamp_columns) == 0:
        timestamp_columns = get_timestamp_columns(session, feature_table)
    for ts_col in timestamp_columns:
        feature_table = feature_table.withColumn(
            col, F.datediff("day", F.col(ts_col), F.col(index_timestamp))
        )
    feature_table = feature_table.select(
        [entity_column] + numeric_columns + categorical_columns
    )

    feature_df = (
        feature_table.join(prediction_table, [entity_column], join_type="left")
        .sort(F.col(score_column_name).desc())
        .to_pandas()
    )
    final_df = pd.concat(
        [feature_df.head(int(top_k)), feature_df.tail(int(bottom_k))], axis=0
    )

    final_df[numeric_columns] = final_df[numeric_columns].replace({pd.NA: np.nan})
    final_df[categorical_columns] = final_df[categorical_columns].replace({pd.NA: None})

    data_x = final_df.drop([entity_column.upper(), score_column_name.upper()], axis=1)
    data_y = final_df[[entity_column.upper(), score_column_name.upper()]]

    pipe = load_stage_file_from_local(
        session, stage_name, model_file_name, ".", "joblib"
    )
    onehot_encoder_columns = get_column_names(
        dict(pipe.steps)["preprocessor"].transformers_[1][1].named_steps["encoder"],
        categorical_columns,
    )
    col_names_ = (
        numeric_columns
        + onehot_encoder_columns
        + [
            col
            for col in list(data_x)
            if col not in numeric_columns and col not in categorical_columns
        ]
    )

    explainer = shap.TreeExplainer(pipe["model"], feature_names=col_names_)
    shap_score = explainer(pipe["preprocessor"].transform(data_x).astype(np.int_))
    shap_df = pd.DataFrame(shap_score.values, columns=col_names_)
    shap_df.insert(0, entity_column.upper(), data_y[entity_column.upper()].values)
    shap_df.insert(
        len(shap_df.columns),
        score_column_name.upper(),
        data_y[score_column_name.upper()].values,
    )
    session.write_pandas(
        shap_df,
        table_name="Material_shopify_feature_importance",
        auto_create_table=True,
        overwrite=True,
    )


# Not being called currently. Functions for plotting feature importance score for single user. ####
def plot_user_feature_importance(
    pipe, user_featues, numeric_columns, categorical_columns
):
    """
    Plot the feature importance score for a single user.

    Parameters:
    pipe (pipeline object): The pipeline object that includes the model and preprocessor.
    user_features (array-like): The user features for which the feature importance scores will be calculated and plotted.
    numeric_columns (list): The list of numeric columns in the user features.
    categorical_columns (list): The list of categorical columns in the user features.

    Returns:
    figure object and a dictionary of feature importance scores for the user.
    """
    onehot_encoder_columns = get_column_names(
        dict(pipe.steps)["preprocessor"].transformers_[1][1].named_steps["encoder"],
        categorical_columns,
    )
    col_names_ = (
        numeric_columns
        + onehot_encoder_columns
        + [
            col
            for col in list(user_featues)
            if col not in numeric_columns and col not in categorical_columns
        ]
    )

    explainer = shap.TreeExplainer(pipe["model"], feature_names=col_names_)
    shap_score = explainer(pipe["preprocessor"].transform(user_featues).astype(np.int_))
    user_feat_imp_dict = dict(zip(col_names_, shap_score.values[0]))
    figure = shap.plots.waterfall(shap_score[0], max_display=10, show=False)
    return figure, user_feat_imp_dict


def replace_seq_no_in_query(query: str, seq_no: int) -> str:
    match = re.search(r"(_\d+)(`|$)", query)
    if match:
        replaced_query = (
            query[: match.start(1)] + "_" + str(seq_no) + query[match.end(1) :]
        )
        return replaced_query
    else:
        raise Exception(f"Couldn't find an integer seq_no in the input query: {query}")


def extract_seq_no_from_select_query(select_query: str) -> int:
    schema_table_name = select_query.split(" ")[-1]
    table_name_wo_schema = schema_table_name.split(".")[-1]
    if table_name_wo_schema.startswith("`") and table_name_wo_schema.endswith("`"):
        table_name = table_name_wo_schema[1:-1]
    else:
        table_name = table_name_wo_schema
    seq_no = int(table_name.split("_")[-1])
    return seq_no
