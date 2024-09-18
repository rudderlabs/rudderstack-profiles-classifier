import math
import re
import random
import string
import warnings
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=NumbaDeprecationWarning)
warnings.simplefilter("ignore", category=NumbaPendingDeprecationWarning)


from sklearn.metrics import (
    auc,
    roc_curve,
    precision_recall_curve,
)

from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
from typing import Tuple, List, Dict

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
    arraytype_columns: List[str]
    booleantype_columns: List[str]
    booleantype_columns: List[str]
    ignore_features: List[str]
    numeric_features: List[str]
    categorical_features: List[str]
    imputation_strategy: dict
    train_size: float
    test_size: float
    val_size: float
    top_k_array_categories: int


@dataclass
class OutputsConfig:
    """OutputsConfig class is used to store the outputs configuration parameters"""

    column_names: dict
    feature_meta_data: List[dict]


def split_train_test(
    feature_df: pd.DataFrame,
    label_column: str,
    entity_column: str,
    train_size: float,
    isStratify: bool,
) -> Tuple:
    """Returns the train_x, train_y, test_x, test_y, val_x, val_y in form of pd.DataFrame"""
    feature_df.columns = feature_df.columns.str.upper()
    X_train, X_test = train_test_split(
        feature_df,
        train_size=train_size,
        random_state=42,
        stratify=feature_df[label_column.upper()].values if isStratify else None,
    )

    train_x = X_train.drop([entity_column.upper(), label_column.upper()], axis=1)
    train_y = X_train[[label_column.upper()]]

    test_x = X_test.drop([entity_column.upper(), label_column.upper()], axis=1)
    test_y = X_test[[label_column.upper()]]

    return train_x, train_y, test_x, test_y


def load_yaml(file_path: str) -> dict:
    with open(file_path, "r") as f:
        data = yaml.safe_load(f)
    return data


def combine_config(default_config: dict, profiles_config: dict = None) -> dict:
    """Combine the configs after overwriting values of profiles.yaml in model_configs.yaml"""
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


def extract_unique_values(input_dict: Dict[str, List[str]]) -> List[str]:
    unique_values = set()
    for _, value_list in input_dict.items():
        unique_values.update(value_list)

    return list(unique_values)


def get_feature_table_column_types(
    feature_table,
    input_column_types: dict,
    label_column: str,
    entity_column: str,
    transformed_arraytype_cols: List[str],
    transformed_booleantype_cols: List[str],
    dtype_mapping: dict,
):
    feature_table_column_types = list()

    for col in transformed_arraytype_cols:
        input_column_types["numeric"][col] = dtype_mapping["numeric"]

    for col in transformed_booleantype_cols:
        input_column_types["numeric"][col] = dtype_mapping["numeric"]

    for col in input_column_types["timestamp"]:
        input_column_types["numeric"][col] = dtype_mapping["numeric"]

    for col in feature_table.columns:
        if col.upper() in (label_column.upper(), entity_column.upper()):
            continue
        feature_table_column_types.append(col)
    return feature_table_column_types


def get_all_ignore_features(
    feature_table, config_ignore_features, high_cardinal_features
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
    if mode == constants.RUDDERSTACK_MODE:
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
            timestamp = datetime.strptime(timestamp_str, constants.MATERIAL_DATE_FORMAT)
        string_date = timestamp.strftime(constants.MATERIAL_DATE_FORMAT)
        return string_date
    except Exception as e:
        logger.get().error(
            f"Error occurred while converting timestamp to date string: {e}"
        )
        raise Exception(
            f"Error occurred while converting timestamp to date string: {e}"
        )


def get_column_names(onehot_encoder: OneHotEncoder, col_names: List[str]) -> List[str]:
    """Returns List of categorical column names of the output dataframe
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
    for col in numeric_columns:
        df[col] = df[col].astype("float64")
    """Replaces the pd.NA values in the numeric and categorical columns of a pandas DataFrame with np.nan and None, respectively."""
    for col in numeric_columns:
        df[col] = df[col].astype("float64")
    df[numeric_columns] = df[numeric_columns].replace({pd.NA: 0})
    df[categorical_columns] = df[categorical_columns].replace({pd.NA: "unknown"})
    return df


def get_output_directory(folder_path: str) -> str:
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
    try:
        os.remove(file_path)
        logger.get().info(f"File '{file_path}' deleted successfully from local.")
    except FileNotFoundError:
        logger.get().error(f"Error: File '{file_path}' not found.")
    except PermissionError:
        logger.get().error(f"Error: Permission denied. Unable to delete '{file_path}'.")
    except OSError as e:
        logger.get().error(f"Error occurred while deleting file '{file_path}': {e}")


def delete_folder(folder_path: str) -> None:
    try:
        shutil.rmtree(folder_path)
        logger.get().info(f"Folder '{folder_path}' deleted successfully from local.")
    except FileNotFoundError:
        logger.get().error(f"Error: Folder '{folder_path}' not found.")
    except PermissionError:
        logger.get().error(
            f"Error: Permission denied. Unable to delete '{folder_path}'."
        )
    except OSError as e:
        logger.get().error(f"Error occurred while deleting folder '{folder_path}': {e}")


def date_add(reference_date: str, add_days: int) -> str:
    """
    Adds the horizon days to the reference date (in the format "YYYY-MM-DD") and
    returns the new date (in the format "YYYY-MM-DD") as a string.
    """
    new_timestamp = datetime.strptime(
        reference_date, constants.MATERIAL_DATE_FORMAT
    ) + timedelta(days=add_days)
    new_date = new_timestamp.strftime(constants.MATERIAL_DATE_FORMAT)
    return new_date


def get_abs_date_diff(ref_date1: str, ref_date2: str) -> int:
    """
    For given two dates (in the format "YYYY-MM-DD") in string format, it will return the difference in days
    """
    d1 = datetime.strptime(ref_date1, constants.MATERIAL_DATE_FORMAT)
    d2 = datetime.strptime(ref_date2, constants.MATERIAL_DATE_FORMAT)
    diff = d2 - d1
    return abs(diff.days)


def dates_proximity_check(reference_date: str, dates: list, distance: int) -> bool:
    """
    For given reference date and list of training dates, it will check
    whether a given date(reference_date) is farther from every date in the dates list, by minimum "distance" days.
    """
    for d in dates:
        if get_abs_date_diff(reference_date, d) < distance:
            return False
    return True


def datetime_to_date_string(datetime_str: str) -> str:
    try:
        if "+" in datetime_str:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S%z")
        elif " " in datetime_str:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        else:
            datetime_obj = datetime.strptime(
                datetime_str, constants.MATERIAL_DATE_FORMAT
            )
    except ValueError:
        # Value error will be raised if its not able
        # to match datetime string with given format
        # In this case datetime sting is in "%Y-%m-%d" format
        # and returning empty string
        logger.get().error(
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
        logger.get().error(
            "Couldn't find a date honouring proximity check. "
            f"Proceeding with {generated_feature_date} as feature_date"
        )

    feature_date = generated_feature_date
    label_date = date_add(feature_date, prediction_horizon_days)

    return (feature_date, label_date)


def merge_lists_to_unique(l1: list, l2: list) -> list:
    return list(set(l1 + l2))


def fetch_key_from_dict(dictionary, key, default_value=None):
    if not dictionary:
        dictionary = dict()
    return dictionary.get(key, default_value)


def get_feature_package_path(model_refs: List[str]) -> str:
    assert (
        len(model_refs) > 0
    ), "No input models provided in the config. Path to profiles input models (ex: models/<entity_var_name>) must be specified in the train data config."
    return ",".join(model_refs)


def subprocess_run(args):
    try:
        response = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
    except Exception as e:
        raise Exception(
            f"Error occurred while running subprocess with params {args}: {e}"
        )
    if response.returncode != 0:
        logger.get().error(f"Error occurred. Exit code:{response.returncode}")
        logger.get().debug(f"Subprocess Output: {response.stdout}")
        raise Exception(f"Subprocess Error: {response.stderr}")
    return response


def plot_regression_deciles(
    y_pred: np.array, y_true: np.array, deciles_file: str, label_column: str
):
    y_true = pd.Series(y_true)
    y_pred = pd.Series(y_pred)

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


def plot_regression_residuals(y_pred: np.array, y_true: np.array, residuals_file: str):
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
    """Create a plot between the percentage of targeted data and the percentage of actual labels covered."""
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


def plot_roc_auc_curve(y_pred: np.array, y_true: np.array, roc_auc_file: str) -> None:
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


def plot_pr_auc_curve(y_pred: np.array, y_true: np.array, pr_auc_file: str) -> None:
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


def plot_lift_chart(y_pred: np.array, y_true: np.array, lift_chart_file: str) -> None:
    """Generates a lift chart for a binary classification model."""
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
    model, train_x, figure_file, top_k_features=20
) -> None:
    try:
        if len(train_x) < 100:
            sample_data = train_x
        else:
            sample_data = train_x.sample(100, random_state=42)

        for col in list(sample_data):
            sample_data[col] = sample_data[col].astype(float)

        model_class = model.__class__.__name__
        explainer_class = constants.EXPLAINER_MAP[model_class]
        explainer = explainer_class(model, sample_data)

        shap_values = explainer(sample_data)
        if len(shap_values.shape) == 3:
            shap_values = shap_values[:, :, 1]

        model_name = model.__class__.__name__
        shap.plots.beeswarm(shap_values, max_display=top_k_features, show=False)
        plt.title(f"Feature Importance for trained {model_name}")
        plt.savefig(figure_file, bbox_inches="tight")

        return None

    except Exception as e:
        logger.get().error(f"Exception occured while plotting feature importance {e}")


def fetch_staged_file(
    session: snowflake.snowpark.Session,
    stage_name: str,
    file_name: str,
    target_folder: str,
) -> None:
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
    """Fetches a file from a Snowflake stage to local, loads it into memory and delete that file from local.

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
    new_creds = {
        k if k != "dbname" else "database": v
        for k, v in credentials.items()
        if k != "type"
    }
    return new_creds


def get_timestamp_columns(table: snowflake.snowpark.Table) -> List[str]:
    timestamp_columns = []
    for field in table.schema.fields:
        if field.datatype in [T.TimestampType(), T.DateType(), T.TimeType()]:
            timestamp_columns.append(field.name)
    return timestamp_columns


# TODO: Remove this unused code
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


def generate_random_string(length):
    letters = string.ascii_letters + string.digits
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def parse_timestamp(timestamp_str: str) -> datetime:
    formats = [
        "%Y-%m-%d %H:%M:%S",  # Format without microseconds
        "%Y-%m-%d %H:%M:%S.%f",  # Format with microseconds
    ]
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Error parsing pb run time '{timestamp_str}' from string to timestamp format."
    )


def replace_seq_no_in_query(query: str, seq_no: int) -> str:
    matches = list(re.finditer(r"(_\d+)(`|'|\"|$)", query))
    if len(matches) == 0:
        raise Exception(f"Couldn't find an integer seq_no in the input query: {query}")
    match = matches[-1]
    if match is None:
        raise Exception(f"Couldn't find an integer seq_no in the input query: {query}")
    replaced_query = query[: match.start(1)] + "_" + str(seq_no) + query[match.end(1) :]
    return replaced_query


def get_model_configs_file_path() -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(
        os.path.dirname(current_dir),
        "config",
        "model_configs.yaml",
    )
