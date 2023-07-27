from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, f1_score, average_precision_score
import numpy as np 
import pandas as pd
from typing import Tuple, List, Union

import snowflake.snowpark
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col

import yaml
from copy import deepcopy
from datetime import datetime, timedelta

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
    """Combine the configs after overwriting values of profiles.yaml in data_prep.yaml

    Args:
        notebook_config (dict): configs from data_prep.yaml file
        profiles_config (dict, optional): configs from profiles.yaml file that should overwrite corresponding values from notebook_config. Defaults to None.

    Returns:
        dict: final merged config
    """
    if not isinstance(profiles_config, dict):
        return notebook_config
    profiles_data = profiles_config.get('data', None)
    if profiles_data is None or not isinstance(profiles_data, dict):
        return notebook_config
    merged_config = deepcopy(notebook_config)
    for key in profiles_data.keys():
        if key in merged_config['data'].keys():
            merged_config['data'][key] = profiles_data[key]
    return merged_config

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
    return str(start_date), str(end_date)

def get_label_date_ref(feature_date: str, horizon_days: int) -> str:
    """This function adds the horizon days to the feature date and returns the label date as a string
    """
    label_timestamp = datetime.strptime(feature_date, "%Y-%m-%d") + timedelta(days=horizon_days)
    label_date = label_timestamp.strftime("%Y-%m-%d")
    return label_date

def get_latest_material_hash(session: snowflake.snowpark.Session,
                       material_table: str,
                       model_name:str) -> Tuple:
    """This function will return the model hash that is latest for given model name in material table

    Args:
        session (snowflake.snowpark.Session): snowpark session
        material_table (str): name of material registry table
        model_name (str): model_name from data_prep file

    Returns:
        Tuple: latest model hash and it's creation timestamp
    """
    snowpark_df = session.table(material_table)
    temp_hash_vector = snowpark_df.filter(col("model_name") == model_name).sort(col("creation_ts"), ascending=False).limit(1).select(col("model_hash"), col("creation_ts")).collect()[0]
    model_hash = temp_hash_vector.MODEL_HASH
    creation_ts = temp_hash_vector.CREATION_TS
    return model_hash, creation_ts

def get_material_names(session: snowflake.snowpark.Session,
                       material_table: str, 
                       start_time: str, 
                       end_time: str, 
                       model_name:str,
                       model_hash: str,
                       material_table_prefix:str,
                       prediction_horizon_days: int) -> List[Tuple[str, str]]:
    """Generates material names as list of tuple of feature table name and label table name required to create the training model.

    Args:
        session (snowflake.snowpark.Session): Snowpark session for data warehouse access
        material_table (str): Name of the material table(present in constants.py file)
        start_time (str): train_start_dt
        end_time (str): train_end_dt
        model_name (str): Present in data_prep file
        model_hash (str) : latest model hash
        material_table_prefix (str): constant
        prediction_horizon_days (int): period of days

    Returns:
        List[Tuple[str, str]]: List of tuples of feature table names and label table names
        ex: [(material_shopify_user_features_94b5f59c_274, material_shopify_user_features_94b5f59c_275),
        (material_shopify_user_features_94b5f59c_276, material_shopify_user_features_94b5f59c_277)]
    """
    material_names = list()

    snowpark_df = session.table(material_table)

    feature_snowpark_df = (snowpark_df
                .filter(col("model_name") == model_name)
                .filter(col("model_hash") == model_hash)
                .filter(f"end_ts between \'{start_time}\' and \'{end_time}\'")
                .select("seq_no", "end_ts")
                )
    label_snowpark_df = (snowpark_df
                .filter(col("model_name") == model_name)
                .filter(col("model_hash") == model_hash)
                .filter(f"end_ts between dateadd(day, {prediction_horizon_days}, \'{start_time}\') and dateadd(day, {prediction_horizon_days}, \'{end_time}\')")
                .select("seq_no", "end_ts")
                )
    
    feature_label_snowpark_df = feature_snowpark_df.join(label_snowpark_df,
                                                            F.datediff("day", feature_snowpark_df.end_ts, label_snowpark_df.end_ts)==prediction_horizon_days
                                                            ).select(feature_snowpark_df.seq_no.alias("feature_seq_no"),
                                                                    label_snowpark_df.seq_no.alias("label_seq_no"))
    for row in feature_label_snowpark_df.collect():
        material_names.append((material_table_prefix+model_name+"_"+model_hash+"_"+str(row.FEATURE_SEQ_NO), material_table_prefix+model_name+"_"+model_hash+"_"+str(row.LABEL_SEQ_NO)))
    return material_names


def prepare_feature_table(session: snowflake.snowpark.Session,
                          feature_table_name: str, 
                          label_table_name: str,
                          entity_column: str, 
                          index_timestamp: str,
                          timestamp_columns: List[str], 
                          eligible_users: str, 
                          label_column: str,
                          label_value: Union[str,int,float], 
                          prediction_horizon_days: int,
                          ignore_features: List[str]) -> snowflake.snowpark.Table:
    """This function creates a feature table as per the requirement of customer that is further used for training and prediction.

    Args:
        session (snowflake.snowpark.Session): Snowpark session for data warehouse access
        feature_table_name (str): feature table from the retrieved material_names tuple
        label_table_name (str): label table from the retrieved material_names tuple
        entity_column (str): name of entity column from data_prep file
        index_timestamp (str): name of column containing timestamp info from data_prep file
        timestamp_columns (List[str]): list of timestamp columns
        eligible_users (str): query as a valid string to filter out eligible users as per need from data_prep file
        label_column (str): name of label column from data_prep file
        label_value (Union[str,int,float]): required label_value from data_prep file
        prediction_horizon_days (int): 
        ignore_features (List[str]): list of all features that are needed to be ignored(dropped)

    Returns:
        snowflake.snowpark.Table: feature table made using given instance from material names
    """
    label_ts_col = f"{index_timestamp}_label_ts"
    feature_table = session.table(feature_table_name)#.withColumn(label_ts_col, F.dateadd("day", F.lit(prediction_horizon_days), F.col(index_timestamp)))
    if eligible_users:
        feature_table = feature_table.filter(eligible_users)
    feature_table = feature_table.drop([label_column])
    for col in timestamp_columns:
        feature_table = feature_table.withColumn(col, F.datediff('day', F.col(col), F.col(index_timestamp)))
    label_table = (session.table(label_table_name)
                   .withColumn(label_column, F.when(F.col(label_column)==label_value, F.lit(1)).otherwise(F.lit(0)))
                   .select(entity_column, label_column, index_timestamp)
                   .withColumnRenamed(F.col(index_timestamp), label_ts_col))
    uppercase_list = lambda names: [name.upper() for name in names]
    lowercase_list = lambda names: [name.lower() for name in names]
    ignore_features_ = [col for col in feature_table.columns if col in uppercase_list(ignore_features) or col in lowercase_list(ignore_features)]
    return feature_table.join(label_table, [entity_column], join_type="left").drop([index_timestamp, label_ts_col]).drop(ignore_features_)
    
def split_train_test(feature_table: snowflake.snowpark.Table, 
                     label_column: str, 
                     entity_column: str, 
                     model_name_prefix: str, 
                     train_size:float, 
                     val_size: float, 
                     test_size: float) -> Tuple:
    """Splits the data in train test and validation according to the their given partition factions.

    Args:
        feature_table (snowflake.snowpark.Table): feature table from the retrieved material_names tuple
        label_column (str): name of label column from feature table
        entity_column (str): name of entity column from feature table
        model_name_prefix (str): prefix for the model from data_prep file
        train_size (float): partition fraction for train data
        val_size (float): partition fraction for validation data
        test_size (float): partition fraction for test data

    Returns:
        Tuple: returns the train_x, train_y, test_x, test_y, val_x, val_y in form of pd.DataFrame
    """
    X_train, X_test, X_val  = feature_table.random_split([train_size, val_size, test_size], seed=42)
    #ToDo: handle timestamp columns, remove customer_id from train_x
    X_train.write.mode("overwrite").save_as_table(f"{model_name_prefix}_train")
    X_test.write.mode("overwrite").save_as_table(f"{model_name_prefix}_test")
    X_val.write.mode("overwrite").save_as_table(f"{model_name_prefix}_val")
    train_x = X_train.drop(label_column, entity_column).to_pandas() # drop labels for training set
    train_y = X_train.select(label_column).to_pandas()
    test_x = X_test.drop(label_column, entity_column).to_pandas() # drop labels for training set
    test_y = X_test.select(label_column).to_pandas()
    val_x = X_val.drop(label_column, entity_column).to_pandas()
    val_y = X_val.select(label_column).to_pandas()
    return train_x, train_y, test_x, test_y, val_x, val_y

def get_classification_metrics(y_true: pd.DataFrame, 
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
    metrics = {"precision": precision, "recall": recall, "f1_score": f1, "roc_auc": roc_auc, 'pr_auc': pr_auc}
    return metrics
    
def get_best_th(y_true: pd.DataFrame, y_pred_proba: np.array) -> Tuple:
    """This function calculates the thresold that maximizes f1 score based on y_true and y_pred_proba and classication metrics on basis of that.

    Args:
        y_true (pd.DataFrame): Array of 1s and 0s. True labels
        y_pred_proba (np.array): Array of predicted probabilities

    Returns:
        Tuple: Returns the metrics at the threshold and that threshold that maximizes f1 score based on y_true and y_pred_proba
    """
    best_f1 = 0.0
    best_th = 0.0

    for th in np.arange(0,1,0.01):
        f1 = f1_score(y_true, np.where(y_pred_proba>th,1,0))
        if f1 >= best_f1:
            best_th = th
            best_f1 = f1
            
    best_metrics = get_classification_metrics(y_true, y_pred_proba, best_th)
    return best_metrics, best_th

def get_metrics(clf,
                X_train: pd.DataFrame, 
                y_train: pd.DataFrame,
                X_test: pd.DataFrame, 
                y_test: pd.DataFrame,
                X_val: pd.DataFrame, 
                y_val: pd.DataFrame) -> Tuple:
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
    train_metrics, prob_threshold = get_best_th(y_train, train_preds)

    test_preds = clf.predict_proba(X_test)[:,1]
    test_metrics = get_classification_metrics(y_test, test_preds, prob_threshold)

    val_preds = clf.predict_proba(X_val)[:,1]
    val_metrics = get_classification_metrics(y_val, val_preds, prob_threshold)

    metrics = {"train": train_metrics, "val": val_metrics, "test": test_metrics}
    predictions = {"train": train_preds, "val": val_preds, "test": test_preds}
    
    return metrics, predictions, prob_threshold