import os
import sys
import yaml
import json
import typing
import joblib
import datetime
import warnings
import cachetools
import numpy as np
import pandas as pd

from typing import Any
from xgboost import XGBClassifier
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import average_precision_score
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, f1_score
from numba.core.errors import NumbaDeprecationWarning, NumbaPendingDeprecationWarning

import snowflake.snowpark
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.session import Session

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import utils
import constants
from Connector import Connector
from RedshiftConnector import RedshiftConnector
from SnowflakeConnector import SnowflakeConnector

warnings.filterwarnings('ignore', category=NumbaDeprecationWarning)
warnings.simplefilter('ignore', category=NumbaPendingDeprecationWarning)

local_folder = "data"

@cachetools.cached(cache={})
def load_model(filename: str):
    """session.import adds the staged model file to an import directory. We load the model file from this location

    Args:
        filename (str): path for the model_name

    Returns:
        _type_: return the trained model after loading it
    """
    import_dir = sys._xoptions.get("snowflake_import_directory")

    if import_dir:
        assert import_dir.startswith('/home/udf/')
        filename = os.path.join(import_dir, filename)
    else:
        filename = os.path.join(local_folder, filename)

    with open(filename, 'rb') as file:
        m = joblib.load(file)
        return m

def predict_helper(df: pd.DataFrame, model_name: str) -> Any:
    trained_model = load_model(model_name)
    categorical_columns = list(df.select_dtypes(include='object'))
    numeric_columns = list(df.select_dtypes(exclude='object'))
    df[numeric_columns] = df[numeric_columns].replace({pd.NA: np.nan})
    df[categorical_columns] = df[categorical_columns].replace({pd.NA: None})        
    return trained_model.predict_proba(df)[:,1]

def predict(creds:dict, aws_config: dict, model_path: str, inputs: str, output_tablename : str, config: dict) -> None:
    """Generates the prediction probabilities and save results for given model_path

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        aws_config (dict): aws credentials - not required for snowflake. only used for redshift
        model_path (str): path to the file where the model details including model id etc are present. Created in training step
        inputs (str): Not being used currently. Can pass a blank string. For future support
        output_tablename (str): name of output table where prediction results are written
        config (dict): configs from profiles.yaml which should overwrite corresponding values from model_configs.yaml file

    Returns:
        None: save the prediction results but returns nothing
    """

    stage_name = constants.STAGE_NAME
    model_file_name = constants.MODEL_FILE_NAME
    current_dir = os.path.dirname(os.path.abspath(__file__))

    notebook_config = utils.load_yaml(os.path.join(current_dir, "config/model_configs.yaml"))
    merged_config = utils.combine_config(notebook_config, config)

    with open(model_path, "r") as f:
        results = json.load(f)
    train_model_hash = results["config"]["material_hash"]
    train_model_id = results["model_info"]["model_id"]
    prob_th = results["model_info"]["threshold"]
    current_ts = datetime.datetime.now()

    score_column_name = merged_config['outputs']['column_names']['score']
    percentile_column_name = merged_config['outputs']['column_names']['percentile']
    output_label_column = merged_config['outputs']['column_names']['output_label_column']
    output_profiles_ml_model = merged_config["data"]["output_profiles_ml_model"]
    label_column = merged_config["data"]["label_column"]
    label_value = merged_config["data"]["label_value"]
    index_timestamp = merged_config["data"]["index_timestamp"]
    eligible_users = merged_config["data"]["eligible_users"]
    ignore_features = merged_config["preprocessing"]["ignore_features"]
    timestamp_columns = merged_config["preprocessing"]["timestamp_columns"]
    entity_column = merged_config["data"]["entity_column"]
    features_profiles_model = merged_config["data"]["features_profiles_model"]

    predict_path = os.path.join(current_dir, 'predict.py')
    import_files = ("utils.py","constants.py", "Connector.py", "SnowflakeConnector.py", "RedshiftConnector.py")
    import_paths = []
    for file in import_files:
        import_paths.append(os.path.join(current_dir, file))

    # model_name = f"{output_profiles_ml_model}_{model_file_name}"
    model_name = f"{model_file_name}"

    if creds["type"] == "snowflake":
        udf_name = "prediction_score"
        connector = SnowflakeConnector()
        session = connector.build_session(creds)
        connector.delete_import_files(session, stage_name, import_paths)
        connector.drop_fn_if_exists(session, udf_name)
    elif creds["type"] == "redshift":
        connector = RedshiftConnector()
        session = connector.build_session(creds)

    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table = connector.get_material_registry_name(session, material_registry_table_prefix)

    latest_model_hash, _ = connector.get_latest_material_hash(session, material_table, features_profiles_model)
    if latest_model_hash != train_model_hash:
        raise ValueError(f"Model hash {train_model_hash} does not match with the latest model hash {latest_model_hash} in the material registry table. Please retrain the model")
    
    latest_hash_df = session.table(material_table)
    latest_hash_df = connector.get_latest_hash_df(session, material_table, latest_model_hash)
    
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX
    latest_seq_no = connector.get_latest_seq_no(latest_hash_df)
    raw_data = connector.get_table(session, f"{material_table_prefix}{features_profiles_model}_{latest_model_hash}_{latest_seq_no}")

    if eligible_users:
        raw_data = connector.filter_columns(raw_data, eligible_users)
        
    arraytype_features = connector.get_arraytype_features_from_table(raw_data)
    ignore_features = utils.merge_lists_to_unique(ignore_features, arraytype_features)
    predict_data = connector.drop_cols(raw_data, ignore_features)

    if len(timestamp_columns) == 0:
        timestamp_columns = connector.get_timestamp_columns_from_table(predict_data, index_timestamp)
    for col in timestamp_columns:
        # predict_data = predict_data.withColumn(col, F.datediff("day", F.col(col), F.col(index_timestamp)))
        predict_data = connector.add_days_diff(predict_data, col, col, index_timestamp)

    # input  = predict_data.drop(label_column, entity_column, index_timestamp)
    input = connector.drop_cols(predict_data, [label_column, entity_column, index_timestamp])
    types = connector.generate_type_hint(input)

    print(model_name)
    print("Caching")
                 
    features = input.columns

    if creds['type'] == 'snowflake':
        @F.pandas_udf(session=session,max_batch_size=10000, is_permanent=True, replace=True,
                stage_location=stage_name, name=udf_name, 
                imports= import_paths+[f"{stage_name}/{model_name}"],
                packages=["snowflake-snowpark-python==0.10.0","typing", "scikit-learn==1.1.1", "xgboost==1.5.0", "numpy==1.23.1","pandas","joblib", "cachetools", "PyYAML"])
        def predict_scores(df: types) -> T.PandasSeries[float]:
            df.columns = features
            predict_proba = predict_helper(df, model_name)
            return predict_proba
        
        prediction_procedure = predict_scores
    elif creds['type'] == 'redshift':
        def predict_scores_rs(df: types) -> pd.Series:
            df = df[features]
            df = df.to_pandas()
            predict_proba = predict_helper(df, model_name)
            return predict_proba
        prediction_procedure = predict_scores_rs
    
    preds_with_percentile = connector.call_prediction_procedure(predict_data, prediction_procedure, entity_column, index_timestamp, score_column_name, percentile_column_name, output_label_column, train_model_id, prob_th, input)
    connector.write_table(preds_with_percentile, output_tablename, write_mode="overwrite")
    print("Everything worked yayayayayayayayay!")
    

if __name__ == "__main__":
    homedir = os.path.expanduser("~")
    with open(os.path.join(homedir, ".pb/siteconfig.yaml"), "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
        print(creds["schema"])
        aws_config=None
        output_folder = 'output/dev/seq_no/7'
        model_path = f"{output_folder}/train_output.json"
        
    predict(creds, aws_config, model_path, None, "test_can_delet",None)