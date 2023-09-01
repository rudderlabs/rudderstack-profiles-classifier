
import sys
import pandas as pd
import numpy as np
import cachetools
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import average_precision_score
from sklearn.compose import ColumnTransformer
from xgboost import XGBClassifier
import joblib
import os
from snowflake.snowpark.session import Session
from snowflake.snowpark.window import Window
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from typing import List
from snowflake.snowpark.functions import sproc
import snowflake.snowpark
import time
from typing import Tuple, List, Union
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import get_metrics, load_yaml, remap_credentials, combine_config, get_material_registry_name, delete_import_files
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, f1_score
import constants
from logger import logger
import yaml
import json
import datetime



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
        
def drop_ignored_features(df: snowflake.snowpark.Table, 
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



def drop_fn_if_exists(session: snowflake.snowpark.Session, 
                      fn_name: str) -> bool:
    """Snowflake caches the functions and it reuses these next time. To avoid the caching, we manually search for the same function name and drop it before we create the udf.

    Args:
        session (snowflake.snowpark.Session): snowpark session
        fn_name (str): 

    Returns:
        bool: 
    """
    fn_list = session.sql(f"show user functions like '{fn_name}'").collect()
    if len(fn_list) == 0:
        logger.info(f"Function {fn_name} does not exist")
        return True
    else:
        logger.info("Function name match found. Dropping all functions with the same name")
        for fn in fn_list:
            fn_signature = fn["arguments"].split("RETURN")[0]
            drop = session.sql(f"DROP FUNCTION IF EXISTS {fn_signature}")
            logger.info(drop.collect()[0].status)
        logger.info("All functions with the same name dropped")
        return True
    


def predict(creds:dict, aws_config: dict, model_path: str, inputs: str, output_tablename : str, config: dict) -> None:
    """Generates the prediction probabilities and save results for given model_path

    Args:
        creds (dict): credentials to access the data warehouse - in same format as site_config.yaml from profiles
        aws_config (dict): aws credentials - not required for snowflake. only used for redshift
        model_path (str): path to the file where the model details including model id etc are present. Created in training step
        inputs (str): Not being used currently. Can pass a blank string. For future support
        output_tablename (str): name of output table where prediction results are written
        config (dict): configs from profiles.yaml which should overwrite corresponding values from data_prep.yaml file

    Returns:
        None: save the prediction results but returns nothing
    """
    connection_parameters = remap_credentials(creds)
    session = Session.builder.configs(connection_parameters).create()
    stage_name = constants.STAGE_NAME
    model_file_name = constants.MODEL_FILE_NAME
    print(model_file_name)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    notebook_config = load_yaml(os.path.join(current_dir, "config/data_prep.yaml"))
    merged_config = combine_config(notebook_config, config)

    score_column_name = merged_config['outputs']['column_names']['score']
    percentile_column_name = merged_config['outputs']['column_names']['percentile']
    model_name_prefix = merged_config["data"]["model_name_prefix"]
    label_column = merged_config["data"]["label_column"]
    index_timestamp = merged_config["data"]["index_timestamp"]
    eligible_users = merged_config["data"]["eligible_users"]
    ignore_feature = merged_config["preprocessing"]["ignore_features"]
    timestamp_columns = merged_config["preprocessing"]["timestamp_columns"]
    entity_column = merged_config["data"]["entity_column"]
    model_name = merged_config["data"]["model_name"]
    udf_name = "churn_predict"

    x_train_sample = session.table(f"{model_name_prefix}_train")
    types = generate_type_hint(x_train_sample.drop(label_column, entity_column))
    current_dir = os.path.dirname(os.path.abspath(__file__))
    predict_path = os.path.join(current_dir, 'predict.py')
    utils_path = os.path.join(current_dir, 'utils.py')
    constants_path = os.path.join(current_dir, 'constants.py')

    import_paths = [utils_path, constants_path]
    delete_import_files(session, stage_name, import_paths)
    
    print("Caching")
    
    #@cachetools.cached(cache={})
    def load_model(filename: str):
        """session.import adds the staged model file to an import directory. We load the model file from this location

        Args:
            filename (str): path for the model_file_name

        Returns:
            _type_: return the trained model after loading it
        """
        import_dir = sys._xoptions.get("snowflake_import_directory")     
        assert import_dir.startswith('/home/udf/')
        if import_dir:
              with open(os.path.join(import_dir, filename), 'rb') as file:
                     m = joblib.load(file)
                     return m
                 
    features = x_train_sample.drop(label_column, entity_column).columns
    drop_fn_if_exists(session, udf_name)
    @F.pandas_udf(session=session,max_batch_size=10000, is_permanent=True, replace=True,
              stage_location=stage_name, name=udf_name, 
              imports= import_paths+[f"{stage_name}/{model_file_name}"],
              packages=["snowflake-snowpark-python==0.10.0", "scikit-learn==1.1.1", "xgboost==1.5.0", "numpy==1.23.1","pandas","joblib", "cachetools", "PyYAML"])
    def predict_scores(df: types) -> T.PandasSeries[float]:
        trained_model = load_model(model_file_name)
        df.columns = features
        categorical_columns = list(df.select_dtypes(include='object'))
        numeric_columns = list(df.select_dtypes(exclude='object'))
        df[numeric_columns] = df[numeric_columns].replace({pd.NA: np.nan})
        df[categorical_columns] = df[categorical_columns].replace({pd.NA: None})        
        return trained_model.predict_proba(df)[:,1]


    f = open(model_path, "r")
    results = json.load(f)
    model_hash = results["config"]["material_hash"]
    model_id = results["model_info"]["model_id"]
    current_ts = datetime.datetime.now()

    material_registry_table_prefix = constants.MATERIAL_REGISTRY_TABLE_PREFIX
    material_table = get_material_registry_name(session, material_registry_table_prefix)
    latest_hash_df = session.table(material_table).filter(F.col("model_hash") == model_hash)
    
    material_table_prefix = constants.MATERIAL_TABLE_PREFIX
    latest_seq_no = latest_hash_df.sort(F.col("end_ts"), ascending=False).select("seq_no").collect()[0].SEQ_NO
    raw_data = session.table(f"{material_table_prefix}{model_name}_{model_hash}_{latest_seq_no}")

    if eligible_users:
        raw_data = raw_data.filter(eligible_users)

    predict_data = drop_ignored_features(raw_data, ignore_feature)
    for col in timestamp_columns:
        predict_data = predict_data.withColumn(col, F.datediff("day", F.col(col), F.col(index_timestamp)))

    input  = predict_data.drop(label_column, entity_column, index_timestamp)
    
    preds = (predict_data.select(entity_column, index_timestamp, predict_scores(*input).alias(score_column_name))
             .withColumn("model_id", F.lit(model_id)))

    preds_with_percentile = preds.withColumn(percentile_column_name, F.percent_rank().over(Window.order_by(F.col(score_column_name)))).select(entity_column, index_timestamp, score_column_name, percentile_column_name, "model_id")
    preds_with_percentile.write.mode("overwrite").save_as_table(output_tablename)
    

if __name__ == "__main__":
    with open("/Users/ambuj/.pb/siteconfig.yaml", "r") as f:
        creds = yaml.safe_load(f)["connections"]["shopify_wh"]["outputs"]["dev"]
        print(creds["schema"])
        aws_config=None
        output_folder = 'output/dev/seq_no/2'
        model_path = f"{output_folder}/train_output.json"
        
    predict(creds, aws_config, model_path, None, "test_can_delet",None)