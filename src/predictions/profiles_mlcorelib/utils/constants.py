# Pb Preferences related constants - around consent and telemetry
from pathlib import Path

CONFIG_DIR = Path.home() / ".pb"
PREFERENCES_FILE = "preferences.yaml"
PREFERENCES_PATH = CONFIG_DIR / PREFERENCES_FILE
TELEMETRY_OPT_OUT_KEY = "telemetry_opt_out"
TELEMETRY_CONSENT_SHOWN_KEY = "telemetry_consent_shown"
LLM_CONSENT_KEY = "llm_consent"
# TODO: Modify them to a prod settings. These are currently from a testing env
WRITE_KEY = "2oqUj9oDjSSp5dGpiCr5Q5IGZk6"
DATA_PLANE_URL = "https://rudderstack-dataplane.rudderstack.com"


# Propensity model related constants
MODEL_FILE_NAME = "rs_predictions_trained_model"
CARDINAL_FEATURE_THRESHOLD = 0.01
MIN_SAMPLES_FOR_TRAINING = 10
POSITIVE_BOOLEAN_FLAGS = [
    "1",
    1,
    "TRUE",
    "True",
    "true",
    "T",
    "t",
    True,
    "YES",
    "Yes",
    "yes",
    "Y",
    "y",
]
LOCAL_STORAGE_DIR = "data"
SF_LOCAL_STORAGE_DIR = "/tmp"
rs_dtypes = '{"text": "character varying(65535)", "num": "float", "bool": "bool", "timestamp": "timestamp without time zone"}'
CLASSIFIER_MIN_LABEL_PROPORTION = 0.05
CLASSIFIER_MAX_LABEL_PROPORTION = 0.95
REGRESSOR_MIN_LABEL_DISTINCT_VALUES = 3
TRAIN_JSON_RESULT_FILE = "train_results.json"
K8S_WH_CREDS_KEY = "WAREHOUSE_CREDS"
CI_MODE = "ci"
K8S_TIMEOUT_IN_SEC = 120
LOCAL_MODE = "local"
WAREHOUSE_MODE = "native-warehouse"
RUDDERSTACK_MODE = "rudderstack-infra"
ML_CORE_PYTHON_PATH = "src.predictions.profiles_mlcorelib.ml_core"
ML_CORE_PYNATIVE_PATH = "profiles_mlcorelib.ml_core"
MIN_PB_VERSION = 53

PRED_OUTPUT_DF_COLUMNS = {
    "classification": {
        "label": "prediction_label",
        "score": "prediction_score",
    },
    "regression": {
        "score": "prediction_label",
    },
}

import shap

EXPLAINER_MAP = {
    "RidgeClassifier": shap.LinearExplainer,
    "AdaBoostClassifier": shap.KernelExplainer,
    "RandomForestClassifier": shap.TreeExplainer,
    "GradientBoostingClassifier": shap.TreeExplainer,
    "XGBClassifier": shap.TreeExplainer,
    "Ridge": shap.LinearExplainer,
    "GradientBoostingRegressor": shap.TreeExplainer,
    "RandomForestRegressor": shap.TreeExplainer,
    "XGBRegressor": shap.TreeExplainer,
    "AdaBoostRegressor": shap.TreeExplainer,
    # "LogisticRegression": shap.LinearExplainer,
    # "KNeighborsClassifier": shap.KernelExplainer,
    # "GaussianNB": shap.KernelExplainer,
    # "DecisionTreeClassifier": shap.TreeExplainer,
    # "LinearSVC": shap.LinearExplainer,
    # "SVC": shap.KernelExplainer,
    # "LinearDiscriminantAnalysis": shap.LinearExplainer,
    # "ExtraTreesClassifier": shap.TreeExplainer,
    # "LGBMClassifier": shap.TreeExplainer,
}
# Smart data preparation

# Min training data requirements
# For classification its minimum negative sample
# For regression its minimum number of samples
MIN_NUM_OF_SAMPLES = 5000

# Material date format
MATERIAL_DATE_FORMAT = "%Y-%m-%d"

# timestamp format for null transformation
NULL_TRANSFORMATION_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

from typing import NamedTuple

SNOWFLAKE_TRAINING_PACKAGES = [
    "snowflake-snowpark-python==1.11.1",
    "scikit-learn==1.1.1",
    "xgboost==1.5.0",
    "joblib==1.2.0",
    "PyYAML==6.0.1",
    "numpy==1.23.1",
    "pandas==1.5.3",
    "hyperopt==0.2.7",
    "shap==0.46.0",
    "matplotlib==3.7.1",
    "seaborn==0.12.0",
    "scikit-plot==0.3.7",
    "pycaret<=3.3.0",
    "cryptography==42.0.2",
]


class TrainTablesInfo(NamedTuple):
    feature_table_name: str
    feature_table_date: int
    label_table_name: str
    label_table_date: int
