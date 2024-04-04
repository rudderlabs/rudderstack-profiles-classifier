MODEL_FILE_NAME = "classifier.joblib"
CARDINAL_FEATURE_THRESOLD = 0.01
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
REMOTE_DIR = "/home/ec2-user"
INSTANCE_ID = "i-001c6544decab0fa3"
EC2_TEMP_OUTPUT_JSON = "train_results.json"
SSM_SLEEP_TIME = 5
K8S_WH_CREDS_KEY = "WAREHOUSE_CREDS"
K8S_MODE = "K8S"
CI_MODE = "ci"
K8S_TIMEOUT_IN_SEC = 120
LOCAL_MODE = "local"
WAREHOUSE_MODE = "native-warehouse"
RUDDERSTACK_MODE = "rudderstack-infra"
ML_CORE_PYTHON_PATH = "src.predictions.rudderstack_predictions.ml_core"
ML_CORE_PYNATIVE_PATH = "rudderstack_predictions.ml_core"
MIN_PB_VERSION = 53

# Smart data preparation

# Min training data requirements
# For classification its minimum negative sample
# For regression its minimum number of samples
MIN_NUM_OF_SAMPLES = 5000

# Material date format
MATERIAL_DATE_FORMAT = "%Y-%m-%d"

from typing import NamedTuple


class TrainTablesInfo(NamedTuple):
    feature_table_name: str
    feature_table_date: int
    label_table_name: str
    label_table_date: int
