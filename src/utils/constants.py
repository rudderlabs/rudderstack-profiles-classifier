MODEL_FILE_NAME = "classifier.joblib"
CARDINAL_FEATURE_THRESOLD = 0.01
MIN_SAMPLES_FOR_TRAINING = 10
METRICS_TABLE = "TRAINING_METRICS_v4"
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
CLASSIFIER_MIN_LABEL_PROPORTION = 0.001  # TODO: Change to 0.05 and next value to 0.95 once actual customers start using this
CLASSIFIER_MAX_LABEL_PROPORTION = 0.999
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

# Smart data preparation

# Min training data requirements
# For classification
MIN_NUM_OF_NEGATIVE_LABELS = 800
# For Regression
MIN_NUM_OF_ROWS = 1000

from typing import NamedTuple


class TrainTablesInfo(NamedTuple):
    feature_table_name: str
    feature_table_date: int
    label_table_name: str
    label_table_date: int
