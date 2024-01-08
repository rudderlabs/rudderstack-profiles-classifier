MATERIAL_REGISTRY_TABLE_PREFIX = 'MATERIAL_REGISTRY'
MATERIAL_TABLE_PREFIX = "material_"
MODEL_FILE_NAME = 'classifier.joblib'
CARDINAL_FEATURE_THRESOLD = 0.01
MIN_SAMPLES_FOR_TRAINING = 10
METRICS_TABLE = "TRAINING_METRICS_v4"
POSITIVE_BOOLEAN_FLAGS = ["1", 1, "TRUE", "True", "true", "T", "t", True, "YES", "Yes", "yes", "Y", "y"]
PROCESSOR_MODE_PREFERENCE = {"snowflake":["native-warehouse"], "redshift":["local", "rudderstack-infra"]}
LOCAL_STORAGE_DIR = "data"
SF_LOCAL_STORAGE_DIR = "/tmp"
rs_dtypes = '{"text": "character varying(65535)", "num": "float", "bool": "bool", "timestamp": "timestamp without time zone"}'
PB = '/venv/bin/pb' # Location of pb executable in rudder-sources
REMOTE_DIR = "/home/ec2-user"
INSTANCE_ID = "i-001c6544decab0fa3"
EC2_TEMP_OUTPUT_JSON = "train_results.json"
S3_BUCKET = "ml-usecases-poc-srinivas"
REGION_NAME="us-east-1"
S3_PATH = "test_export/"