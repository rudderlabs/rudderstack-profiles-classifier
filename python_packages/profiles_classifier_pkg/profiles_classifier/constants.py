class constants:
    MATERIAL_REGISTRY_TABLE_PREFIX = 'MATERIAL_REGISTRY'
    MATERIAL_TABLE_PREFIX = "material_"
    MODEL_FILE_NAME = 'classifier.joblib'
    STAGE_NAME = "@ml_models5"
    METRICS_TABLE = "CLASSIFIER_METRICS"
    CARDINAL_FEATURE_THRESOLD = 0.01
    POSITIVE_BOOLEAN_FLAGS = ["1", 1, "TRUE", "True", "true", "T", "t", True, "YES", "Yes", "yes", "Y", "y"]
    LOCAL_STORAGE_DIR = "data"
    SF_LOCAL_STORAGE_DIR = "/tmp"
    rs_dtypes = '{"text": "character varying(65535)", "num": "float", "bool": "bool", "timestamp": "timestamp without time zone"}'
    PB = '/venv/bin/pb' # Location of pb executable in rudder-sources