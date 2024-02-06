from src.processors.AWSProcessor import AWSProcessor
from src.processors.LocalProcessor import LocalProcessor
from src.processors.SnowflakeProcessor import SnowflakeProcessor

LOCAL_MODE = "local"
WAREHOUSE_MODE = "native-warehouse"
RUDDERSTACK_MODE = "rudderstack-infra"

processor_mode_map = {
    LOCAL_MODE: LocalProcessor,
    WAREHOUSE_MODE: SnowflakeProcessor,
    RUDDERSTACK_MODE: AWSProcessor,
}
