from AWSProcessor import AWSProcessor
from LocalProcessor import LocalProcessor
from SnowflakeProcessor import SnowflakeProcessor

LOCAL_MODE = "local"
WAREHOUSE_MODE = "native-warehouse"
RUDDERSTACK_MODE = "rudderstack-infra"

processor_mode_map = {
    LOCAL_MODE: LocalProcessor,
    WAREHOUSE_MODE: SnowflakeProcessor,
    RUDDERSTACK_MODE: AWSProcessor,
}
