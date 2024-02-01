from AWSProcessor import AWSProcessor
from LocalProcessor import LocalProcessor
from SnowflakeProcessor import SnowflakeProcessor

processor_mode_map = {
    "local": LocalProcessor,
    "native-warehouse": SnowflakeProcessor,
    "rudderstack-infra": AWSProcessor,
}
