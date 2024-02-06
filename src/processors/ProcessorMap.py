from src.processors.AWSProcessor import AWSProcessor
from src.processors.LocalProcessor import LocalProcessor
from src.processors.SnowflakeProcessor import SnowflakeProcessor

processor_mode_map = {
    "local": LocalProcessor,
    "native-warehouse": SnowflakeProcessor,
    "rudderstack-infra": AWSProcessor,
}
