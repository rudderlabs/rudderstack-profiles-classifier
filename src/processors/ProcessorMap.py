from src.processors.AWSProcessor import AWSProcessor
from src.processors.LocalProcessor import LocalProcessor
from src.processors.SnowflakeProcessor import SnowflakeProcessor

import src.utils.constants as constants

processor_mode_map = {
    constants.LOCAL_MODE: LocalProcessor,
    constants.WAREHOUSE_MODE: SnowflakeProcessor,
    constants.RUDDERSTACK_MODE: AWSProcessor,
}
