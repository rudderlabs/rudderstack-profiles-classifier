from ..processors.AWSProcessor import AWSProcessor
from ..processors.LocalProcessor import LocalProcessor
from ..processors.SnowflakeProcessor import SnowflakeProcessor

from ..utils import constants

processor_mode_map = {
    constants.LOCAL_MODE: LocalProcessor,
    constants.WAREHOUSE_MODE: SnowflakeProcessor,
    constants.RUDDERSTACK_MODE: AWSProcessor,
}
