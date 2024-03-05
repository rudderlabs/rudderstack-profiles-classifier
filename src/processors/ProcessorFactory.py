from src.processors.LocalProcessor import LocalProcessor
from src.processors.SnowflakeProcessor import SnowflakeProcessor

import src.utils.constants as constants


class ProcessorFactory:
    def create(mode: str, trainer, connector, session):
        processor = None
        if mode == constants.RUDDERSTACK_MODE:
            # Lazy load K8sProcessor since kubernetes might not be installed in all environments
            from src.processors.K8sProcessor import K8sProcessor

            processor = K8sProcessor
        elif mode == constants.WAREHOUSE_MODE:
            processor = SnowflakeProcessor
        elif mode == constants.LOCAL_MODE:
            processor = LocalProcessor
        else:
            raise Exception(f"Invalid processor mode {mode}")
        return processor(trainer, connector, session)
