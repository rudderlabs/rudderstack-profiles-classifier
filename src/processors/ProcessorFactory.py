from src.processors.LocalProcessor import LocalProcessor
from src.processors.SnowflakeProcessor import SnowflakeProcessor

import src.utils.constants as constants


class ProcessorFactory:
    def create(self, mode: str):
        if mode == constants.RUDDERSTACK_MODE:
            # Lazy load K8sProcessor since kubernetes might not be installed in all environments
            from src.processors.K8sProcessor import K8sProcessor

            return K8sProcessor()
        elif mode == constants.WAREHOUSE_MODE:
            return SnowflakeProcessor()
        elif mode == constants.LOCAL_MODE:
            return LocalProcessor()
        else:
            raise Exception(f"Invalid processor mode {mode}")
