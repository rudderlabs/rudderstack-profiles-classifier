from .LocalProcessor import LocalProcessor
from .SnowflakeProcessor import SnowflakeProcessor

from ..utils import constants


class ProcessorFactory:
    def create(mode: str, trainer, connector, session, ml_core_path: str):
        processor = None
        if mode == constants.RUDDERSTACK_MODE:
            # Lazy load K8sProcessor since kubernetes might not be installed in all environments
            from .K8sProcessor import K8sProcessor

            return K8sProcessor(trainer, connector, session, ml_core_path)
        elif mode == constants.WAREHOUSE_MODE:
            return SnowflakeProcessor(trainer, connector, session, ml_core_path)
        elif mode == constants.LOCAL_MODE:
            return LocalProcessor(trainer, connector, session, ml_core_path)
        else:
            raise Exception(f"Invalid processor mode {mode}")
