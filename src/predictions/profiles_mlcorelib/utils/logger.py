import logging


class LoggerClass:
    def __init__(self):
        self._logger = self.default_logger()

    def default_logger(self):
        logger = logging.getLogger("churn_prediction")
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def get(self):
        return self._logger

    def set_logger(self, newLogger):
        self._logger = newLogger


# Don't use "warning" method of the logger since it is not impelmented by the pynative logger
logger = LoggerClass()
