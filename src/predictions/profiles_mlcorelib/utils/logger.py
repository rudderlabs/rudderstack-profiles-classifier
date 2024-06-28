import logging

class LoggerClass():

    def __init__(self):
        self._logger = self.default_logger()

    def default_logger(self):
        # Define the logger with the desired name
        logger = logging.getLogger("churn_prediction")

        # Configure the logger with the desired log level and handlers
        logger.setLevel(logging.DEBUG)

        # file_handler = logging.FileHandler("classifier.log")
        # file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def get(self):
        return self._logger

    def set_logger(self, newLogger):
        self._logger = newLogger

logger = LoggerClass()