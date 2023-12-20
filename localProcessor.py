from typing import Any, List, Tuple, Union

import utils
import constants
from logger import logger
from Processor import Processor

class LocalProcessor(Processor):
    def _call_procedure(self, *args, **kwargs):
        """Calls the given function for training

        Args:
            cursor (redshift_connector.cursor.Cursor): Redshift connection cursor for warehouse access
            args (list): List of arguments to be passed to the training function
            kwargs (dict): Dictionary of keyword arguments to be passed to the training function
        
        Returns:
            Results of the training function
        """
        train_function = args[0]
        args = args[1:]
        return train_function(*args, **kwargs)