import utils
import constants
from logger import logger
from Processor import Processor
from typing import Any, List, Tuple, Union

class SnowflakeProcessor(Processor):

    def _call_procedure(self, *args, **kwargs):
        """Calls the given procedure on the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            args (list): List of arguments to be passed to the procedure
        
        Returns:
            Results of the procedure call
        """
        session = kwargs.get('session', None)
        if session == None:
            raise Exception("Session object not found")
        return session.call(*args)