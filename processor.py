from logger import logger
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union

import utils
import constants
from MLTrainer import MLTrainer
from Connector import Connector

import snowflake.snowpark
import redshift_connector
import redshift_connector.cursor

class processor(ABC):
    def __init__(self, trainer: MLTrainer, connector: Connector, session: Union[snowflake.snowpark.Session, redshift_connector.cursor.Cursor]):
        self.trainer = trainer
        self.connector = connector
        self.session = session

    @abstractmethod
    def prepare_and_train(self, material_names):
        pass