
import unittest
from utils import load_yaml

class TestDefaults(unittest.TestCase):

    def test_default_config(self):
        default_config = load_yaml('./config/model_configs.yaml')
        self.assertEqual(default_config["data"]["task"], "classification")