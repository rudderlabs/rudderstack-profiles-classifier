import unittest
from src.predictions.profiles_mlcorelib.py_native.training import TrainingModel


class TestTrainingModel(unittest.TestCase):
    def testInputsPopulation(self) -> None:
        label_column = "models/ev1"
        test_cases = [
            {
                "name": "label column is part of input",
                "inputs": [label_column, "models/ev2"],
            },
            {
                "name": "label column not part of input",
                "inputs": ["models/ev2"],
            },
        ]
        for case in test_cases:
            with self.subTest(case=case["name"]):
                build_spec = {
                    "ml_config": {
                        "data": {
                            "label_column": label_column,
                        },
                    },
                    "inputs": case["inputs"],
                }
                model = TrainingModel(build_spec, 0, "")
                self.assertEqual(
                    sorted(model.build_spec["inputs"]),
                    sorted(["models/ev1", "models/ev2"]),
                )
