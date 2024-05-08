import unittest
from unittest.mock import patch
from src.predictions.rudderstack_predictions.py_native.llm import LLMModel


class TestLLMModelValidation(unittest.TestCase):
    build_spec = {
        "prompt": "sample prompt input{[0]}",
        "prompt_inputs": ["input1", "input2"],
        "llm_model_name": None
    }
    schema_ver = 53
    pb_version = "v0.11.2"

    @patch('tennr.re')
    def test_llm_model_name(self, mock_re):
        # Defining Schema
        build_spec = {
            "prompt": "sample prompt input{[0]}",
            "prompt_inputs": ["input1", "input2"],
            "llm_model_name": None
        }
        # Testing empty model name
        llm_model = LLMModel(build_spec, self.schema_ver, self.pb_version)
        mock_re.findall.return_value = [("input1", "0"), ("input2", "1")]
        llm_model.validate()

        default_llm_model_name = "llama2-70b-chat"
        self.assertEqual(default_llm_model_name, build_spec["llm_model_name"])
        # Testing invalid model name
        build_spec['llm_model_name'] = "invalid_model"
        llm_model = LLMModel(build_spec, self.schema_ver, self.pb_version)
        with self.assertRaises(ValueError):
            llm_model.validate()

    @patch('tennr.re')
    def test_max_index_prompt_inputs(self, mock_re):
        # Mocking LLMModel class
        llm_model = LLMModel(self.build_spec, 53, "v0.11.2")
        input_indices = mock_re.findall.return_value = [("input1", "0"), ("input2", "1")]
        max_index_direct = len(self.build_spec["prompt_inputs"]) - 1
        # Validate the SnowflakeCortexModel
        llm_model.validate()

        # Get the max index calculated by SnowflakeCortexModel
        max_index_snowflake = max(int(index) for _, index in input_indices)
        are_equal = max_index_direct == max_index_snowflake

        # Assert
        self.assertTrue(are_equal)

        self.build_spec['prompt'] = "sample prompt {input1[2]} {input2[3]}"
        llm_model = LLMModel(self.build_spec, 53, "v0.11.2")
        mock_re.findall.return_value = [("input1", "2"), ("input2", "3")]
        with self.assertRaises(ValueError):
            llm_model.validate()
