import unittest
from src.predictions.rudderstack_predictions.py_native.llm import LLMModel


class TestLLMModelValidation(unittest.TestCase):
    def setUp(self):
        self.build_spec = {
            "prompt": "sample prompt input{[0]}",
            "prompt_inputs": ["input1", "input2"],
            "llm_model_name": None,
        }
        self.schema_ver = 53
        self.pb_version = "v0.11.2"

    def test_llm_model_name(self):
        # Defining Schema
        # Testing empty model name
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        default_llm_model_name = "llama2-70b-chat"
        self.assertEqual(default_llm_model_name, self.build_spec["llm_model_name"])

        # Testing invalid model name
        self.build_spec["llm_model_name"] = "invalid_model"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        with self.assertRaises(ValueError) as context:
            llm_model.validate()
        self.assertEqual(
            str(context.exception),
            "Invalid llm model name: invalid_model. Valid options are: mistral-large, reka-flash, mixtral-8x7b, llama2-70b-chat, mistral-7b, gemma-7b",
        )

    def test_prompt_length_validation(self):
        # Create a prompt with length exceeding the limit
        self.build_spec["prompt"] = (
            "a " * 40000
        )  # Assuming the token limit for "llama2-70b-chat" is 4096

        # Ensure that ValueError is raised due to prompt length exceeding the limit
        with self.assertRaises(ValueError) as context:
            llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
            llm_model.validate()
        self.assertEqual(
            str(context.exception),
            "The prompt exceeds the token limit for model 'llama2-70b-chat'. Maximum allowed tokens: 4096",
        )
