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

    def test_max_index_prompt_inputs(self):
        # Testing max index prompt inputs
        # First, test with valid indices
        self.build_spec["prompt"] = "sample prompt"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Testing the case for correct maximum index
        self.build_spec["prompt"] = "sample prompt {input1[0]} {input2[1]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Now, test with invalid indices
        self.build_spec["prompt"] = "sample prompt {input1[2]} {input2[3]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        with self.assertRaises(ValueError) as context:
            llm_model.validate()
        self.assertEqual(
            str(context.exception),
            "Maximum index 3 is out of range for input_columns list.",
        )
