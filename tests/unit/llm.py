import unittest
from src.predictions.profiles_mlcorelib.py_native.llm import LLMModel


class TestLLMModelValidation(unittest.TestCase):
    def setUp(self):
        self.build_spec = {
            "prompt": "sample prompt input{[0]}",
            "prompt_inputs": ["input1", "input2"],
            "llm_model_name": None,
        }
        self.schema_ver = 53
        self.pb_version = "v0.11.2"

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
