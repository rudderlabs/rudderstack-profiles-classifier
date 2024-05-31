import unittest
from src.predictions.profiles_mlcorelib.py_native.llm import LLMModel


class TestLLMModelValidation(unittest.TestCase):
    def setUp(self):
        self.build_spec = {
            "prompt": "sample prompt input{[0]}",
            "var_inputs": ["input1", "input2"],
            "sql_inputs": ["query1", "query2"],
            "llm_model_name": None,
        }
        self.schema_ver = 53
        self.pb_version = "v0.11.2"

    def test_max_index_var_inputs(self):
        # Testing max index var inputs
        # First, test with valid indices
        self.build_spec["prompt"] = "sample prompt"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Testing the case for correct maximum index
        self.build_spec["prompt"] = "sample prompt {var_inputs[0]} {var_inputs[1]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Now, test with invalid indices
        self.build_spec["prompt"] = "sample prompt {var_inputs[2]} {var_inputs[3]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        with self.assertRaises(ValueError) as context:
            llm_model.validate()
        self.assertEqual(
            str(context.exception),
            "Maximum index 3 is out of range for var_inputs list.",
        )

    def test_max_index_sql_inputs(self):
        # Testing max index sql inputs
        # First, test with valid indices
        self.build_spec["prompt"] = "sample prompt"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Testing the case for correct maximum index with sql_inputs only
        self.build_spec["prompt"] = "sample prompt {sql_inputs[0]} {sql_inputs[1]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Testing the case for correct maximum index with sql_inputs and var_inputs both
        self.build_spec["prompt"] = (
            "sample prompt {var_inputs[0]} {sql_inputs[0]} {sql_inputs[1]}"
        )
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Now, test with invalid indices
        self.build_spec["prompt"] = "sample prompt {sql_inputs[2]} {sql_inputs[3]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        with self.assertRaises(ValueError) as context:
            llm_model.validate()
        self.assertEqual(
            str(context.exception),
            "Maximum index 3 is out of range for sql_inputs list.",
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
