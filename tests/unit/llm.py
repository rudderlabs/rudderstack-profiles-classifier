import unittest
from src.predictions.profiles_mlcorelib.py_native.llm import LLMModel, Utility


class TestLLMModelValidation(unittest.TestCase):
    def setUp(self):
        self.build_spec = {
            "entity_key": "user",
            "prompt": "sample prompt {var_inputs[0]}",
            "var_inputs": ["input1", "input2"],
            "sql_inputs": ["query1", "query2"],
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
            "Maximum index 3 is out of range for the inputs list.",
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
        self.build_spec[
            "prompt"
        ] = "sample prompt {var_inputs[0]} {sql_inputs[0]} {sql_inputs[1]}"
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

    def test_max_index_eligible_users(self):
        # Testing max index sql inputs
        # First, test with valid indices
        self.build_spec["prompt"] = "sample prompt"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Testing the case for correct maximum index
        self.build_spec["prompt"] = "sample prompt {sql_inputs[0]} {var_inputs[1]}"
        self.build_spec["eligible_users"] = "sample eligible users {var_inputs[1]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        llm_model.validate()

        # Now, test with invalid indices
        self.build_spec["prompt"] = "sample prompt {sql_inputs[0]}"
        self.build_spec["eligible_users"] = "sample eligible users {var_inputs[2]}"
        llm_model = LLMModel(self.build_spec, self.schema_ver, self.pb_version)
        with self.assertRaises(ValueError) as context:
            llm_model.validate()
        self.assertEqual(
            str(context.exception),
            "Maximum index 2 is out of range for the inputs list.",
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


class TestLLMModelRecipeUtility(unittest.TestCase):
    def setUp(self):
        self.build_spec = {
            "entity_key": "user",
            "prompt": "sample prompt",
            "var_inputs": ["input1", "input2"],
            "eligible_users": "sample eligible users",
            "sql_inputs": ["query1", "query2"],
        }
        self.input_columns_vars = ["input1", "input2"]
        self.sql_inputs_df = ['[{"a":1,"b":4},{"a":2,"b":5}]']
        self.utils = Utility()

    def test_replace_placeholders_in_correct_case(self):
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)

    def test_replace_placeholders_with_empty_var_inputs(self):
        self.input_columns_vars = []
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)

    def test_replace_placeholders_with_var_inputs_references_in_task_prompt(self):
        self.build_spec["prompt"] = "sample prompt {var_inputs[0]} {var_inputs[1]}"
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            "sample prompt ' ||input1|| ' ' ||input2|| '",
            self.build_spec["eligible_users"],
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)

    def test_replace_placeholders_with_sql_inputs_references_in_task_prompt(self):
        self.build_spec["prompt"] = "sample prompt {sql_inputs[0]}"
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            f"sample prompt {self.sql_inputs_df[0]}",
            self.build_spec["eligible_users"],
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)

    def test_replace_placeholders_with_var_and_sql_inputs_references_in_task_prompt(
        self,
    ):
        self.build_spec[
            "prompt"
        ] = "sample prompt {var_inputs[0]} {var_inputs[1]} {sql_inputs[0]}"
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            f"sample prompt ' ||input1|| ' ' ||input2|| ' {self.sql_inputs_df[0]}",
            self.build_spec["eligible_users"],
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)

    def test_replace_placeholders_with_var_and_sql_inputs_references_in_task_prompt_and_var_reference_in_eligible_users(
        self,
    ):
        self.build_spec[
            "prompt"
        ] = "sample prompt {var_inputs[0]} {var_inputs[1]} {sql_inputs[0]}"
        self.build_spec[
            "eligible_users"
        ] = "sample eligible users {var_inputs[0]}, {var_inputs[1]}"
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            f"sample prompt ' ||input1|| ' ' ||input2|| ' {self.sql_inputs_df[0]}",
            f"sample eligible users input1, input2",
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)

    def test_replace_placeholders_with_var_and_sql_inputs_references_in_task_prompt_and_empty_eligible_users(
        self,
    ):
        self.build_spec[
            "prompt"
        ] = "sample prompt {var_inputs[0]} {var_inputs[1]} {sql_inputs[0]}"
        self.build_spec["eligible_users"] = ""
        var_inputs_indices = self.utils.get_index_list(self.build_spec["prompt"])
        eligible_users_indices = self.utils.get_index_list(
            self.build_spec["eligible_users"]
        )

        replaced_prompt_tuple = self.utils.replace_placeholders(
            var_inputs_indices,
            eligible_users_indices,
            self.build_spec["prompt"],
            self.build_spec["eligible_users"],
            self.input_columns_vars,
            self.sql_inputs_df,
        )
        replaced_prompt_tuple_expected = (
            f"sample prompt ' ||input1|| ' ' ||input2|| ' {self.sql_inputs_df[0]}",
            self.build_spec["eligible_users"],
        )

        # Assert the result
        self.assertEqual(replaced_prompt_tuple, replaced_prompt_tuple_expected)
