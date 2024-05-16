import unittest

from src.predictions.rudderstack_predictions.ml_core.preprocess_and_predict import (
    prepare_data_prep_pipeline,
    data_prep_wrapper_func,
)


class TestBuildDataPrepPipeline(unittest.TestCase):
    def test_empty_steps(self):
        """Tests building a pipeline with an empty steps list."""
        steps = []
        pipeline = prepare_data_prep_pipeline(steps)
        self.assertEqual(pipeline.steps, [])

    def test_single_step(self):
        """Tests building a pipeline with a single step."""

        def mock_func(data):
            return data + "_transformed"

        step = (mock_func, {})
        steps = [step]
        pipeline = prepare_data_prep_pipeline(steps)

        result = pipeline.fit_transform("input_data")
        self.assertEqual(result, "input_data_transformed")

    def test_multiple_steps(self):
        """Tests building a pipeline with multiple steps."""

        def mock_func1(data):
            return data * 2

        def mock_func2(data):
            return data + 1

        step1 = (mock_func1, {})
        step2 = (mock_func2, {})
        steps = [step1, step2]
        pipeline = prepare_data_prep_pipeline(steps)

        result = pipeline.fit_transform(3)
        self.assertEqual(result, 7)


class TestDataPrepWrapperFunc(unittest.TestCase):
    def test_wrapper_function(self):
        """Tests the data preparation wrapper function."""

        def mock_func(data, param1, param2):
            return data + "_transformed", param1, param2

        kw_args = {"param1": 10, "param2": 20}

        pick_nth_result = 0
        wrapper = data_prep_wrapper_func(mock_func, kw_args, pick_nth_result)
        result = wrapper("input_data")
        self.assertEqual(result, "input_data_transformed")

        pick_nth_result = 2
        wrapper1 = data_prep_wrapper_func(mock_func, kw_args, pick_nth_result)
        result1 = wrapper1("input_data")
        self.assertEqual(result1, 20)
