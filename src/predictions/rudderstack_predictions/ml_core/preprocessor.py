from typing import List, Tuple, Dict, Any
from sklearn.preprocessing import FunctionTransformer


class PipelineData:
    connector: Any
    data: Any
    step_args: Dict = {}
    step_results: Dict = {}


def connector_transformation_step(
    pipeline_data: PipelineData, func_name: str, pick_nth_result: int = None
) -> PipelineData:
    """
    This function is a transformation step (which is basically a function from the connector object)
    that can be used to apply any transformation

    Args:
        data (PipelineData): Data to be transformed
        func_name (str): Name of the function to be applied from the input connector object
        pick_nth_result (int): Index of the result to be picked from the transformation function to be given to the next step
    """
    connector = pipeline_data.connector
    data = pipeline_data.data
    kw_args = pipeline_data.step_args.get(func_name, {})

    try:
        func = getattr(connector, func_name)
        transform_result = func(data, **kw_args)
        other_results = []

        if isinstance(transform_result, tuple):
            transformed_data = transform_result[pick_nth_result]
            for i in range(len(transform_result)):
                if i != pick_nth_result:
                    other_results.append(transform_result[i])
                else:
                    other_results.append(None)  # To maintain the order of the results
        else:
            transformed_data = transform_result

        pipeline_data.step_results[func_name] = other_results
        pipeline_data.data = transformed_data

        return pipeline_data

    except Exception as e:
        print(
            f"Exception occurred while applying transformation step {func_name}, Please check the logs for more details"
        )
        raise e


class Preprocessor:

    def __init__(self, connector):
        self.connector = connector
        self.pipeline_steps = {}

        self.add_step("add_days_diff")
        self.add_step("transform_arraytype_features", pick_nth_result=1)
        self.add_step("transform_booleantype_features")
        self.add_step("drop_cols")

    def add_step(self, func_name: str, pick_nth_result: int = None):
        self.pipeline_steps[func_name] = FunctionTransformer(
            func=connector_transformation_step,
            kw_args={"func_name": func_name, "pick_nth_result": pick_nth_result},
            validate=False,
        )

    def transform(self, data: Any, transformations_info: List[Tuple]) -> PipelineData:
        pipeline_data = PipelineData()
        pipeline_data.connector = self.connector
        pipeline_data.data = data

        for transformation_name, options in transformations_info:
            pipeline_data.step_args[transformation_name] = options
            pipeline_data = self.pipeline_steps[transformation_name].transform(
                pipeline_data
            )

        return pipeline_data.data, pipeline_data.step_results
