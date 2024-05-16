import uuid
from functools import partial
from typing import List, Tuple, Dict, Any


from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline


def prepare_data_prep_pipeline(steps: List[Tuple[callable, Dict]]) -> Pipeline:
    """
    Builds the data preparation pipeline.
    """
    pipeline_steps = []

    for step in steps:
        func_name = step[0].__name__ + uuid.uuid4()

        transformation_step = FunctionTransformer(
            func=step[0],
            kw_args=step[1],
            validate=False,
        )
        pipeline_steps.append((func_name, transformation_step))

    return Pipeline(steps=pipeline_steps)


def data_prep_wrapper_func(
    func: callable, kw_args: Dict, pick_nth_result: int
) -> callable:
    """
    This function is a wrapper around the data preparation function to
    pick desired return value from the function with multiple return values.
    """

    func_with_args = partial(func, **kw_args)

    def data_prep_wrapper(data: Any) -> Any:
        result = func_with_args(data)
        return result[pick_nth_result]

    return data_prep_wrapper


def build_data_prep_pipeline(
    connector, **kwargs
):
    try:
        preprocessing_steps = []
        timestamp_columns = kwargs.get("timestamp_columns", [])
        end_ts = kwargs.get("end_ts", None)

        if len(timestamp_columns) != 0 and end_ts is not None:
            preprocessing_steps += [
                (
                    connector.add_days_diff,
                    {"new_col": col, "time_col": col, "end_ts": end_ts},
                )
                for col in timestamp_columns
            ]

        array_type_columns = kwargs.get("array_type_columns", [])
        if len(array_type_columns) != 0:
            array_type_transformer = data_prep_wrapper_func(
                connector.transform_arraytype_features,
                kw_args={"arraytype_features": array_type_columns},
                pick_nth_result=1,
            )

            preprocessing_steps.append(
                (array_type_transformer, {})
            )

        ignore_features = kwargs.get("ignore_features", [])
        if len(ignore_features):
            preprocessing_steps.append(
                (connector.drop_cols, {"col_list": ignore_features})
            )

        return prepare_data_prep_pipeline(preprocessing_steps)

    except Exception as e:
        print(
            "Exception occurred while preparing preprocessing pipeline, Please check the logs for more details"
        )
        raise e
