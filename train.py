from src.predictions.rudderstack_predictions.train import _train


def train(
    creds: dict,
    inputs: str,
    output_filename: str,
    config: dict,
    site_config_path: str = None,
    project_folder: str = None,
    runtime_info: dict = None,
) -> None:
    _train(
        creds,
        inputs,
        output_filename,
        config,
        site_config_path,
        project_folder,
        runtime_info,
    )
