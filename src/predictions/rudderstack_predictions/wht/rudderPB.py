import json
from typing import List, Tuple
from ..utils import utils
from ..utils.constants import MATERIAL_DATE_FORMAT
from ..utils.logger import logger
from datetime import datetime, timezone

PB_PATH = "pb"
MATERIAL_PREFIX = "Material_"


class RudderPB:
    def _compile(self, arg: dict):
        pb_args = [
            PB_PATH,
            "compile",
            "-p",
            arg["project_folder"],
            "--migrate_on_load=True",
            "-c",
            arg["site_config_path"],
        ]
        logger.info(
            f"Fetching latest model hash by running command: {' '.join(pb_args)}"
        )
        pb_compile_output_response = utils.subprocess_run(pb_args)
        pb_compile_output = (pb_compile_output_response.stdout).lower()
        logger.info(f"pb compile output: {pb_compile_output}")
        return pb_compile_output

    def run(self, arg: dict):
        """
        Args:
            features_valid_time (str): The date for which the past data needs to be materialized.
            feature_package_path (str): The path to the feature package.
            site_config_path (str): path to the siteconfig.yaml file
            project_folder (str): project folder path to pb_project.yaml file
        """
        features_valid_time_unix = int(
            datetime.strptime(arg["features_valid_time"], MATERIAL_DATE_FORMAT)
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        pb_args = [
            PB_PATH,
            "run",
            "-p",
            arg["project_folder"],
            "-m",
            arg["feature_package_path"],
            "--migrate_on_load=True",
            "--end_time",
            str(features_valid_time_unix),
            "-c",
            arg["site_config_path"],
        ]
        logger.info(
            f"Materialising historic data for {arg['features_valid_time']} using pb: {' '.join(pb_args)} "
        )
        try:
            utils.subprocess_run(pb_args)
        except Exception as e:
            raise Exception(
                f"Error occurred while materialising data for date {arg['features_valid_time']} : {e}"
            )

    def show_models(self, arg: dict) -> List[str]:
        pb_args = [
            PB_PATH,
            "show",
            "models",
            "--json",
            "--migrate_on_load=True",
            "-p",
            arg["project_folder"],
            "-c",
            arg["site_config_path"],
        ]
        logger.info(f"Fetching all models by running command: {' '.join(pb_args)}")

        try:
            pb_show_models_response = utils.subprocess_run(pb_args)
            pb_show_models_response_output = (pb_show_models_response.stdout).lower()
        except Exception as e:
            raise Exception(f"Error occurred while fetching all models : {e}")

        return self._extract_json_from_stdout(pb_show_models_response_output)

    def _extract_json_from_stdout(self, stdout):
        lines = stdout.splitlines()

        start_index = next(
            (i for i, line in enumerate(lines) if line.strip().startswith("{")), None
        )
        if start_index is None:
            return None

        # Parse JSON
        json_string = "".join(lines[start_index:])
        json_data = json.loads(json_string)

        return json_data

    def get_latest_material_hash(
        self,
        entity_key: str,
        site_config_path: str = None,
        project_folder: str = None,
    ) -> Tuple[str, str]:
        args = {
            "project_folder": project_folder,
            "site_config_path": site_config_path,
        }
        pb_compile_output = self._compile(args)
        model_name = None
        for var_table in ["_var_table", "_all_var_table"]:
            if entity_key + var_table in pb_compile_output:
                model_name = entity_key + var_table
                break
        if model_name is None:
            raise Exception(
                f"Could not find any matching var table in the output of pb compile command"
            )
        material_file_prefix = (MATERIAL_PREFIX + model_name + "_").lower()

        try:
            model_hash = pb_compile_output[
                pb_compile_output.index(material_file_prefix)
                + len(material_file_prefix) :
            ].split("_")[0]
        except ValueError:
            raise Exception(
                f"Could not find material file prefix {material_file_prefix} in the output of pb compile command: {pb_compile_output}"
            )
        return model_hash, model_name

    def get_latest_entity_var_table_name(
        model_hash: str, entity_var_model: str, inputs: list
    ) -> str:
        try:
            input = inputs[0]
            seq_no = int(input.split("_")[-1])
            return MATERIAL_PREFIX + entity_var_model + "_" + f"{seq_no:.0f}"
        except IndexError:
            raise Exception(
                "Error while getting feature table name using model "
                f"hash {model_hash}, feature profile model {entity_var_model} and input {inputs}"
            )
