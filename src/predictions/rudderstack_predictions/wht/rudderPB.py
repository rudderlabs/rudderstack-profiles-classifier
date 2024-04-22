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

        return pb_show_models_response_output

    def extract_json_from_stdout(self, stdout):
        start_index = stdout.find("printing models")
        if start_index == -1:
            return None

        # Find the index of the first '{' after the line
        start_index = stdout.find("{", start_index)

        if start_index == -1:
            return None

        # Find the index of the last '}'
        end_index = stdout.rfind("}")

        # Extract the JSON string between the first '{' and the last '}'
        json_string = stdout[start_index : end_index + 1]

        # Replace single quotes with double quotes
        json_string = json_string.replace("'", '"')
        logger.debug(f"parsing {json_string} as json")

        # Parse JSON
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
        entity_var_model_name = None
        for var_table in ["_var_table", "_all_var_table"]:
            if entity_key + var_table in pb_compile_output:
                entity_var_model_name = entity_key + var_table
                break
        if entity_var_model_name is None:
            raise Exception(
                f"Could not find any matching var table in the output of pb compile command"
            )
        material_file_prefix = (MATERIAL_PREFIX + entity_var_model_name + "_").lower()

        try:
            model_hash = pb_compile_output[
                pb_compile_output.index(material_file_prefix)
                + len(material_file_prefix) :
            ].split("_")[0]
        except ValueError:
            raise Exception(
                f"Could not find material file prefix {material_file_prefix} in the output of pb compile command: {pb_compile_output}"
            )
        return model_hash, entity_var_model_name

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
