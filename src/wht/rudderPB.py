from typing import List, Tuple
import src.utils.utils as utils
from src.utils.logger import logger
from datetime import datetime, timezone

PB_PATH = "pb"
MATERIAL_PREFIX = "material_"


class RudderPB:
    def _compile(self, arg: dict):
        project_folder = utils.get_project_folder(
            arg["project_folder"], arg["output_filename"]
        )
        pb_args = [
            PB_PATH,
            "compile",
            "-p",
            project_folder,
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
            datetime.strptime(arg["features_valid_time"], "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        project_folder_path = utils.get_project_folder(
            arg["project_folder"], arg["output_path"]
        )
        pb_args = [
            PB_PATH,
            "run",
            "-p",
            project_folder_path,
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

    def get_material_name(self, model_name: str, model_hash: str, seq_no: int) -> str:
        return f"{MATERIAL_PREFIX}{model_name}_{model_hash}_{seq_no:.0f}"

    def get_latest_material_hash(
        self,
        entity_key: str,
        var_table_suffix: List[str],
        output_filename: str,
        site_config_path: str = None,
        project_folder: str = None,
    ) -> Tuple[str, str]:
        args = {
            "project_folder": project_folder,
            "output_filename": output_filename,
            "site_config_path": site_config_path,
        }
        pb_compile_output = self._compile(args)
        features_profiles_model = None
        for var_table in var_table_suffix:
            if entity_key + var_table in pb_compile_output:
                features_profiles_model = entity_key + var_table
                break
        if features_profiles_model is None:
            raise Exception(
                f"Could not find any matching var table in the output of pb compile command"
            )
        material_file_prefix = (MATERIAL_PREFIX + features_profiles_model + "_").lower()

        try:
            model_hash = pb_compile_output[
                pb_compile_output.index(material_file_prefix)
                + len(material_file_prefix) :
            ].split("_")[0]
        except ValueError:
            raise Exception(
                f"Could not find material file prefix {material_file_prefix} in the output of pb compile command: {pb_compile_output}"
            )
        return model_hash, features_profiles_model
