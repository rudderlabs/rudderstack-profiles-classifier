import src.utils.utils as utils
from src.utils.logger import logger


class RudderPB:
    def compile(self, arg: dict):
        project_folder = utils.get_project_folder(
            arg["project_folder"], arg["output_filename"]
        )
        pb = utils.get_pb_path()
        pb_args = [
            pb,
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
