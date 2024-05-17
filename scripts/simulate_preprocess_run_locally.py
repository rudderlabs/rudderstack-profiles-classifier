import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.chdir("../")
import json

from src.predictions.rudderstack_predictions.ml_core.preprocess_and_train import (
    preprocess_and_train,
    train_and_store_model_results,
)
from src.predictions.rudderstack_predictions.utils import utils, constants
from src.predictions.rudderstack_predictions.trainers.TrainerFactory import (
    TrainerFactory,
)
from src.predictions.rudderstack_predictions.connectors.ConnectorFactory import (
    ConnectorFactory,
)
from dotenv import load_dotenv  # pip3 install python-dotenv

load_dotenv()

# print current working directory
print(os.getcwd())
# The next four vars should be modified based on the run.
profiles_yaml_path = "samples/predictions_dev_project/models/profiles-ml.yaml"
connection_name = os.getenv("SITE_CONN_NAME", None)
# You can get these two vars from `ps -ef | grep preprocess_and_train` command when the parent process (simulate_pb_run or pb run) is running and the code enters training part.
# Or they can be hardcoded based on the run.
material_names = '[["Material_user_var_table_7f41ca02_55", "2024-04-04", "Material_user_var_table_7f41ca02_63", "2024-04-11"], ["Material_user_var_table_7f41ca02_62", "2024-04-05", "Material_user_var_table_7f41ca02_60", "2024-04-12"], ["Material_user_var_table_7f41ca02_62", "2024-04-05", "Material_user_var_table_7f41ca02_61", "2024-04-12"]]'
input_column_types = '{"numeric": {"total_revenue": "INTEGER", "days_since_last_seen": "INTEGER", "days_since_payment": "INTEGER", "win_ratio": "NUMERIC"}, "categorical": {}, "arraytype": {},"booleantype": {}, "timestamp": {"last_used_day": "DATE", "last_payment_day": "DATE"}}'

# output folder path. Can leave as is or modify.
output_dir = "output/dev/seq_no/3"

home_dir = os.path.expanduser("~")
siteconfig_path = os.path.join(home_dir, ".pb/siteconfig.yaml")
siteconfig = utils.load_yaml(siteconfig_path)
python_path = siteconfig["py_models"]["python_path"]
default_config = utils.load_yaml(
    "src/predictions/rudderstack_predictions/config/model_configs.yaml"
)
input_model = utils.load_yaml(profiles_yaml_path)
assert (
    len(input_model["models"]) == 1
), "we are assuming a single model. If there are more models in the input file, change the index in the next line and modify this assertion"
input_config = input_model["models"][0]["model_spec"]["train"]["config"]
merged_config = utils.combine_config(default_config, input_config)
wh_creds_ = siteconfig["connections"][connection_name]
wh_creds = wh_creds_["outputs"][wh_creds_["target"]]
mode = "local"
metrics_table = "TRAINING_METRICS_v4"
params = [
    python_path,
    "-u",
    "-m",
    "src.predictions.rudderstack_predictions.ml_core.preprocess_and_train",
    "--material_names",
    material_names,
    "--merged_config",
    json.dumps(merged_config),
    "--input_column_types",
    input_column_types,
    "--wh_creds",
    json.dumps(wh_creds),
    "--output_path",
    output_dir,
    "--mode",
    mode,
    "--metrics_table",
    metrics_table,
]

subprocess_mode = False
if subprocess_mode:
    print("Running subprocess")
    import subprocess

    response = subprocess.run(
        params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    print(response.returncode)
    print("Done")
    print(response.stdout)
else:
    print("Calling preprocess_and_train directly")

    input_column_types = json.loads(input_column_types)
    trainer = TrainerFactory.create(merged_config)

    warehouse = wh_creds["type"]
    train_procedure = train_and_store_model_results
    connector = ConnectorFactory.create(warehouse, output_dir)
    session = connector.build_session(wh_creds)
    local_folder = connector.get_local_dir()
    material_info_ = json.loads(material_names)

    # converting material info back to named tuple after serialisation and deserialisation
    material_info = []
    if isinstance(material_info_[0], list):
        for material in material_info_:
            material_info.append(constants.TrainTablesInfo(*material))
    train_results_json = preprocess_and_train(
        train_procedure,
        material_info,
        merged_config,
        input_column_types,
        metrics_table,
        session=session,
        connector=connector,
        trainer=trainer,
    )
    print(f"Done training. Following is the output:\n\n\t{train_results_json}")
