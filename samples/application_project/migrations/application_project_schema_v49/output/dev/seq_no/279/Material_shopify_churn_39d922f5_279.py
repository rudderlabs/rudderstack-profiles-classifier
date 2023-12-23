import json
import sys
import pkg_resources

"""
Both the paths are relative to the root of the py_repo folder
They are added to the path so that the train and predict modules can be imported
"""
sys.path.append("../")
sys.path.append("./")

with open('requirements.txt') as f:
    required = f.read().splitlines()

not_installed = []
for package in required:
    try:
        pkg_resources.require(package)
    except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict):
        not_installed.append(package)

if not_installed:
    install_command = """
    Local Environment: pip install -r requirements.txt # in the repo folder
    Conda Environment: conda install --file requirements.txt
    Kubernetes: https://kubernetes.io/blog/2019/07/23/get-started-with-kubernetes-using-python/
    """
    error_message = "The following package(s) are not installed or their version is not correct: {}.\nPlease install them using any of the following commands: {}".format(", ".join(not_installed), install_command)
    raise ImportError(error_message)

from rudderstack_profiles_classifier import train as T, predict as P

train_inputs = [
    """SELECT * FROM ML_TEST1.Material_rudder_user_base_features_891650ed_279""",
]

predict_inputs = [
    """SELECT * FROM ML_TEST1.Material_rudder_user_base_features_891650ed_279""",
]

train_file_extension = ".json"

if __name__ == "__main__":
    creds = json.loads(sys.argv[1])
    credentials_presets = json.loads(sys.argv[2])
    p_output_tablename = sys.argv[3]
    t_output_filename = sys.argv[4] + train_file_extension
    should_train = sys.argv[5].lower() == "true"
    site_config_path = sys.argv[6]
    project_folder = sys.argv[7]
    train_config = json.loads('{"data":{"eligible_users":"1=1","features_profiles_model":"rudder_user_base_features","inputs":["packages/feature_table/models/rudder_user_base_features"],"label_column":"is_churned_7_days","label_value":1,"output_profiles_ml_model":"shopify_churn","package_name":"feature_table","prediction_horizon_days":7},"preprocessing":{"ignore_features":["user_email","first_name","last_name"]}}')
    predict_config = json.loads('{"data":{"eligible_users":"1=1","features_profiles_model":"rudder_user_base_features","inputs":["packages/feature_table/models/rudder_user_base_features"],"label_column":"is_churned_7_days","label_value":1,"output_profiles_ml_model":"shopify_churn","package_name":"feature_table","prediction_horizon_days":7},"outputs":{"column_names":{"percentile":"percentile_churn_score_7_days","score":"churn_score_7_days"},"feature_meta_data":{"features":[{"description":"Percentile of churn score. Higher the percentile, higher the probability of churn","name":"percentile_churn_score_7_days"}]}},"preprocessing":{"ignore_features":["user_email","first_name","last_name"]}}')

    if should_train:
        T.train(creds, train_inputs, t_output_filename, train_config, site_config_path, project_folder)

    if credentials_presets is None:
        credentials_presets = {}

    s3_config = credentials_presets.get("s3", {})
    P.predict(creds, s3_config, t_output_filename, predict_inputs, p_output_tablename, predict_config)
