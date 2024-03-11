import yaml


def create_site_config_file(creds, siteconfig_path):
    json_data = {
        "connections": {"test": {"target": "test", "outputs": {"test": creds}}},
        "py_models": {"credentials_presets": None},
    }
    yaml_data = yaml.dump(json_data, default_flow_style=False)
    with open(siteconfig_path, "w") as file:
        file.write(yaml_data)
