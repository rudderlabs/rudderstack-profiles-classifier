from .rudderPB import RudderPB


class MockPB:
    def get_latest_material_hash(self, *args):
        # SELECT * FROM MATERIAL_REGISTRY_4 WHERE model_type='entity_var_model'
        # AND end_ts BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '7 days';
        return "3aa7f556", "user_var_table"

    def run(self, *args):
        return RudderPB().run(*args)

    def show_models(self, arg: dict):
        return RudderPB().show_models(arg)

    def extract_json_from_stdout(self, stdout):
        return RudderPB().extract_json_from_stdout(stdout)
