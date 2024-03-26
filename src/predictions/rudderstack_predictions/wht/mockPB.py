from .rudderPB import RudderPB


class MockPB:
    def get_latest_material_hash(self, *args):
        # SELECT * FROM MATERIAL_REGISTRY_4 WHERE model_type='entity_var_model'
        # AND end_ts BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '7 days';
        return "8b944948", "user_var_table"

    def run(self, *args):
        return RudderPB().run(*args)

    def show_models(self, *args):
        return RudderPB().show_models(*args)
