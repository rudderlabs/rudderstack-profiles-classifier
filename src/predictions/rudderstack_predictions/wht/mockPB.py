from .rudderPB import RudderPB


class MockPB:
    def get_latest_material_hash(self, *args):
        # SELECT * FROM MATERIAL_REGISTRY_4 WHERE model_type='entity_var_model'
        # AND end_ts BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '7 days';
        return "54ddc22a", "user_var_table"

    def run(self, *args):
        return RudderPB().run(*args)

    def get_material_name(self, *args):
        return RudderPB().get_material_name(*args)

    def split_material_table(self, *args):
        return RudderPB().split_material_table(*args)

    def get_material_registry_name(self, *args):
        return RudderPB().get_material_registry_name(*args)
