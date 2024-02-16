class MockPB:
    def compile(self, arg: dict):
        # SELECT * FROM MATERIAL_REGISTRY_4 WHERE model_type='entity_var_model'
        # AND end_ts BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '7 days';
        return "material_user_var_table_54ddc22a_1903"
