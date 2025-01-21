class MockModel:
    def __init__(
        self,
        name: str,
        model_type: str,
        db_object_name: str,
        output_type: str,
        model_ref: str,
        encapsulating_db_object_name: str,
    ) -> None:
        self._name = name
        self._model_type = model_type
        self.db_object_name = db_object_name
        self.output_type = output_type
        self._model_ref = model_ref
        self.encapsulating_db_object_name = encapsulating_db_object_name

    def model_type(self):
        return self._model_type

    def model_ref(self):
        return self._model_ref

    def model_ref_from_level_root(self):
        return self._model_ref

    def db_object_name_prefix(self):
        return self.db_object_name

    def materialization(self):
        return {"output_type": self.output_type}

    def entity(self):
        return {"IdColumnName": "user_main_id"}

    def name(self):
        return self._name

    def time_filtering_column(self):
        return "timestamp_mock"

    def encapsulating_model(self):
        return MockModel(
            None,
            "entity_var_table",
            self.encapsulating_db_object_name,
            "table",
            "entity/user/var_table",
            None,
        )


class MockMaterial:
    def __init__(
        self,
        model_type: str,
        db_object_name: str,
        output_type: str,
        model_ref: str,
        encapsulating_db_object_name: str,
    ) -> None:
        self.model = MockModel(
            db_object_name,
            model_type,
            db_object_name,
            output_type,
            model_ref,
            encapsulating_db_object_name,
        )
        self.db_object_name = db_object_name
        self.output_type = output_type
        self.encapsulating_db_object_name = encapsulating_db_object_name

    def name(self):
        return f"Material_{self.db_object_name}_hash_100"

    def get_selector_sql(self):
        if self.output_type == "column":
            return (
                f"SELECT {self.db_object_name} FROM {self.encapsulating_db_object_name}"
            )
        return f"SELECT * FROM {self.db_object_name}"


class MockProject:
    def entities(self):
        return {
            "user": {"IdColumnName": "user_abc_id"},
            "campaign": {"IdColumnName": "campaign_def_id"},
        }


class MockCtx:
    def time_info(self):
        return ["2021-01-01", "2021-01-31"]

    def client(self):
        return None
