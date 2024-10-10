from typing import Optional
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial, WhtModel
from profiles_rudderstack.logger import Logger
from .yaml_report import YamlReport
from .table_report import TableReport
from .cluster_report import ClusterReport


class IdStitcherDebuggerModel(BaseModelType):
    TypeName = "id_stitcher_debugger"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "access_token": {"type": "string"},
        },
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return ModelRecipe(self.build_spec)

    def validate(self):
        return super().validate()


class ModelRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        super().__init__()
        self.build_spec = build_spec
        self.logger = Logger("IdStitcherDebuggerModelRecipe")
        self.id_stitcher_model: Optional[WhtModel] = None

    def describe(self, this: WhtMaterial):
        return (
            f"""
        Material - {this.name()}
        """,
            ".txt",
        )

    def register_dependencies(self, this: WhtMaterial):
        pass

    def _set_id_stitcher_model(self, this: WhtMaterial):
        if self.id_stitcher_model is not None:
            return
        id_stitcher_models = {}
        # With the current implementation of "models" method, it shouldn't be called in "register_dependencies" method
        # because it will try to compile itself and that will cause a deadlock
        models = this.base_wht_project.models()
        for model in models:
            if model.model_type() == "id_stitcher":
                id_stitcher_models[model.name()] = model
        if len(id_stitcher_models) > 1:
            selected_model_name = self.reader.get_input(
                f"Multiple id_stitcher models found. Please select one {id_stitcher_models.keys()}"
            )
        else:
            selected_model_name = list(id_stitcher_models.keys())[0]
        self.id_stitcher_model = id_stitcher_models[selected_model_name]

    def execute(self, this: WhtMaterial):
        self._set_id_stitcher_model(this)
        edge_sources = self.id_stitcher_model.build_spec()["edge_sources"]
        for edge_source in edge_sources:
            input_model_ref = edge_source["from"]
            input_material = this.de_ref(input_model_ref)
            if input_material is None:
                raise ValueError(f"Model {input_model_ref} not found")
            edge_source["input_model"] = input_material.model
        entity = self.id_stitcher_model.entity()
        yaml_report = YamlReport(edge_sources, entity)
        table_report = TableReport(
            this.wht_ctx.client, self.id_stitcher_model, entity, yaml_report
        )
        cluster_report = ClusterReport(
            self.reader, this.wht_ctx.client, entity, table_report
        )
        reports = [yaml_report, table_report, cluster_report]
        for report in reports:
            report.run()
        # FIXME:
        # 1. material warehouse object does not exist after material run
        # 2. Stop generating the .txt file
