from typing import Optional
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial, WhtModel
from profiles_rudderstack.logger import Logger

from .llm_report import LLMReport
from .yaml_report import YamlReport
from .table_report import TableReport
from .cluster_report import ClusterReport


class AuditIdStitcherModel(BaseModelType):
    TypeName = "audit_id_stitcher"
    BuildSpecSchema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "access_token": {"type": "string"},
        },
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        build_spec["materialization"] = {"output_type": "ephemeral"}
        super().__init__(build_spec, schema_version, pb_version)
        self.recipe = ModelRecipe(self.build_spec)

    def get_material_recipe(self) -> PyNativeRecipe:
        return self.recipe

    def validate(self):
        return super().validate()


class ModelRecipe(PyNativeRecipe):
    def __init__(self, build_spec: dict) -> None:
        super().__init__()
        self.build_spec = build_spec
        self.logger = Logger("AuditIdStitcherModelRecipe")
        self.id_stitcher_model: Optional[WhtModel] = None
        # There is no guarantee that the register_dependencies will be called only once
        # So we need to keep track of whether the run has been completed
        self.run_completed = False

    def describe(self, this: WhtMaterial):
        pass

    def register_dependencies(self, this: WhtMaterial):
        self._set_id_stitcher_model(this)
        edge_sources = self.id_stitcher_model.build_spec()["edge_sources"]
        for edge_source in edge_sources:
            input_model_ref = edge_source["from"]
            this.de_ref(input_model_ref)
        self._run(this)

    def _set_id_stitcher_model(self, this: WhtMaterial):
        if self.id_stitcher_model is not None:
            return
        id_stitcher_models = {}
        models = this.base_wht_project.models(model_types=["id_stitcher"])
        for model in models:
            id_stitcher_models[model.name()] = model
        if len(id_stitcher_models) > 1:
            selected_model_name = self.reader.get_input(
                f"Multiple id_stitcher models found. Please select one {id_stitcher_models.keys()}"
            )
        else:
            selected_model_name = list(id_stitcher_models.keys())[0]
        self.id_stitcher_model = id_stitcher_models[selected_model_name]

    def _run(self, this: WhtMaterial):
        if self.run_completed:
            return

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
        llm_report = LLMReport(
            self.reader,
            self.build_spec["access_token"],
            this.base_wht_project.warehouse_credentials(),
            table_report,
            entity,
        )
        reports = [yaml_report, table_report, cluster_report, llm_report]
        for report in reports:
            report.run()
        self.run_completed = True

    def execute(self, this: WhtMaterial):
        pass
