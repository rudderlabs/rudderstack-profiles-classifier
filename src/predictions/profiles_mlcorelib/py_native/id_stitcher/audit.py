from typing import Dict
import uuid
from datetime import datetime
import traceback
from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial, WhtModel
from profiles_rudderstack.logger import Logger

from profiles_mlcorelib.utils.tracking import Analytics

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
        build_spec["materialization"] = {
            "output_type": "shell",
            "run_type": "interactive",
        }
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
        self.run_id = str(uuid.uuid4())

    def describe(self, this: WhtMaterial):
        return "", ""

    def register_dependencies(self, this: WhtMaterial):
        if this.wht_ctx.is_null_ctx:
            return

        ids_model_count = len(this.base_wht_project.models(model_types=["id_stitcher"]))
        while True:
            id_stitcher_model = self.select_id_stitcher_model(this)
            edge_sources = id_stitcher_model.build_spec()["edge_sources"]
            for edge_source in edge_sources:
                input_model_ref = edge_source["from"]
                this.de_ref(input_model_ref)

            self._run(id_stitcher_model, this)
            if ids_model_count == 1:
                break

            user_input = self.reader.get_input(
                "\n\nDo you want to run the audit on a different id_stitcher model? (yes/no)"
            )
            if user_input.lower() == "yes":
                continue
            elif user_input.lower() == "no":
                break
            else:
                print("\nInvalid input. Exiting.")
                break
        print("\n\nID Stitcher Audit Completed.\n")

    def select_id_stitcher_model(self, this: WhtMaterial):
        """
        Prompt the user to select an id_stitcher model from the list of available models
        """
        id_stitcher_models: Dict[str, WhtModel] = {}
        # FIXME: Remove "identity" from the list once the ListModels bug is fixed in wht code
        models = this.base_wht_project.models(model_types=["identity", "id_stitcher"])
        for model in models:
            # FIXME: Remove "id_collator" check once "identity" model type filter is removed
            if model.model_type() == "id_collator":
                continue
            id_stitcher_models[model.name()] = model
        if len(id_stitcher_models) == 0:
            raise ValueError("No id_stitcher model found in the project")
        if len(id_stitcher_models) > 1:
            max_retries = 5
            retry_count = 0
            while retry_count < max_retries:
                selected_model_name = self.reader.get_input(
                    f"Multiple id_stitcher models found. Please select one of {list(id_stitcher_models.keys())}"
                ).strip()
                if selected_model_name in id_stitcher_models:
                    break
                retry_count += 1
                if retry_count < max_retries:
                    self.logger.warn(
                        f"Invalid selection: '{selected_model_name}'. Please enter one of the following: {list(id_stitcher_models.keys())}"
                    )
                else:
                    raise ValueError(
                        f"Max retries exceeded. No valid id-stitcher model selected."
                    )
        else:
            selected_model_name = list(id_stitcher_models.keys())[0]

        return id_stitcher_models[selected_model_name]  # type: ignore

    def _run(self, ids_model: WhtModel, this: WhtMaterial):
        analytics = Analytics()
        analytics.show_consent_message(self.logger)
        start_time = datetime.now()
        analytics.track(
            "model_run_start",
            {"run_id": self.run_id, "model_type": "audit_id_stitcher"},
        )
        run_completed = False
        try:
            edge_sources = ids_model.build_spec()["edge_sources"]
            for edge_source in edge_sources:
                input_model_ref = edge_source["from"]
                input_material = this.de_ref(input_model_ref)
                if input_material is None:
                    raise ValueError(f"Model {input_model_ref} not found")
                edge_source["input_model"] = input_material.model
            entity = ids_model.entity()
            assert entity is not None, f"Entity not found in model {ids_model.name()}"

            yaml_report = YamlReport(edge_sources, entity)
            table_report = TableReport(
                this, ids_model, entity, yaml_report, self.logger
            )
            cluster_report = ClusterReport(
                self.reader, this, entity, table_report, self.logger
            )
            llm_report = LLMReport(
                self.reader,
                this,
                self.build_spec["access_token"],
                self.logger,
                table_report,
                entity,
            )
            reports = [yaml_report, table_report, cluster_report, llm_report]
            for report in reports:
                report.run()
            run_completed = True
            n_visualisations = cluster_report.counter
        except Exception as e:
            self.logger.debug(traceback.format_exc())
            self.logger.warn(f"An error occurred while running the audit: {e}")
            n_visualisations = None

        duration = (datetime.now() - start_time).total_seconds()
        analytics.track(
            "model_run_end",
            {
                "run_id": self.run_id,
                "model_type": "audit_id_stitcher",
                "duration_in_sec": duration,
                "is_run_completed": run_completed,
                "n_visualisations": n_visualisations,
            },
        )

    def execute(self, this: WhtMaterial):
        pass
