from profiles_rudderstack.model import BaseModelType
from profiles_rudderstack.recipe import PyNativeRecipe
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger


class ClassifierPredictionModel(BaseModelType):
    TypeName = "classifier_prediction"
    BuildSpecSchema = {
        "type": "object",
    }

    def __init__(self, build_spec: dict, schema_version: int, pb_version: str) -> None:
        super().__init__(build_spec, schema_version, pb_version)

    def get_material_recipe(self) -> PyNativeRecipe:
        return ClassifierPredictionRecipe()

    def validate(self) -> tuple[bool, str]:
        return super().validate()


class ClassifierPredictionRecipe(PyNativeRecipe):
    def __init__(self) -> None:
        self.logger = Logger("ClassifierPredictionRecipe")

    def describe(self, this: WhtMaterial):
        return "TODO", ".txt"

    def prepare(self, this: WhtMaterial):
        # TODO
        pass

    def execute(self, this: WhtMaterial):
        # TODO
        pass
