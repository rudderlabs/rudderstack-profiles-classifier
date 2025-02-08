import unittest
from unittest.mock import Mock, patch
import pandas as pd
import json
from src.predictions.profiles_mlcorelib.py_native.id_stitcher.table_report import (
    TableReport,
)
from ..mocks import MockModel, MockProject, MockCtx, MockMaterial


class MockWhtMaterial:
    def __init__(self) -> None:
        self.model = MockModel(None, None, None, None, None, None)
        self.wht_ctx = MockCtx()

    def de_ref(self, input):
        return MockMaterial(None, None, None, None, None)


class DummyLogger:
    def __init__(self):
        self.messages = []

    def warn(self, msg):
        self.messages.append(msg)

    def info(self, msg):
        self.messages.append(msg)


class DummyClient:
    def __init__(self):
        self.db = "dummy_db"
        self.schema = "dummy_schema"
        self.wh_type = "snowflake"


class DummyWhtCtx:
    def __init__(self):
        self.client = DummyClient()


class DummyMaterial:
    def __init__(self):
        self.wht_ctx = DummyWhtCtx()


class DummyModel:
    def hash(self):
        return "dummy_hash"

    def model_ref(self):
        return "dummy_model_ref"


class DummyYamlReport:
    def edge_source_pairs(self):
        return {}


class TestTransformRow(unittest.TestCase):
    def test_transform_row_with_string_input(self):
        row = '{"material_objects":[{"material_name":"test_edges"}],"dependency_context_metadata":{"incremental_base":{"seqno":123}}}'
        material_objects, seqno = TableReport.transform_row(row)
        self.assertEqual(material_objects[0]["material_name"], "test_edges")
        self.assertEqual(seqno, 123)

    def test_transform_row_with_dict_input(self):
        input_dict = {
            "material_objects": [
                {"material_name": "test_edges"},
                {"material_name": "test_output"},
            ],
            "dependency_context_metadata": {"incremental_base": {"seqno": 456}},
        }

        material_objects, seqno = TableReport.transform_row(input_dict)

        self.assertEqual(len(material_objects), 2)
        self.assertEqual(material_objects[0]["material_name"], "test_edges")
        self.assertEqual(material_objects[1]["material_name"], "test_output")
        self.assertEqual(seqno, 456)

    def test_transform_row_without_seqno(self):
        input_dict = {"material_objects": [{"material_name": "test_edges"}]}

        material_objects, seqno = TableReport.transform_row(input_dict)

        self.assertEqual(len(material_objects), 1)
        self.assertEqual(material_objects[0]["material_name"], "test_edges")
        self.assertIsNone(seqno)

    def test_transform_row_with_empty_material_objects(self):
        input_dict = {
            "material_objects": [],
            "dependency_context_metadata": {"incremental_base": {"seqno": 789}},
        }

        material_objects, seqno = TableReport.transform_row(input_dict)

        self.assertEqual(len(material_objects), 0)
        self.assertEqual(seqno, 789)

    def test_transform_row_invalid_json(self):
        with self.assertRaises(Exception) as context:
            TableReport.transform_row("{invalid json}")

        self.assertIn("Unable to fetch material objects", str(context.exception))

    def test_transform_row_missing_material_objects(self):
        input_dict = {"some_other_key": "value"}

        material_objects, seqno = TableReport.transform_row(input_dict)

        self.assertEqual(len(material_objects), 0)
        self.assertIsNone(seqno)

    def test_transform_row_none_input(self):
        with self.assertRaises(Exception) as context:
            TableReport.transform_row(None)

        self.assertIn("Unable to fetch material objects", str(context.exception))

    def test_transform_row_complex_nested_structure(self):
        input_dict = {
            "material_objects": [
                {"material_name": "edges_table", "other_field": "value1"},
                {"material_name": "output_table", "other_field": "value2"},
            ],
            "dependency_context_metadata": {
                "incremental_base": {"seqno": 789, "other_info": "some_value"},
                "other_metadata": "value",
            },
            "additional_field": "value",
        }

        material_objects, seqno = TableReport.transform_row(input_dict)

        self.assertEqual(len(material_objects), 2)
        self.assertEqual(material_objects[0]["material_name"], "edges_table")
        self.assertEqual(material_objects[0]["other_field"], "value1")
        self.assertEqual(material_objects[1]["material_name"], "output_table")
        self.assertEqual(material_objects[1]["other_field"], "value2")
        self.assertEqual(seqno, 789)


class TestGetIncrementalEdgesTables(unittest.TestCase):
    def setUp(self):
        # Create dummy instances to mimic dependencies.
        self.dummy_this = DummyMaterial()
        self.dummy_model = DummyModel()
        self.dummy_entity = {"Name": "dummy_entity", "IdColumnName": "id"}
        self.dummy_yaml = DummyYamlReport()
        self.logger = DummyLogger()

        # Create an instance of TableReport.
        self.tr = TableReport(
            self.dummy_this,
            self.dummy_model,
            self.dummy_entity,
            self.dummy_yaml,
            self.logger,
        )

    def test_no_incremental_base(self):
        # When there is no incremental base, simply return the current edges table.
        self.tr.incr_base_seqno = None
        self.tr.edges_table = "current_edges"
        # The registry_df doesn't matter in this case.
        self.tr.registry_df = pd.DataFrame(columns=["SEQ_NO", "METADATA"])

        result = self.tr._get_incremental_edges_tables()
        self.assertEqual(result, ["current_edges"])

    def test_single_dependency_dict_metadata(self):
        # Single level dependency with metadata as a dict.
        self.tr.incr_base_seqno = 1
        self.tr.edges_table = "current_edges"
        metadata = {
            "dependency_context_metadata": {"incremental_base": {"seqno": None}},
            "material_objects": [{"material_name": "prev_edges"}],
        }
        df = pd.DataFrame([{"SEQ_NO": 1, "METADATA": metadata}])
        self.tr.registry_df = df

        result = self.tr._get_incremental_edges_tables()
        # Expected: parent's table comes first then the current edges table.
        self.assertEqual(result, ["prev_edges", "current_edges"])

    def test_single_dependency_string_metadata(self):
        # Single dependency with the metadata provided as a JSON string.
        self.tr.incr_base_seqno = 1
        self.tr.edges_table = "current_edges"
        metadata_dict = {
            "dependency_context_metadata": {"incremental_base": {"seqno": None}},
            "material_objects": [{"material_name": "prev_edges"}],
        }
        metadata_str = json.dumps(metadata_dict)
        df = pd.DataFrame([{"SEQ_NO": 1, "METADATA": metadata_str}])
        self.tr.registry_df = df

        result = self.tr._get_incremental_edges_tables()
        self.assertEqual(result, ["prev_edges", "current_edges"])

    def test_multiple_dependency_chain(self):
        # Multiple dependency chain: seqno=2 -> seqno=1 -> None.
        self.tr.incr_base_seqno = 2
        self.tr.edges_table = "current_edges"
        metadata_row_2 = {
            "dependency_context_metadata": {"incremental_base": {"seqno": 1}},
            "material_objects": [{"material_name": "middle_edges"}],
        }
        metadata_row_1 = {
            "dependency_context_metadata": {"incremental_base": {"seqno": None}},
            "material_objects": [{"material_name": "first_edges"}],
        }
        df = pd.DataFrame(
            [
                {"SEQ_NO": 2, "METADATA": metadata_row_2},
                {"SEQ_NO": 1, "METADATA": metadata_row_1},
            ]
        )
        self.tr.registry_df = df

        result = self.tr._get_incremental_edges_tables()
        # The expected chain after reversing is: first_edges, middle_edges, current_edges.
        self.assertEqual(result, ["first_edges", "middle_edges", "current_edges"])

    def test_loop_detection(self):
        # Test loop detection: a row refers back to itself.
        self.tr.incr_base_seqno = 1
        self.tr.edges_table = "current_edges"
        metadata = {
            "dependency_context_metadata": {"incremental_base": {"seqno": 1}},
            "material_objects": [{"material_name": "prev_edges"}],
        }
        df = pd.DataFrame([{"SEQ_NO": 1, "METADATA": metadata}])
        self.tr.registry_df = df

        result = self.tr._get_incremental_edges_tables()
        # Expect to break out of the loop with only the parent's table appended plus the current table.
        self.assertEqual(result, ["prev_edges", "current_edges"])
        loop_warning_found = any(
            "Detected a loop" in msg for msg in self.logger.messages
        )
        self.assertTrue(loop_warning_found)

    def test_missing_row(self):
        # When the registry_df does not contain the matching SEQ_NO.
        self.tr.incr_base_seqno = 10
        self.tr.edges_table = "current_edges"
        # Create a DataFrame without a row with SEQ_NO 10.
        self.tr.registry_df = pd.DataFrame(columns=["SEQ_NO", "METADATA"])

        result = self.tr._get_incremental_edges_tables()
        # Since no row was found, only the current table should be returned.
        self.assertEqual(result, ["current_edges"])
        missing_row_warning = any(
            "Could not find table with seqno" in msg for msg in self.logger.messages
        )
        self.assertTrue(missing_row_warning)

    def test_invalid_json_metadata(self):
        # When the metadata is an invalid JSON string.
        self.tr.incr_base_seqno = 1
        self.tr.edges_table = "current_edges"
        df = pd.DataFrame([{"SEQ_NO": 1, "METADATA": "invalid_json"}])
        self.tr.registry_df = df

        result = self.tr._get_incremental_edges_tables()
        # Expected: exception is caught; no parent's table is added.
        self.assertEqual(result, ["current_edges"])
        invalid_json_warning = any(
            "Error processing metadata for seq_no" in msg
            for msg in self.logger.messages
        )
        self.assertTrue(invalid_json_warning)

    def test_no_edges_in_material_objects(self):
        # When material_objects does not contain any table name that ends with 'edges'.
        self.tr.incr_base_seqno = 1
        self.tr.edges_table = "current_edges"
        metadata = {
            "dependency_context_metadata": {"incremental_base": {"seqno": None}},
            "material_objects": [{"material_name": "not_edge"}],
        }
        df = pd.DataFrame([{"SEQ_NO": 1, "METADATA": metadata}])
        self.tr.registry_df = df

        result = self.tr._get_incremental_edges_tables()
        # Since no parent's edges table is found, only the current table is returned.
        self.assertEqual(result, ["current_edges"])


if __name__ == "__main__":
    unittest.main()
