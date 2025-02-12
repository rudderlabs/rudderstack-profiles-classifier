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


class TestParseIdStitcherMetadata(unittest.TestCase):
    def test_valid_string_input_without_seqno(self):
        # Test with valid JSON string that has no incremental seqno (null)
        row = '{"material_objects": [{"material_name": "edges_table"}], "dependency_context_metadata": {"incremental_base": {"seqno": null}}}'
        result = TableReport.parse_id_stitcher_metadata(row)
        self.assertEqual(result[0]["material_name"], "edges_table")

    def test_valid_dict_input_without_seqno(self):
        # Test with dict input without incremental seqno
        input_dict = {
            "material_objects": [{"material_name": "edges_table"}],
            "dependency_context_metadata": {"incremental_base": {"seqno": None}},
        }
        result = TableReport.parse_id_stitcher_metadata(input_dict)
        self.assertEqual(result[0]["material_name"], "edges_table")

    def test_input_with_seqno_raises_exception(self):
        # When a non-null seqno is provided, the method should raise.
        row_str = '{"material_objects": [{"material_name": "edges_table"}], "dependency_context_metadata": {"incremental_base": {"seqno": 100}}}'
        with self.assertRaises(Exception) as context:
            TableReport.parse_id_stitcher_metadata(row_str)
        self.assertIn(
            "Audit is not supported on incremental runs", str(context.exception)
        )

        input_dict = {
            "material_objects": [{"material_name": "edges_table"}],
            "dependency_context_metadata": {"incremental_base": {"seqno": 200}},
        }
        with self.assertRaises(Exception) as context:
            TableReport.parse_id_stitcher_metadata(input_dict)
        self.assertIn(
            "Audit is not supported on incremental runs", str(context.exception)
        )

    def test_invalid_json_string(self):
        # Test that an invalid JSON string raises a json.JSONDecodeError.
        with self.assertRaises(json.JSONDecodeError):
            TableReport.parse_id_stitcher_metadata("{invalid_json}")

    def test_none_input_raises_exception(self):
        # Passing None should raise an exception.
        with self.assertRaises(Exception):
            TableReport.parse_id_stitcher_metadata(None)


if __name__ == "__main__":
    unittest.main()
