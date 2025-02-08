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


if __name__ == "__main__":
    unittest.main()
