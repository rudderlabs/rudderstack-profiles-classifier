import unittest
from unittest.mock import patch
import pandas as pd
from src.predictions.profiles_mlcorelib.py_native.id_stitcher.cluster_report import (
    ClusterReport,
)
from .mocks import MockModel, MockProject, MockCtx, MockMaterial


class MockWhtMaterial:
    def __init__(self) -> None:
        self.model = MockModel(None, None, None, None, None, None)
        self.base_wht_project = MockProject()
        self.wht_ctx = MockCtx()
        self.de_refs = set()

    def de_ref(self, input):
        self.de_refs.add(input)
        return MockMaterial(
            None,
            None,
            None,
            None,
            None,
        )

    def execute_text_template(self, template):
        return template


class TestAttributionModel(unittest.TestCase):
    @patch("profiles_rudderstack.logger.Logger")
    def setUp(self, mockLogger) -> None:
        self.build_spec = {
            "access_token": "dummy_access_token",
            "materialization": {"output_type": "ephemeral"},
        }
        self.material = MockWhtMaterial()
        self.entity = "user"
        self.edges_df = pd.DataFrame(
            {
                "id1": {
                    0: "0015x00002Ex4fHAAR",
                    1: "samsung.com",
                    2: "0015x00002FzxwLAAR",
                    3: "Samsung Electronics America",
                    4: "Samsung South Africa",
                    5: "Samsung South Africa",
                    6: "0015x00002Ex4fHAAR",
                    7: "0015x00002FzxwLAAR",
                },
                "id2": {
                    0: "Samsung Electronics America",
                    1: "samsung.com",
                    2: "0015x00002FzxwLAAR",
                    3: "Samsung Electronics America",
                    4: "samsung.com",
                    5: "Samsung South Africa",
                    6: "0015x00002Ex4fHAAR",
                    7: "Samsung South Africa",
                },
                "id1_type": {
                    0: "sf_account_id",
                    1: "domain",
                    2: "sf_account_id",
                    3: "sf_account_name",
                    4: "sf_account_name",
                    5: "sf_account_name",
                    6: "sf_account_id",
                    7: "sf_account_id",
                },
                "id2_type": {
                    0: "sf_account_name",
                    1: "domain",
                    2: "sf_account_id",
                    3: "sf_account_name",
                    4: "domain",
                    5: "sf_account_name",
                    6: "sf_account_id",
                    7: "sf_account_name",
                },
            }
        )
        self.cluster_report_obj = ClusterReport(
            None, self.material, self.entity, None, mockLogger
        )
        self.graph = self.cluster_report_obj.create_graph_with_metadata(self.edges_df)

    def test_valid_graph_metrics(self):
        metrics = self.cluster_report_obj.compute_graph_metrics(self.graph)
        expected_metrics = {
            "num_nodes": 5,
            "num_edges": 8,
            "avg_degree": 3.2,
            "top_degree_nodes": [
                ("Samsung South Africa", "sf_account_name"),
                ("0015x00002Ex4fHAAR", "sf_account_id"),
                ("Samsung Electronics America", "sf_account_name"),
                ("samsung.com", "domain"),
                ("0015x00002FzxwLAAR", "sf_account_id"),
            ],
            "edge_count": {
                ("0015x00002Ex4fHAAR", "sf_account_id"): 3,
                ("Samsung Electronics America", "sf_account_name"): 3,
                ("samsung.com", "domain"): 3,
                ("0015x00002FzxwLAAR", "sf_account_id"): 3,
                ("Samsung South Africa", "sf_account_name"): 4,
            },
            "top_bridge_nodes": [("Samsung South Africa", "sf_account_name")],
            "betweenness_centrality": {
                ("0015x00002Ex4fHAAR", "sf_account_id"): 0.0,
                ("Samsung Electronics America", "sf_account_name"): 0.0,
                ("samsung.com", "domain"): 0.0,
                ("0015x00002FzxwLAAR", "sf_account_id"): 0.0,
                ("Samsung South Africa", "sf_account_name"): 0.16666666666666666,
            },
            "diameter": 2,
        }

        self.assertEqual(
            metrics,
            expected_metrics,
        )
