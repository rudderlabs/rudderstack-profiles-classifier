from typing import Dict
import requests

from .config import LLM_SERVICE_URL
from .table_report import TableReport

# TODO: Uncomment the following line after adding the Reader class to the profiles_rudderstack package
# from profiles_rudderstack.reader import Reader

from enum import Enum


class ProgramState(Enum):
    STOP = 1


class LLMReport:
    def __init__(
        self,
        reader,
        access_token: str,
        warehouse_credentials: dict,
        table_report: TableReport,
        entity: Dict,
    ):
        self.access_token = access_token
        self.warehouse_credentials = warehouse_credentials
        self.table_report = table_report
        self.reader = reader
        self.entity = entity
        self.session_id = ""

    def run(self):
        print("You can now ask questions about the ID Stitcher analysis results.")
        if not self.access_token:
            state = self._prompt_for_access_token(
                "We couldn't find RudderStack access token in your siteconfig file. To access the LLM analysis, please enter an access token (if you don't have one, you can create it from the RudderStack dashboard)."
            )
            if state == ProgramState.STOP:
                return
        while True:
            user_input = self.reader.get_input(
                "Enter your question. (or 'quit' to skip this step): \n"
            )
            if user_input.lower() in ["quit", "exit", "done"]:
                break
            state = self._request(user_input)
            if state == ProgramState.STOP:
                break

    def _prompt_for_access_token(self, prompt: str) -> ProgramState:
        user_input = self.reader.get_input(
            prompt
            + " If you'd prefer to complete this step later, you can choose 'quit' to skip it for now."
        )
        if user_input.lower() in ["quit", "exit", "done"]:
            return ProgramState.STOP
        self.access_token = user_input
        print(
            "Please add this token to the 'rudderstack_access_token' key in your siteconfig file to avoid entering it each time."
        )

    def _get_report(self, report):
        unique_id_counts = []
        for key, value in report["unique_id_counts"].items():
            unique_id_counts.append({"id_type": key, "count": int(value)})
        singleton_node_analysis = []
        for key, value in report["singleton_analysis"].items():
            singleton_node_analysis.append(
                {"id_type": key, "singleton_count": int(value)}
            )
        return {
            "entity": self.entity["Name"],
            "main_id_column_name": self.entity["IdColumnName"],
            "average_edge_count": report["average_edge_count"],
            "node_types": report["node_types"],
            "top_nodes": report["top_nodes"],
            "top_clusters": report["top_clusters"],
            "potential_issues": report["potential_issues"],
            "unique_id_counts": unique_id_counts,
            "singleton_node_analysis": singleton_node_analysis,
        }

    def _request(self, prompt: str):
        body = {
            "prompt": prompt,
            "session_id": self.session_id,
            "tables": {
                "edges": self.table_report.edges_table,
                "id_graph": self.table_report.output_table,
            },
            "warehouse_credentials": self.warehouse_credentials,
            "report": self._get_report(self.table_report.analysis_results),
        }
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.post(LLM_SERVICE_URL, json=body, headers=headers)
        if not response.ok:
            status_code = response.status_code
            error_response = response.json()["message"]
            if status_code == 401 or status_code == 403:
                return self._prompt_for_access_token(
                    f"\n{error_response}: The provided access token is invalid. Please enter a valid access token."
                )
            print(f"\n{status_code} {error_response}\n")
        else:
            data = response.json()
            message = data["result"]["message"]
            self.session_id = data["session_id"]
            print(f"\n\n{message}\n\n")
