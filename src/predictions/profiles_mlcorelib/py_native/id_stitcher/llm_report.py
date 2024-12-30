import os
from typing import Dict
import requests
from profiles_rudderstack.material import WhtMaterial

from .config import LLM_ID_DEBUG_URL, LLM_INVOKE_URL
from .prompts import id_stitcher_static_report_prompt
from .table_report import TableReport
from .consent_manager import ConsentManager
from profiles_rudderstack.logger import Logger

from profiles_rudderstack.reader import Reader
from enum import Enum
from colorama import init, Fore, Style

init()  # Initialize colorama


class Colors:
    BLUE = Fore.BLUE
    YELLOW = Fore.YELLOW
    GREEN = Fore.GREEN
    ENDC = Style.RESET_ALL


class ProgramState(Enum):
    STOP = 1


class LLMReport:
    def __init__(
        self,
        reader,
        this: WhtMaterial,
        access_token: str,
        logger: Logger,
        table_report: TableReport,
        entity: Dict,
    ):
        self.access_token = access_token
        self.warehouse_credentials = this.base_wht_project.warehouse_credentials()
        # TODO: This is a temporary fix. permanent fix will be from wht.
        if "port" in self.warehouse_credentials:
            self.warehouse_credentials["port"] = int(self.warehouse_credentials["port"])
        self.table_report = table_report
        self.reader = reader
        self.entity = entity
        self.session_id = ""
        self.logger = logger

    def check_consent(self):
        consent_manager = ConsentManager(self.logger)
        consent = consent_manager.get_stored_consent()
        if consent is None:
            consent = consent_manager.prompt_for_consent(self.reader)
        return consent

    def run(self):
        consent = self.check_consent()
        if not consent:
            print("LLM-based analysis is disabled. Skipping LLM-based analysis.")
            return

        if not self.access_token:
            state = self._prompt_for_access_token(
                "We couldn't find RudderStack access token in your siteconfig file. To access the LLM analysis, please enter an access token (if you don't have one, you can create it from the RudderStack dashboard)."
            )
            if state == ProgramState.STOP:
                return
        try:
            self._interpret_results_with_llm()
        except Exception:
            # If the Analysis of the report fails, we don't want to stop the program.
            pass
        print("You can now ask questions about the ID Stitcher analysis results.")
        self.run_interactive_session()

    def run_interactive_session(self):
        while True:
            user_input: str = self.reader.get_input(
                "Enter your question. (or 'quit' to skip this step): \n"
            )
            if user_input.lower() in ["quit", "exit", "done"]:
                break
            body = {
                "prompt": user_input,
                "session_id": self.session_id,
                "tables": {
                    "edges": self.table_report.edges_table,
                    "id_graph": self.table_report.output_table,
                },
                "warehouse_credentials": self.warehouse_credentials,
                "report": self._get_report(self.table_report.analysis_results),
            }
            response = self._request(LLM_ID_DEBUG_URL, body)
            if response == ProgramState.STOP:
                break
            debug_info_message = response["debug_info"]["messages"][0]
            response_message = response["result"]["message"]
            self.session_id = response["session_id"]
            print(
                f"\n\n{Colors.BLUE}Question:{Colors.ENDC}\n\t{user_input}\n\n{Colors.YELLOW}Thought:{Colors.ENDC}\n\t{debug_info_message}\n\n{Colors.GREEN}Response:{Colors.ENDC}\n\t{response_message}\n\n"
            )

    def _interpret_results_with_llm(self) -> str:
        results = self.table_report.analysis_results
        prompt = id_stitcher_static_report_prompt.format(
            entity_name=self.entity["Name"],
            node_types=results["node_types"],
            unique_id_counts=results["unique_id_counts"],
            top_nodes=results["top_nodes"],
            average_edge_count=results["average_edge_count"],
            potential_issues=results["potential_issues"],
            clusters=results["clusters"],
            top_clusters=results["top_clusters"],
            singleton_analysis=results["singleton_analysis"],
            cluster_stats=results["cluster_stats"],
        )
        body = {
            "prompt": prompt,
        }
        response = self._request(LLM_INVOKE_URL, body)
        if response == ProgramState.STOP:
            return
        message = response["message"]
        print(f"\n{message}\n\n")

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
            "average_edge_count": float(report["average_edge_count"]),
            "node_types": report["node_types"],
            "top_nodes": report["top_nodes"],
            "clusters": int(report["clusters"]),
            "top_clusters": report["top_clusters"],
            "potential_issues": report["potential_issues"],
            "unique_id_counts": unique_id_counts,
            "singleton_node_analysis": singleton_node_analysis,
        }

    def _request(self, url, body):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.post(url, json=body, headers=headers)
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
            return data
