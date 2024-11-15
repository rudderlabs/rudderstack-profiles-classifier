import os
from typing import Dict
import pandas as pd
from profiles_rudderstack.client import BaseClient
from profiles_rudderstack.material import WhtMaterial
from profiles_rudderstack.logger import Logger

from profiles_rudderstack.reader import Reader
import networkx as nx
from pyvis.network import Network
import plotly.graph_objects as go
import networkx as nx
from .table_report import TableReport
from ..warehouse import run_query
import colorsys

cluster_analysis_prompt = """
        Network overview:
        -----------------
        1. Size: {num_nodes} ids connected by {num_edges} links
        2. Connectivity: On average, each id is connected to {avg_degree:.1f} other ids
        3. Spread: The farthest connected ids are {diameter} steps apart

        Key Players:
        ------------       

        Most connected ids:
        {top_degree_nodes}

        Critical Linking Nodes, and percent of id pairs they are connecting (these are the nodes that act as bridges across all nodes):
        {top_betweenness_nodes}

        """


class ClusterReport:
    def __init__(
        self,
        reader: Reader,
        this: WhtMaterial,
        entity: Dict,
        table_report: TableReport,
        logger: Logger,
    ):
        self.reader = reader
        self.wh_client = this.wht_ctx.client
        self.entity = entity
        self.table_report = table_report
        self.counter = 0
        self.logger = logger

    def get_edges_data(self, node_id: str) -> pd.DataFrame:
        cluster_query_template = """
            with cluster_cte as 
            (select other_id from {db}.{schema}.{id_graph_table} where {entity_main_id} in 
            (select {entity_main_id} from {db}.{schema}.{id_graph_table} where other_id = '{node_id}' or {entity_main_id} = '{node_id}')
            )
            select distinct id1, id1_type, id2, id2_type from 
            (select id1, id1_type, id2, id2_type from {db}.{schema}.{edges_table} where id1 in (select other_id from cluster_cte) 
            union all
            select id1, id1_type, id2, id2_type from {db}.{schema}.{edges_table} where id2 in (select other_id from cluster_cte) )
            where id1 != id2 and id1_type != id2_type
        """
        query = cluster_query_template.format(
            entity_main_id=self.entity["IdColumnName"],
            edges_table=self.table_report.edges_table,
            id_graph_table=self.table_report.output_table,
            db=self.table_report.db,
            schema=self.table_report.schema,
            node_id=node_id,
        )
        result = run_query(self.wh_client, query)
        result.columns = [col.lower() for col in result.columns]
        return result

    def create_graph_with_metadata(self, edges):
        G = nx.Graph()
        for _, row in edges.iterrows():
            G.add_edge(row["id1"], row["id2"])
            G.nodes[row["id1"]]["id_type"] = row["id1_type"]
            G.nodes[row["id2"]]["id_type"] = row["id2_type"]
        return G

    def compute_graph_metrics(
        self, G: nx.Graph, degree_top_n: int = 10, betweenness_top_n: int = 10
    ) -> dict:
        metrics = {}
        # Basic Graph Metrics
        metrics["num_nodes"] = G.number_of_nodes()
        metrics["num_edges"] = G.number_of_edges()
        metrics["avg_degree"] = sum(dict(G.degree()).values()) / metrics["num_nodes"]
        # Degree Centrality
        degree_centrality = nx.degree_centrality(G)
        sorted_degree = sorted(
            degree_centrality.items(), key=lambda x: x[1], reverse=True
        )
        metrics["top_degree_nodes"] = [node for node, _ in sorted_degree[:degree_top_n]]
        metrics["edge_count"] = {node: degree for node, degree in G.degree()}

        # Bridge Nodes (combining betweenness and articulation points)
        if G.number_of_nodes() > 100:
            self.logger.warn(
                f"Skipping complex metrics for large graph with {G.number_of_nodes()} nodes"
            )
            metrics["top_bridge_nodes"] = []
            metrics["betweenness_centrality"] = {}
            metrics["diameter"] = -1
        else:
            betweenness_centrality = nx.betweenness_centrality(G)
            articulation_points = set(nx.articulation_points(G))
            bridge_nodes = sorted(
                [
                    (node, score)
                    for node, score in betweenness_centrality.items()
                    if node in articulation_points
                ],
                key=lambda x: x[1],
                reverse=True,
            )
            metrics["top_bridge_nodes"] = [
                node for node, _ in bridge_nodes[:betweenness_top_n]
            ]
            metrics["betweenness_centrality"] = betweenness_centrality
            # Diameter (of the largest component)
            largest_cc = max(nx.connected_components(G), key=len)
            largest_cc_subgraph = G.subgraph(largest_cc)
            metrics["diameter"] = nx.diameter(largest_cc_subgraph)
        return metrics

    def _analyse_cluster(self, node_id: str):
        edges_df = self.get_edges_data(node_id)
        if len(edges_df) == 0:
            print(
                f"ID {node_id} is not found in the graph, either in other_ids or in the main_id. The ids are case-sensitive and do not do partial match. Please enter a valid ID."
            )
            return None, None
        G = self.create_graph_with_metadata(edges_df)
        cluster_summary = self.compute_graph_metrics(G)
        return cluster_summary, G

    def get_cluster_summary(self, metrics: dict) -> str:
        cluster_summary = cluster_analysis_prompt.format(
            num_nodes=metrics["num_nodes"],
            num_edges=metrics["num_edges"],
            avg_degree=metrics["avg_degree"],
            top_degree_nodes=self._format_node_list(
                metrics["top_degree_nodes"], metrics["edge_count"], "No:of edges"
            ),
            top_betweenness_nodes=self._format_node_list(
                metrics["top_bridge_nodes"],
                metrics["betweenness_centrality"],
                "Betweenness Centrality",
            ),
            diameter=metrics["diameter"],
        )
        return cluster_summary

    def _format_node_list(self, nodes, metric_dict, metric_name):
        if metric_name == "No:of edges":
            return "\n\t".join(
                [f"- ID: {node}, {metric_name}: {metric_dict[node]}" for node in nodes]
            )
        elif metric_name == "Betweenness Centrality":
            return "\n\t".join(
                [
                    f"- ID: {node}, % of shortest paths: {metric_dict[node]*100:.2f}%"
                    for node in nodes
                ]
            )

    def create_interactive_graph(self, G: nx.Graph, file_path: str):
        # Removing self-loops from the graph
        G.remove_edges_from(nx.selfloop_edges(G))

        def generate_colors(n):
            HSV_tuples = [(x * 1.0 / n, 0.7, 0.7) for x in range(n)]
            return [
                "#%02x%02x%02x" % tuple(int(i * 255) for i in colorsys.hsv_to_rgb(*hsv))
                for hsv in HSV_tuples
            ]

        def get_opacity(degree, max_degree):
            return 0.3 + (degree / max_degree) * 0.7

        id_types = set(nx.get_node_attributes(G, "id_type").values())
        colors = generate_colors(len(id_types))
        color_map = dict(zip(id_types, colors))

        degrees = dict(G.degree())
        max_degree = max(degrees.values()) if degrees else 0

        net = Network(
            notebook=False,
            height="1000px",
            width="100%",
            bgcolor="#ffffff",
            font_color="black",
        )

        # Add nodes and edges for the visualization graph
        for node, attrs in G.nodes(data=True):
            color = color_map.get(attrs["id_type"], "#808080")
            degree = degrees[node]
            opacity = get_opacity(degree, max_degree)
            rgba_color = f'rgba{tuple(int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4)) + (opacity,)}'
            net.add_node(
                node,
                color=rgba_color,
                title=f"ID: {node}\nID-Type: {attrs['id_type']}\nDegree: {degree}",
            )

        for source, target in G.edges():
            net.add_edge(source, target, color="#888888")

        net.set_options(
            """
            var options = {
                "physics": {
                    "forceAtlas2Based": {
                    "gravitationalConstant": -50,
                    "centralGravity": 0.01,
                    "springLength": 100,
                    "springConstant": 0.08
                    },
                    "minVelocity": 0.75,
                    "solver": "forceAtlas2Based"
                },
                "nodes": {
                    "font": {
                    "color": "black"
                    },
                    "borderWidth": 2,
                    "borderWidthSelected": 4
                },
                "edges": {
                    "color": {
                    "inherit": false
                    },
                    "smooth": {
                    "enabled": true,
                    "type": "dynamic"
                    }
                }
            }
        """
        )
        net.save_graph(file_path)

        # Create legend HTML
        legend_items = []
        for id_type, color in color_map.items():
            legend_items.append(
                f"""
                <div style="display: flex; align-items: center; margin-bottom: 5px;">
                    <div style="width: 20px; height: 20px; background-color: {color}; margin-right: 10px; border-radius: 3px;"></div>
                    <div>{id_type}</div>
                </div>
            """
            )

        legend_html = f"""
        <div style="position: fixed; top: 20px; right: 20px; background-color: rgba(255, 255, 255, 0.9); 
                    padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); z-index: 1000;">
            <div style="font-weight: bold; margin-bottom: 10px;">ID Types</div>
            {''.join(legend_items)}
        </div>
        """

        # Add the legend HTML in saved file
        with open(file_path, "r") as file:
            content = file.read()

        modified_content = content.replace("<body>", f"<body>{legend_html}")

        with open(file_path, "w") as file:
            file.write(modified_content)

        print(
            f"Your network visualization is ready! We've saved an interactive map of your data connections here:\n{file_path}"
        )
        print(
            "You can open this file in your web browser to explore the network visually."
        )

    def _generate_color_scheme(self, id_types):
        # Generate a color for each unique ID type
        num_colors = len(id_types)
        color_scheme = {}
        for i, id_type in enumerate(id_types):
            hue = i / num_colors
            rgb = colorsys.hsv_to_rgb(hue, 0.7, 0.9)  # Saturation: 0.7, Value: 0.9
            hex_color = "#{:02x}{:02x}{:02x}".format(
                int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255)
            )
            color_scheme[id_type] = hex_color
        return color_scheme

    def run(self):
        print(
            "You can explore specific clusters by entering an ID to see how the other ids are all connected and the cluster is formed."
        )
        print("The ID can be either the main ID or any other ID type.")
        output_dir = os.path.join(os.getcwd(), "graph_outputs")
        while True:
            user_input = self.reader.get_input(
                "Enter an ID to visualize (or 'skip' to skip this step): "
            )
            if user_input.lower() in ["quit", "exit", "done", "skip"]:
                print("Exiting interactive mode.")
                break
            print("\n\n")
            metrics, G = self._analyse_cluster(user_input)
            if metrics is None:
                continue
            cluster_summary = self.get_cluster_summary(metrics)
            self.counter += 1
            if metrics.get("num_nodes", 0) > 1000:
                self.logger.warn(
                    f"Skipping visualization as it is connected to too many ({metrics['num_nodes']}) other ids."
                )
            else:
                os.makedirs(output_dir, exist_ok=True)
                filename = f"{user_input}_graph.html"
                file_path = os.path.join(output_dir, filename)
                self.create_interactive_graph(G, file_path)
            print(f"Cluster Summary:\n{cluster_summary}\n\n")
