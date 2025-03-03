import os
import webbrowser
import pandas as pd
import numpy as np
from typing import Dict
from urllib.parse import urljoin
from urllib.request import pathname2url
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
        self.color_map = {}
        self.cluster_specific_id_types = set()

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
            node1 = (row["id1"], row["id1_type"])
            node2 = (row["id2"], row["id2_type"])
            G.add_edge(node1, node2)
            G.nodes[node1]["id_type"] = row["id1_type"]
            G.nodes[node2]["id_type"] = row["id2_type"]
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

        # Determine visualization strategy based on graph size
        if len(G) <= 200:
            return self._visualize_small_graph(G, file_path)
        else:
            return self._visualize_large_graph(G, file_path)

    def _visualize_small_graph(self, G: nx.Graph, file_path: str):
        net = self._initialise_network()
        degrees, max_degree = self._pre_compute_graph_info(G)

        # Add nodes and edges for the visualization graph
        for node, attrs in G.nodes(data=True):
            color = self._get_node_color(attrs["id_type"], degrees[node], max_degree)
            net.add_node(
                f"{node[0]}<br>{node[1]}",  # complex nodes can't be used as ids in pyvis but they can be used in networkx
                label=node[0],
                color=color,
                title=f"ID: {node[0]}\nID-Type: {attrs['id_type']}\nNo. of connections: {degrees[node]}",
            )

        for source, target in G.edges():
            net.add_edge(
                f"{source[0]}<br>{source[1]}",
                f"{target[0]}<br>{target[1]}",
                color="#888888",
            )

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
        self._add_legend_to_file(file_path)
        return file_path

    def _visualize_large_graph(self, G: nx.Graph, file_path: str):
        net = self._initialise_network()
        degrees, max_degree = self._pre_compute_graph_info(G)

        # Pre-calculate layout using NetworkX
        print("Computing initial layout...")
        layout = nx.spring_layout(
            G,
            k=1 / np.sqrt(len(G)),  # Optimal distance between nodes
            iterations=50,  # Reduce iterations for large graphs
            seed=42,  # For consistency
        )

        # Scale layout to reasonable coordinates
        scale_factor = 1000
        layout = {
            node: (coord[0] * scale_factor, coord[1] * scale_factor)
            for node, coord in layout.items()
        }

        # Add nodes and edges for the visualization graph
        print("Adding nodes...")
        for node, attrs in G.nodes(data=True):
            x, y = layout[node]
            color = self._get_node_color(attrs["id_type"], degrees[node], max_degree)
            size = 5 + (degrees[node] / max_degree) * 15  # Smaller node sizes
            net.add_node(
                f"{node[0]}<br>{node[1]}",  # complex nodes can't be used as ids in pyvis but they can be used in networkx
                label=node[0],
                x=int(x),
                y=int(y),
                physics=False,  # Disable physics for pre-positioned nodes
                size=size,
                color=color,
                title=f"ID: {node[0]}\nID-Type: {attrs['id_type']}\nNo. of connections: {degrees[node]}",
            )

        print("Adding edges...")
        for source, target in G.edges():
            net.add_edge(
                f"{source[0]}<br>{source[1]}",
                f"{target[0]}<br>{target[1]}",
                color="#88888844",
                width=0.5,
            )

        net.set_options(
            """
            var options = {
                "physics": {
                    "enabled": false
                },
                "nodes": {
                    "fixed": true,
                    "shape": "dot",
                    "font": {
                        "size": 8,
                        "color": "black",
                        "face": "arial"
                    },
                    "scaling": {
                        "min": 5,
                        "max": 20
                    },
                    "shadow": false,
                    "borderWidth": 2,
                    "borderWidthSelected": 4
                },
                "edges": {
                    "smooth": false,
                    "shadow": false,
                    "width": 0.5,
                    "selectionWidth": 2,
                    "color": {
                        "opacity": 0.25
                    }
                }
            }
        """
        )

        net.save_graph(file_path)
        self._add_legend_to_file(file_path)
        return file_path

    def _pre_compute_graph_info(self, G: nx.Graph):
        degrees = dict(G.degree())
        max_degree = max(degrees.values()) if degrees else 0
        return degrees, max_degree

    def _initialise_network(self):
        # Initialize network
        net = Network(
            notebook=False,
            height="1000px",
            width="100%",
            bgcolor="#ffffff",
            font_color="black",
            directed=False,
        )
        return net

    def _add_legend_to_file(self, file_path):
        legend_items = [
            f"""
            <div style="display: flex; align-items: center; margin-bottom: 5px;">
                <div style="width: 20px; height: 20px; background-color: {color}; 
                     margin-right: 10px; border-radius: 3px;"></div>
                <div>{id_type}</div>
            </div>
            """
            for id_type, color in self.color_map.items()
            if id_type in self.cluster_specific_id_types
        ]

        legend_html = f"""
        <div style="position: fixed; top: 20px; right: 20px; background-color: rgba(255, 255, 255, 0.9); 
                    padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); z-index: 1000;">
            <div style="font-weight: bold; margin-bottom: 10px;">ID Types</div>
            {''.join(legend_items)}
        </div>
        """

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read()

        modified_content = content.replace("<body>", f"<body>{legend_html}")

        with open(file_path, "w", encoding="utf-8") as file:
            file.write(modified_content)

    def _generate_color_map(self, id_types):
        HSV_tuples = [(x * 1.0 / len(id_types), 0.7, 0.7) for x in range(len(id_types))]
        colors = [
            "#%02x%02x%02x" % tuple(int(i * 255) for i in colorsys.hsv_to_rgb(*hsv))
            for hsv in HSV_tuples
        ]
        return dict(zip(id_types, colors))

    def _get_node_color(self, id_type, degree, max_degree):
        base_color = self.color_map.get(id_type, "#808080")
        opacity = 0.3 + (degree / max_degree) * 0.7 if max_degree > 0 else 1
        rgb = tuple(int(base_color.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
        return f"rgba{rgb + (opacity,)}"

    def run(self):
        if len(self.table_report.analysis_results["node_types"]) <= 1:
            print(
                "The ID stitcher has only a single ID type. Skipping cluster analysis."
            )
            return
        print(
            "You can explore specific clusters by entering an ID to see how the other ids are all connected and the cluster is formed."
        )
        print("The ID can be either the main ID or any other ID type.")
        self.color_map = self._generate_color_map(
            self.table_report.analysis_results["node_types"]
        )
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
            self.cluster_specific_id_types = set(
                nx.get_node_attributes(G, "id_type").values()
            )
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
                print(
                    f"\nVisualising all ids and their connections in the cluster as a graph.\n\n"
                )
                self.create_interactive_graph(G, file_path)
                print(
                    f"Your network visualization is ready! We've saved a map of your data connections here:\n{file_path}"
                )
                try:
                    file_url = urljoin("file:", pathname2url(file_path))
                    webbrowser.open_new_tab(file_url)
                except:
                    self.logger.warn("Unable to open the visualisation.")
            print(f"Cluster Summary:\n{cluster_summary}\n\n")
