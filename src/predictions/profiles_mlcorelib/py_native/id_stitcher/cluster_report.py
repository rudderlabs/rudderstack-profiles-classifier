import os
from typing import Dict
import pandas as pd
from profiles_rudderstack.client import BaseClient

# TODO: Uncomment the following line after adding the Reader class to the profiles_rudderstack package
# from profiles_rudderstack.reader import Reader
import networkx as nx
import plotly.graph_objects as go
import networkx as nx
from .table_report import TableReport
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
        reader,
        wh_client: BaseClient,
        entity: Dict,
        table_report: TableReport,
    ):
        self.reader = reader
        self.wh_client = wh_client
        self.entity = entity
        self.table_report = table_report

    def get_edges_data(self, node_id: str) -> pd.DataFrame:
        cluster_query_template = """
            with cluster_cte as 
            (select other_id from {id_graph_table} where {entity_main_id} in 
            (select {entity_main_id} from {id_graph_table} where other_id = '{node_id}' or {entity_main_id} = '{node_id}')
            )
            select distinct id1, id1_type, id2, id2_type from 
            (select id1, id1_type, id2, id2_type from {edges_table} where id1 in (select other_id from cluster_cte) 
            union all
            select id1, id1_type, id2, id2_type from {edges_table} where id2 in (select other_id from cluster_cte) )
            where id1 != id2 and id1_type != id2_type
        """
        query = cluster_query_template.format(
            entity_main_id=self.entity["IdColumnName"],
            edges_table=self.table_report.edges_table,
            id_graph_table=self.table_report.output_table,
            node_id=node_id,
        )
        result = self.wh_client.query_sql_with_result(query)
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
        if G.number_of_nodes() > 50:
            print(
                f"As there are too many ids ({G.number_of_nodes()}) in this cluster, we use an approximate betweenness centrality calculation to identify bridge nodes"
            )
            betweenness_centrality = nx.betweenness_centrality(G, k=50)
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
        if G.number_of_nodes() > 300:
            pos = nx.random_layout(G)
        else:
            # Use Fruchterman-Reingold layout for initial node positions
            pos = nx.spring_layout(G, k=0.5, iterations=50)
        # Create edges
        edge_x = []
        edge_y = []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])

        edge_trace = go.Scatter(
            x=edge_x,
            y=edge_y,
            line=dict(width=0.5, color="#888"),
            hoverinfo="none",
            mode="lines",
            showlegend=False,
        )

        # Create nodes
        node_x = []
        node_y = []
        node_text = []
        node_color = []

        # Create a dynamic color map
        distinct_id_types = set(G.nodes[node]["id_type"] for node in G.nodes())
        color_scheme = self._generate_color_scheme(distinct_id_types)

        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(f"Node: {node}<br>Type: {G.nodes[node]['id_type']}")
            node_color.append(color_scheme[G.nodes[node]["id_type"]])

        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers",
            hoverinfo="text",
            text=node_text,
            marker=dict(showscale=False, color=node_color, size=10, line_width=2),
            showlegend=False,
        )

        # Create figure
        fig = go.Figure(data=[edge_trace, node_trace])

        # Add dragmode and newshape parameters
        fig.update_layout(
            title=f"Network Graph",
            showlegend=False,
            hovermode="closest",
            dragmode="pan",
            clickmode="event+select",
            newshape=dict(line_color="cyan"),
            margin=dict(b=20, l=5, r=5, t=40),
            annotations=[
                dict(
                    text="Network Graph",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.005,
                    y=-0.002,
                )
            ],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        )

        # Make nodes draggable
        fig.update_traces(
            marker=dict(size=10, line=dict(width=2, color="DarkSlateGrey")),
            selector=dict(mode="markers"),
        )

        # Add legend
        for id_type, color in color_scheme.items():
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode="markers",
                    marker=dict(size=10, color=color),
                    name=id_type,
                    showlegend=True,
                )
            )

        # Save the figure

        fig.write_html(
            file_path,
            auto_open=False,
            include_plotlyjs="cdn",
            config={
                "displayModeBar": True,
                "scrollZoom": True,
                "editable": True,
                "modeBarButtonsToAdd": ["drawopenpath", "eraseshape"],
            },
        )

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
                "Enter an ID to visualize (or 'quit' to exit and go to the next step): "
            )
            if user_input.lower() in ["quit", "exit", "done"]:
                print("Exiting interactive mode.")
                break
            print("\n\n")
            metrics, G = self._analyse_cluster(user_input)
            cluster_summary = self.get_cluster_summary(metrics)
            os.makedirs(output_dir, exist_ok=True)
            filename = f"{user_input}_graph.html"
            file_path = os.path.join(output_dir, filename)
            self.create_interactive_graph(G, file_path)
            print(f"Cluster Summary:\n{cluster_summary}\n\n")
