import json
from typing import Any, Dict, List
from profiles_rudderstack.client import BaseClient
from profiles_rudderstack.material import WhtMaterial, WhtModel
from profiles_rudderstack.logger import Logger
from .yaml_report import YamlReport
from ..warehouse import run_query


class TableReport:
    def __init__(
        self,
        this: WhtMaterial,
        model: WhtModel,
        entity,
        yaml_report: YamlReport,
        logger: Logger,
    ):
        self.wh_client = this.wht_ctx.client
        self.db = this.wht_ctx.client.db
        self.schema = this.wht_ctx.client.schema
        self.wh_type = this.wht_ctx.client.wh_type
        self.model = model
        self.edges_table = ""
        self.output_table = ""
        self.entity = entity
        self.analysis_results = {}
        self.yaml_report = yaml_report
        self.logger = logger

    @staticmethod
    def parse_id_stitcher_metadata(row):
        if isinstance(row, str):
            metadata = json.loads(row)
        else:
            metadata = row
        incremental_base_seq_no = (
            metadata.get("dependency_context_metadata", {})
            .get("incremental_base", {})
            .get("seqno")
        )
        if incremental_base_seq_no:
            raise Exception(
                "Audit is not supported on incremental runs. Do a full pb run using rebase_incremental flag and re-run the audit. Please refer to our docs for more details."
            )
        material_objects = metadata.get("material_objects", [])
        return material_objects

    def get_table_names(self):
        model_hash = self.model.hash()
        filter_dict_map = {
            "snowflake": """metadata:"complete":"status" = 2""",
            "redshift": "metadata.complete.status::integer = 2",
            "bigquery": "cast(json_extract_scalar(metadata, '$.complete.status') as int) = 2",
            "databricks": "cast(get_json_object(metadata, '$.complete.status') as int) = 2",
        }
        if self.wh_type not in filter_dict_map:
            raise Exception(
                "Warehouses other than Snowflake, Redshift, BigQuery and Databricks are not supported yet."
            )
        filter_dict_subquery = filter_dict_map[self.wh_type]

        query = f"""
            select * from {self.db}.{self.schema}.MATERIAL_REGISTRY_4
            where
            model_ref = '{self.model.model_ref()}' and
            model_hash = '{model_hash}' and
            {filter_dict_subquery}
            order by end_ts desc
            LIMIT 1;
        """
        result = run_query(self.wh_client, query)
        if result.empty:
            raise ValueError(
                f"no valid run found for the id stitcher model with hash {model_hash}"
            )

        material_objects = result["METADATA"].apply(
            TableReport.parse_id_stitcher_metadata
        )
        material_names = [object["material_name"] for object in material_objects[0]]
        for name in material_names:
            # An assumption is made here that only 3 type of objects exist
            # 1. Edges table
            # 2. Stitcher table
            # 3. Mapping table
            if name.lower().endswith("edges"):
                self.edges_table = name
            elif not name.lower().endswith("mappings"):
                self.output_table = name

    def get_node_types(self):
        query = f"""
        SELECT DISTINCT id1_type FROM {self.db}.{self.schema}.{self.edges_table} 
        UNION DISTINCT 
        SELECT DISTINCT id2_type FROM {self.db}.{self.schema}.{self.edges_table}
        """
        result = run_query(self.wh_client, query)
        return [row for row in result["ID1_TYPE"]]

    def get_unique_id_count(self, id_type: str) -> int:
        query = f"""
        SELECT COUNT(DISTINCT id) as count
        FROM (
            SELECT id1 as id FROM {self.db}.{self.schema}.{self.edges_table} WHERE id1_type = '{id_type}'
            UNION DISTINCT 
            SELECT id2 as id FROM {self.db}.{self.schema}.{self.edges_table} WHERE id2_type = '{id_type}'
        )
        """
        result = run_query(self.wh_client, query)
        return 0 if result.empty else result["COUNT"][0]

    def get_total_main_ids(self) -> int:
        entity_key = self.entity["IdColumnName"]
        query = f"select count(distinct {entity_key}) as count from {self.db}.{self.schema}.{self.output_table}"
        result = run_query(self.wh_client, query)
        return 0 if result.empty else result["COUNT"][0]

    def get_top_nodes_by_edges(
        self, limit: int, id_type: str = None
    ) -> List[Dict[str, Any]]:
        # only id1_type or id2_type filters should be considered in type_condition, not both. we can create a lambda with id1 or id2 and pass it to type_condition
        type_condition = lambda column, id_type: (
            f" AND {column} = '{id_type}'" if id_type else ""
        )
        query = f"""
        SELECT id, id_type, COUNT(*) as edge_count
        FROM (
            SELECT id1 as id, id1_type as id_type FROM {self.db}.{self.schema}.{self.edges_table} where id1 != id2 {type_condition("id1_type", id_type)}
            UNION ALL
            SELECT id2 as id, id2_type as id_type FROM {self.db}.{self.schema}.{self.edges_table} where id1 != id2 {type_condition("id2_type", id_type)}
            UNION ALL
            SELECT id1 as id, id1_type as id_type FROM {self.db}.{self.schema}.{self.edges_table} where id1 = id2 {type_condition("id1_type", id_type)}
        )
        GROUP BY id, id_type
        ORDER BY edge_count DESC
        LIMIT {limit}
        """
        result = run_query(self.wh_client, query)
        return [
            {
                "id": row["ID"],
                "id_type": row["ID_TYPE"],
                "edge_count": row["EDGE_COUNT"],
            }
            for row in result.to_dict(orient="records")
        ]

    def get_average_edge_count(self, id_type: str = None) -> float:
        type_condition = (
            f"AND id1_type = '{id_type}' OR id2_type = '{id_type}'" if id_type else ""
        )
        query = f"""
        SELECT AVG(edge_count) as avg_edge_count
        FROM (
            SELECT id, COUNT(*) as edge_count
            FROM (
                SELECT id1 as id FROM {self.db}.{self.schema}.{self.edges_table} WHERE id1 != id2 {type_condition}
                UNION ALL
                SELECT id2 as id FROM {self.db}.{self.schema}.{self.edges_table} WHERE id1 != id2 {type_condition}
                UNION ALL
                SELECT id1 as id FROM {self.db}.{self.schema}.{self.edges_table} WHERE id1 = id2 {type_condition}
            )
            GROUP BY id
        )
        """
        result = run_query(self.wh_client, query)
        return 0 if result.empty else result["AVG_EDGE_COUNT"][0]

    def get_cluster_stats(self):
        main_id_key = self.entity["IdColumnName"]

        if self.wh_type == "bigquery":
            percentile_subquery = """
                    approx_quantiles(cluster_size, 100)[offset(25)] as p25,
                    approx_quantiles(cluster_size, 100)[offset(50)] as p50,
                    approx_quantiles(cluster_size, 100)[offset(75)] as p75,
                    approx_quantiles(cluster_size, 100)[offset(90)] as p90,
                    approx_quantiles(cluster_size, 100)[offset(99)] as p99
                    """
        elif self.wh_type == "databricks":
            percentile_subquery = """
                    percentile(cluster_size, 0.25) as p25,
                    percentile(cluster_size, 0.5) as p50,
                    percentile(cluster_size, 0.75) as p75,
                    percentile(cluster_size, 0.9) as p90,
                    percentile(cluster_size, 0.99) as p99
                    """
        elif self.wh_type in ("snowflake", "redshift"):
            percentile_subquery = """
                    percentile_cont(0.25) within group (order by cluster_size) as p25,
                    percentile_cont(0.5) within group (order by cluster_size) as p50, 
                    percentile_cont(0.75) within group (order by cluster_size) as p75, 
                    percentile_cont(0.9) within group (order by cluster_size) as p90, 
                    percentile_cont(0.99) within group (order by cluster_size) as p99 
                    """
        else:
            raise Exception(
                "Warehouses other than Snowflake, Redshift, BigQuery and Databricks are not supported yet."
            )

        # Output should indicate the cluster sizes - min, max, count of single, average, median , percentils - 25, 50, 75, 90, 99
        query = f"""
                select 
                sum(case when cluster_size = 1 then 1 else 0 end) as singletons,
                avg(cluster_size) as avg_cluster_size,
                min(cluster_size) as min_cluster_size, 
                max(cluster_size) as max_cluster_size, 
                {percentile_subquery} 
                from 
                (
                select {main_id_key}, count(*) as cluster_size from {self.db}.{self.schema}.{self.output_table} group by {main_id_key}) a 
                """
        result = run_query(self.wh_client, query)
        return result.to_dict(orient="records")

    def get_top_clusters(self, limit: int) -> List[Dict[str, Any]]:
        main_id_key = self.entity["IdColumnName"]
        query = f"""
        SELECT {main_id_key}, COUNT(*) as cluster_size
        FROM {self.db}.{self.schema}.{self.output_table}
        GROUP BY {main_id_key}
        ORDER BY cluster_size DESC
        LIMIT {limit}
        """
        result = run_query(self.wh_client, query)
        return [
            {
                "main_id": row[main_id_key.upper()],
                "cluster_size": row["CLUSTER_SIZE"],
            }
            for row in result.to_dict(orient="records")
        ]

    def analyze_singleton_nodes(self) -> Dict[str, int]:
        main_id_key = self.entity["IdColumnName"]
        query = f"""
        WITH singletons AS (
            SELECT {main_id_key}
            FROM {self.db}.{self.schema}.{self.output_table}
            GROUP BY {main_id_key}
            HAVING COUNT(*) = 1
        )
        SELECT other_id_type, COUNT(*) as singleton_count
        FROM {self.db}.{self.schema}.{self.output_table}
        WHERE {main_id_key} IN (SELECT {main_id_key} FROM singletons)
        GROUP BY other_id_type
        ORDER BY singleton_count DESC
        """
        result = run_query(self.wh_client, query).to_dict(orient="records")
        return {row["OTHER_ID_TYPE"]: row["SINGLETON_COUNT"] for row in result}

    def get_singleton_count(self, id_type: str) -> int:
        query = f"""
        SELECT COUNT(*) as count
        FROM {self.db}.{self.schema}.{self.output_table}
        GROUP BY {self.entity["IdColumnName"]}
        HAVING COUNT(*) = 1 AND MAX(other_id_type) = '{id_type}'
        """
        result = run_query(self.wh_client, query)
        return 0 if result.empty else result["COUNT"][0]

    def check_missing_connections(self, node_types: List[str]) -> List[str]:
        missing_connections = []
        warn = False

        # Get valid edge source pairs from the IDGraphAnalyzer
        valid_pairs = set()
        for _, pairs in self.yaml_report.edge_source_pairs().items():
            valid_pairs.update(pairs)

        for type1, type2 in valid_pairs:
            query = f"""
            SELECT COUNT(*) as count
            FROM {self.db}.{self.schema}.{self.edges_table}
            WHERE (id1_type = '{type1}' AND id2_type = '{type2}')
                OR (id1_type = '{type2}' AND id2_type = '{type1}')
            """
            result = run_query(self.wh_client, query)
            if not result.empty and result["COUNT"][0] == 0:
                warn = True
                issue = f"No direct edges found between {type1} and {type2}"
                missing_connections.append(issue)
        # missing indirect connections too.
        if warn:
            print(
                "Following id types are defined in id stitcher model inputs to come from same table, but we never see them together before id-stitching (they may be linked later by the stitching through other ids though):"
            )
            for issue in missing_connections:
                print(issue)
        print(
            "\nCheck for missing edges between id types (direct or indirect) after id-stitching:"
        )
        missing_connections = []

        # Create a query that checks for co-occurrence of each pair of node types
        main_id_key = self.entity["IdColumnName"]
        query = f"""
        WITH type_pairs AS (
            {' UNION ALL '.join([f"SELECT '{t1}' as type1, '{t2}' as type2" for t1 in node_types for t2 in node_types if t1 < t2])}
        ),
        cluster_type_counts AS (
            SELECT 
                {main_id_key},
                other_id_type,
                COUNT(*) as type_count
            FROM {self.db}.{self.schema}.{self.output_table}
            GROUP BY {main_id_key}, other_id_type
        ),
        pair_occurrences AS (
            SELECT 
                tp.type1,
                tp.type2,
                COUNT(DISTINCT CASE WHEN c1.type_count > 0 AND c2.type_count > 0 THEN c1.{main_id_key} END) as cooccurrence_count
            FROM type_pairs tp
            LEFT JOIN cluster_type_counts c1 ON tp.type1 = c1.other_id_type
            LEFT JOIN cluster_type_counts c2 ON tp.type2 = c2.other_id_type AND c1.{main_id_key} = c2.{main_id_key}
            GROUP BY tp.type1, tp.type2
        )
        SELECT type1, type2, cooccurrence_count
        FROM pair_occurrences
        WHERE cooccurrence_count = 0
        """

        result = run_query(self.wh_client, query)
        if result.empty:
            print("No missing edges found between node types. GREAT!!")
            return missing_connections

        for row in result.to_dict(orient="records"):
            issue = f"Warning: No clusters found containing both {row['TYPE1']} and {row['TYPE2']}"
            missing_connections.append(issue)
            print(f"WARN: {issue}")

        return missing_connections

    def check_for_issues(self, node_types: List[str]) -> List[str]:
        issues = []
        print("\nChecking for potential issues:")

        # Check for overstitching
        top_nodes = self.get_top_nodes_by_edges(10)
        if (
            top_nodes and top_nodes[0]["edge_count"] > 1000
        ):  # Adjust this threshold as needed
            issue = f"Potential overstitching detected: Node {top_nodes[0]['id']} of type {top_nodes[0]['id_type']} has {top_nodes[0]['edge_count']} edges"
            issues.append(issue)
            print(f"WARN: {issue}")

        # Check for understitching
        for node_type in node_types:
            singleton_count = self.get_singleton_count(node_type)
            total_count = self.get_unique_id_count(node_type)
            if total_count > 0:
                singleton_percentage = (singleton_count / total_count) * 100
                if singleton_percentage > 30:  # Adjust this threshold as needed
                    issue = f"Potential understitching for {node_type}: {singleton_percentage:.2f}% are singletons"
                    issues.append(issue)
                    print(f"WARN: {issue}")

        # Check for missing connections between node types
        missing_connections = self.check_missing_connections(node_types)
        issues.extend(missing_connections)

        return issues

    def run(self):
        self.get_table_names()
        entity_key = self.entity["Name"]
        print(f"Edges table name: {self.edges_table}")
        main_id_key = self.entity["IdColumnName"]
        print(f"main id: {main_id_key}")

        print(f"\n\nAnalyzing ID Stitcher for entity: {entity_key}")

        self.analysis_results = {
            "node_types": [],
            "unique_id_counts": {},
            "top_nodes": [],
            "clusters": 0,
            "top_clusters": [],
            "average_edge_count": 0,
            "potential_issues": [],
            "singleton_analysis": {},
        }
        node_types = self.get_node_types()
        self.analysis_results["node_types"] = node_types
        print(f"\tNode types: {node_types}")
        print("\tUnique IDs of each type and their counts:")
        # Check unique IDs for each type
        for node_type in node_types:
            count = self.get_unique_id_count(node_type)
            self.analysis_results["unique_id_counts"][node_type] = count
            print(f"\t\t{node_type}: {count}")
        total_distinct_ids = sum(self.analysis_results["unique_id_counts"].values())

        print("\nTotal Distinct IDs:")
        print(f"\tBefore stitching: {total_distinct_ids}")
        clusters = self.get_total_main_ids()
        self.analysis_results["clusters"] = clusters
        print(f"\tAfter stitching: {clusters}")

        consolidation_rate = (1 - (clusters / total_distinct_ids)) * 100
        print(f"\nConsolidation rate: {consolidation_rate:.2f}% consolidation")

        top_nodes = self.get_top_nodes_by_edges(10)
        self.analysis_results["top_nodes"] = top_nodes
        print("\nTop 10 nodes by edge count:")
        for node in top_nodes:
            print(
                f"\t\tID: {node['id']}, Type: {node['id_type']}, Edges: {node['edge_count']}"
            )

        # Top N nodes by edge count for each type
        print(
            "For each id type, here are the top 5 id values that had the highest edge counts:"
        )
        for node_type in node_types:
            top_nodes = self.get_top_nodes_by_edges(5, node_type)
            print(f"\n\ttype {node_type} by edge count:")
            for node in top_nodes:
                print(f"\t\tID: {node['id']}, Edges: {node['edge_count']}")

        average_edge_count = self.get_average_edge_count()
        self.analysis_results["average_edge_count"] = average_edge_count
        print(
            f"\n\nAverage edge count per node (before stitching): {average_edge_count}\n\n"
        )

        # Average edge count by type
        print("\nAverage edge count by node type (before stitching):")
        for node_type in node_types:
            average_edge_count = self.get_average_edge_count(node_type)
            print(f"\t\t{node_type}: {average_edge_count}")

        print("\n\t\tPOST ID STITCHING ANALYSIS\n\n")
        # Distribution stats for cluster size after stitching
        cluster_stats = self.get_cluster_stats()
        self.analysis_results["cluster_stats"] = cluster_stats
        print("Cluster size after stitching:")
        print(
            f"\t\tNo:of main ids with a single other id (Singleton nodes): {cluster_stats[0]['SINGLETONS']}"
        )
        print(f"\t\tAverage other id counts: {cluster_stats[0]['AVG_CLUSTER_SIZE']}")
        print(
            f"\t\tMin other id counts for a single main id: {cluster_stats[0]['MIN_CLUSTER_SIZE']}"
        )
        print(
            f"\t\tMax other id counts for a single main id: {cluster_stats[0]['MAX_CLUSTER_SIZE']}"
        )
        print(f"\t\t25th percentile: {cluster_stats[0]['P25']:.0f}")
        print(f"\t\t50th percentile: {cluster_stats[0]['P50']:.0f}")
        print(f"\t\t75th percentile: {cluster_stats[0]['P75']:.0f}")
        print(f"\t\t90th percentile: {cluster_stats[0]['P90']:.0f}")
        print(f"\t\t99th percentile: {cluster_stats[0]['P99']:.0f}\n")

        # After stitching, average no:of ids per main id, of different id types
        query = f"""
        select other_id_type, avg(count) as avg_count from 
        (SELECT {main_id_key}, other_id_type, count(*) as count
        FROM {self.db}.{self.schema}.{self.output_table}
        GROUP BY {main_id_key}, other_id_type) a group by other_id_type
        """
        result = run_query(self.wh_client, query)
        print(
            "Average number of ids of different id types, per main id, after stitching:"
        )
        for row in result.to_dict(orient="records"):
            print(f"\t\t{row['OTHER_ID_TYPE']}: {row['AVG_COUNT']}")

        # Main-ids with the highest count of other ids, for each id type
        query = f"""
        select other_id_type, max(count_value) as max_count from
        (SELECT {main_id_key}, other_id_type, count(*) as count_value
        FROM {self.db}.{self.schema}.{self.output_table}
        GROUP BY {main_id_key}, other_id_type) a group by other_id_type
        """
        result = run_query(self.wh_client, query)
        print("\nHighest count of other ids, for each id type, after stitching:")
        for row in result.to_dict(orient="records"):
            print(f"\t\t{row['OTHER_ID_TYPE']}: {row['MAX_COUNT']}")

        # Top N biggest clusters
        top_clusters = self.get_top_clusters(5)
        self.analysis_results["top_clusters"] = top_clusters
        print(
            "\n\nTop 5 biggest clusters after id stitching (and the distinct id types in each cluster):"
        )
        for _, cluster in enumerate(top_clusters):
            print(f"\tMain ID: {cluster['main_id']}, Size: {cluster['cluster_size']}")
            query = f"""
            SELECT other_id_type, count(*) as count 
            FROM {self.db}.{self.schema}.{self.output_table} 
            WHERE {main_id_key} = '{cluster['main_id']}' GROUP BY 1 ORDER BY 2 DESC
            """
            result = run_query(self.wh_client, query).to_dict(orient="records")
            for row in result:
                print(f"\t\t{row['OTHER_ID_TYPE']}: {row['COUNT']}")

        singleton_analysis = self.analyze_singleton_nodes()
        self.analysis_results["singleton_analysis"] = singleton_analysis

        print("\nSingleton Node Analysis (after stitching):")
        # Add the total count of that specific node type also in the output for reference
        for id_type, count in singleton_analysis.items():
            print(
                f"{id_type}: {count} nodes ({round(count/self.analysis_results['unique_id_counts'][id_type] * 100, 2)}%) not connected to any other ID type"
            )

        if len(node_types) > 1:
            # Check for potential issues
            potential_issues = self.check_for_issues(node_types)
            self.analysis_results["potential_issues"] = potential_issues
        print(f"\n\nANALYSIS COMPLETE FOR ENTITY: {entity_key}\n\n")
