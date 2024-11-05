from typing import Dict


def dfs(node, visited, component, graph):
    visited.add(node)
    component.append(node)
    for neighbor in graph[node]:
        if neighbor not in visited:
            dfs(neighbor, visited, component, graph)


class YamlReport:
    def __init__(self, edge_sources, entity):
        self.edge_sources = edge_sources
        self.entity = entity

    def _get_id_types(self, edge_source):
        entity = self.entity
        result = []
        input_model = edge_source["input_model"]
        if "ids" not in edge_source or len(edge_source["ids"]) == 0:
            for id in input_model.ids():
                if id.entity() == entity["Name"]:
                    result.append(id.type())
            return result
        for id in edge_source["ids"]:
            for input_model_id in input_model.ids():
                if input_model_id.entity() == entity["Name"]:
                    result.append(input_model_id.type())
        return result

    def edge_source_pairs(self) -> Dict:
        result = {}
        for edge_source in self.edge_sources:
            ids = self._get_id_types(edge_source)
            pairs = []
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    pair = (ids[i], ids[j])
                    pairs.append(pair)
            result[edge_source["from"]] = pairs
        return result

    def run(self):
        graph = {}
        id_types = set()
        for edge_source in self.edge_sources:
            ids = self._get_id_types(edge_source)
            if len(ids) <= 1:
                input_model_name = edge_source["input_model"].name()
                print(
                    f"{input_model_name} has {len(ids)} id type, so it never links to other id types. You may want to add id types in this table to create edges."
                )
            for i, id1 in enumerate(ids):
                id_types.update([id1])
                if id1 not in graph:
                    graph[id1] = []
                for j, id2 in enumerate(ids):
                    if j >= i + 1:
                        if id2 not in graph:
                            graph[id2] = []
                        graph[id1].append(id2)
                        graph[id2].append(id1)
        components = []
        visited = set()
        for node in graph:
            if node not in visited:
                component = []
                dfs(node, visited, component, graph)
                components.append(component)
        if len(components) > 1:
            print(
                "\n**Disconnected graphs:**\nYour id stitcher definition does not produce a connected graph.\nIt creates",
                len(components),
                "distinct connected components:",
            )
            for i, component in enumerate(components):
                print(i + 1, ":", component)
            print(
                "These",
                len(components),
                "groups are not connected to each other, so one id within each group is never connected to another id in a different group. This typically creates an understitched graph",
            )
        missing_id_types = []
        for id_type in self.entity["IdTypes"]:
            if id_type == "rudder_id":
                continue
            if id_type not in id_types:
                missing_id_types.append(id_type)
        if len(missing_id_types) > 0:
            print(
                "\n**Missing id types:**\nThese id types are defined in the entity but not present in the inputs:",
                missing_id_types,
            )
