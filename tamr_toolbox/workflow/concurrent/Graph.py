import logging
from typing import List, Set, Tuple

from dataclasses import dataclass
import networkx as nx

from tamr_unify_client import Client
from tamr_unify_client.project.resource import Project
from tamr_toolbox.models.project_type import ProjectType
from typing import Dict, Any

LOGGER = logging.getLogger(__name__)


@dataclass()
class Graph:
    """
    A dataclass for holding a set of Tamr project dependencies (edges),
    and the generated graph from them.
    """

    edges: set
    directed_graph: nx.DiGraph


def _get_upstream_projects(project: Project) -> List[Project]:
    """
    get projects immediately upstream of a given project
    Args:
        project: the project to check
    Returns:
        A list of project names upstream of the project
    """
    client = project.client
    # find upstream datasets - if GR project just get input datasets
    if ProjectType[project.type] == ProjectType.GOLDEN_RECORDS:
        upstream_datasets = [x for x in project.input_datasets().stream()]
    # else find the upstream datasets of the UD (not input datasets to capture datasets used in Tx)
    else:
        unified_dataset_id = project.unified_dataset().relative_id
        unified_dataset = client.datasets.by_relative_id(unified_dataset_id)
        upstream_datasets = unified_dataset.upstream_datasets()

    upstream_project_names = []
    # walk through upstream datasets
    for upstream_result in upstream_datasets:
        # get the upstream object as a dataset
        upstream_dataset = client.datasets.by_resource_id(upstream_result.resource_id)
        # see if it is the output of a project and if so add to the list
        upstream_dataset_projects = set(
            x.project_name for x in upstream_dataset.usage().usage.output_from_project_steps
        )
        upstream_project_names.extend([x for x in upstream_dataset_projects])

    return [client.projects.by_name(x) for x in upstream_project_names]


def _build_edges(project: Project, client: Client, edges: set = None) -> Set[Tuple[str, str]]:
    """
    builds a set of tuples of all edges of format (source, target)
    Args:
        project: the project to get edges for
        client: tamr client
        edges: set of tuples (source, target)

    Returns:

    """
    upstream_projects = _get_upstream_projects(project)

    if edges is None:
        edges = set()

    for upstream_project in upstream_projects:
        # add the edge for this upstream dataset
        edges.add((upstream_project.name, project.name))
        # if we've already walked backward for this upstream dataset don't keep walking
        # you know we've walked it if it shows up as the target in an edge (hence the [1] index)
        if any(upstream_project.name == x[1] for x in edges):
            LOGGER.debug(
                f"skipping dataset {upstream_project.name} since it is already"
                f" in edges as target: {[ x for x in edges if x[1] ==upstream_project.name]}"
            )
            continue
        else:
            # and then go to it and get it's upstream datasets
            further_upstream_edges = _build_edges(upstream_project, client, edges=set(edges))
            # print(f"adding further upstream edges {further_upstream_edges}")
            edges = edges.union(further_upstream_edges)

    return edges


def from_edges(edges: Set[tuple]) -> Graph:
    """
    Directly build a graph from a list of edges - tuples of format (source, target) dependencies

    Args:
        edges: List of edges in tuple format

    Returns:
        Graph object
    """
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    return Graph(edges=edges, directed_graph=graph)


def from_project_list(projects: List[Project], client: Client) -> Graph:
    """
    Creates a graph from a list of datasets
    Args:
        projects: list of Tamr dataset objects
        client: tamr client

    Returns:
        A Graph object built from the dependencies of the datasets passed
    """
    # start with empty set
    graph_edges = set()
    # for each dataset get the edges and take union
    for project in projects:
        graph_edges = set(graph_edges.union(_build_edges(project, client)))

    graph = nx.DiGraph()
    graph.add_edges_from(graph_edges)
    return Graph(edges=graph_edges, directed_graph=graph)


def get_source_nodes(graph: Graph) -> List[str]:
    """
    Gives all source nodes in a graph

    Args:
        graph: Graph for which to find source nodes

    Returns:
        List of node names
    """
    di_graph = graph.directed_graph
    source_nodes = []
    for n in di_graph.nodes():
        if not [x for x in di_graph.predecessors(n)]:
            source_nodes.append(n)
    return source_nodes


def get_end_nodes(graph: Graph):
    """
    Returns all end nodes in a directed graph

    Args:
        graph: Graph for which to find end nodes

    Returns:
        List of names of all end nodes
    """
    di_graph = graph.directed_graph
    end_nodes = []
    for n in di_graph.nodes():
        if not [x for x in di_graph.successors(n)]:
            end_nodes.append(n)
    return end_nodes


def get_projects_by_tier(graph: Graph) -> Dict[int, Any]:
    """
    Find the different projects at each tier

    Args:
        graph: the Graph for which to generate the tiers

    Returns:
        A json dict who's structure is {'tier': [paths_at_that_tier], ...}
        e.g. {1: ['SM_project_1', 'Classification_project_1'], 2: ['Mastering_project'],
        3: ['Golden_records_project']}
    """
    source_nodes = get_source_nodes(graph)

    path_tier_dict = {0: []}
    for n in graph.directed_graph.nodes:
        # just add source nodes to tier 0
        if n in source_nodes:
            path_tier_dict[0].append(n)
            continue
        # since all dependent projects must be run first
        # the tier is the maximum length of all simple paths to this node
        # so get the max length from all source nodes
        tier = 0
        for s in source_nodes:
            current_paths = [x for x in nx.all_simple_paths(graph.directed_graph, s, n)]
            # skip source nodes that don't link to this project
            if not current_paths:
                continue
            max_path_length = len(max(current_paths, key=lambda x: len(x)))
            if max_path_length > tier:
                tier = max_path_length

        # decrement tier to count from 0
        tier = tier - 1
        # check if tier exists in the dict, if so add to it, else create entry
        if tier in path_tier_dict:
            path_tier_dict[tier].append(n)
        else:
            path_tier_dict[tier] = [n]

    return {k: set(v) for k, v in path_tier_dict.items()}


def get_all_downstream_nodes(graph: Graph, node: str) -> Set[str]:
    """
    Get all nodes downstream of this one (i.e. they have a path from this node to them)

    Args:
        graph: the graph to use
        node: the node to check

    Returns:
        A list of downstream node names
    """
    downstream_paths = []
    diGraph = graph.directed_graph
    for n in diGraph.nodes():
        all_paths = [x for x in nx.all_simple_paths(diGraph, node, n)]
        if all_paths:
            for path in all_paths:
                downstream_paths.extend(path)

    # the above generates a list of things like
    # [projectA, projectB, projectA, projectC] if projectA goes to both project_B and project_C
    # so make a set, then remove the node itself
    downstream_nodes = {x for x in set(downstream_paths) if x != node}
    return downstream_nodes


def get_successors(graph: Graph, node: str) -> Set[str]:
    """
    Get all successor nodes to the current node

    Args:
        graph: the graph to use
        node: the node to check

    Returns:
        A set of nodes that are successors to the current node
    """
    di_graph = graph.directed_graph
    return set(x for x in di_graph.successors(node))


def get_predecessors(graph: Graph, node: str) -> Set[str]:
    """
    Get all predecessor nodes to the current node

    Args:
        graph: the graph to use
        node: the node to check

    Returns:
        A set of nodes that are predecessors to the current node
    """
    di_graph = graph.directed_graph
    return set(x for x in di_graph.predecessors(node))


def add_edges(graph: Graph, edges: Set[tuple]) -> Graph:
    """
    Takes an existing graph and creates a new one with the new edge

    Args:
        graph: the graph to start with
        edges: the edges to add

    Returns:
        A copy of initial graph with new edge
    """

    old_edges = set(graph.directed_graph.edges)
    new_edges = old_edges.union(edges)
    return from_edges(new_edges)
