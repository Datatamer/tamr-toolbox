import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Union

import networkx as nx
from tamr_unify_client import Client
from tamr_unify_client.project.resource import Project
from typing_extensions import DefaultDict

from tamr_toolbox.models.project_type import ProjectType

LOGGER = logging.getLogger(__name__)


def _get_upstream_projects(project: Project, *, all_projects: List[Project]) -> List[Project]:
    """Get projects immediately upstream of a given project.

    Args:
        project: the project to check
        all_projects: a list of all Projects on the Tamr Core instance

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

    # we have all projects in all_projects variables so only return those whose
    # name is in upstream_project_names
    return [x for x in all_projects if x.name in upstream_project_names]


def _build_edges(
    project: Project,
    client: Client,
    *,
    edges: Union[None, Set[Tuple[str, str]]] = None,
    all_projects: List[Project],
) -> Set[Tuple[str, str]]:
    """
    builds a set of tuples of all edges of format (source, target)
    Args:
        project: the project to get edges for
        client: tamr client
        edges: set of tuples (source, target), or None
        all_projects: a list of all Projects on the Tamr Core instance

    Returns:
        set of tuples describing dependencies in the form (upstream_project_name, project_name)

    """
    upstream_projects = _get_upstream_projects(project, all_projects=all_projects)

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
            # and then go to it and get its upstream datasets
            further_upstream_edges = _build_edges(
                upstream_project, client, edges=set(edges), all_projects=all_projects
            )
            # print(f"adding further upstream edges {further_upstream_edges}")
            edges = edges.union(further_upstream_edges)

    return edges


def from_edges(edges: Set[Tuple[str, str]], nodes: Set[str]) -> nx.DiGraph:
    """Directly build a graph from sets of edges and nodes.

    Edges are tuples of format (source project name, target project name) dependencies. Nodes are
    project names.

    Only used for testing.

    Args:
        edges: Set of edges in tuple format
        nodes: Set of nodes of graph

    Returns:
        nx.DiGraph object
    """
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    graph.add_nodes_from(nodes)
    return graph


def from_project_list(projects: List[Project], client: Client) -> nx.DiGraph:
    """
    Creates a graph from a list of projects
    Args:
        projects: list of Tamr project objects
        client: tamr client

    Returns:
        A nx.DiGraph object built from the dependencies of the projects passed
    """
    # start with empty set
    graph_edges: Set[Tuple[str, str]] = set()
    # save all projects to list so we don't have to hit the API every time
    all_projects = [x for x in client.projects.stream()]
    # for each project get the edges and take union
    for project in projects:
        graph_edges = graph_edges.union(_build_edges(project, client, all_projects=all_projects))

    graph = nx.DiGraph()
    graph.add_edges_from(graph_edges)
    graph.add_nodes_from([p.name for p in projects])  # add nodes to ensure singletons are retained
    return graph


def get_source_nodes(graph: nx.DiGraph) -> List[str]:
    """
    Gives all source nodes in a graph

    Args:
        graph: nx.DiGraph for which to find source nodes

    Returns:
        List of node names
    """
    source_nodes = []
    for n in graph.nodes():
        if not [x for x in graph.predecessors(n)]:
            source_nodes.append(n)
    return source_nodes


def get_end_nodes(graph: nx.DiGraph):
    """Returns all end nodes in a directed graph.

    Only used for testing.

    Args:
        graph: nx.DiGraph for which to find end nodes

    Returns:
        List of names of all end nodes
    """
    end_nodes = []
    for n in graph.nodes():
        if not [x for x in graph.successors(n)]:
            end_nodes.append(n)
    return end_nodes


def get_projects_by_tier(graph: nx.DiGraph) -> Dict[int, Set[str]]:
    """
    Find the different projects at each tier

    Args:
        graph: the nx.DiGraph for which to generate the tiers

    Returns:
        A json dict who's structure is {'tier': {projects_at_that_tier}, ...}
        e.g. {1: {'SM_project_1', 'Classification_project_1'}, 2: {}Mastering_project'},
        3: {'Golden_records_project'}}
    """
    source_nodes = get_source_nodes(graph)

    path_tier_dict: DefaultDict[int, Set[str]] = defaultdict(set)

    for n in graph.nodes():
        # just add source nodes to tier 0
        if n in source_nodes:
            path_tier_dict[0].add(n)
            continue
        # since all dependent projects must be run first
        # the tier is the maximum length of all simple paths to this node
        # so get the max length from all source nodes
        tier = 0
        for s in source_nodes:
            current_paths = [x for x in nx.all_simple_paths(graph, s, n)]
            # skip source nodes that don't link to this project n
            if not current_paths:
                continue
            max_path_length = len(max(current_paths, key=len))
            if max_path_length > tier:
                tier = max_path_length

        # decrement tier since we count starting at 0
        tier = tier - 1
        path_tier_dict[tier].add(n)

    return dict(path_tier_dict)


def get_all_downstream_nodes(graph: nx.DiGraph, node: str) -> Set[str]:
    """
    Get all nodes downstream of this one (i.e. they have a path from this node to them)

    Args:
        graph: the graph to use
        node: the node to check

    Returns:
        A list of downstream node names
    """
    downstream_paths = []
    for n in graph.nodes():
        all_paths = [x for x in nx.all_simple_paths(graph, node, n)]
        if all_paths:
            for path in all_paths:
                downstream_paths.extend(path)

    # the above generates a list of things like
    # [projectA, projectB, projectA, projectC] if projectA goes to both project_B and project_C
    # so make a set, then remove the node itself
    downstream_nodes = {x for x in set(downstream_paths) if x != node}
    return downstream_nodes


def get_successors(graph: nx.DiGraph, node: str) -> Set[str]:
    """
    Get all successor nodes to the current node

    Args:
        graph: the graph to use
        node: the node to check

    Returns:
        A set of nodes that are successors to the current node
    """
    return set(x for x in graph.successors(node))


def get_predecessors(graph: nx.DiGraph, node: str) -> Set[str]:
    """
    Get all predecessor nodes to the current node

    Args:
        graph: the graph to use
        node: the node to check

    Returns:
        A set of nodes that are predecessors to the current node
    """
    return set(x for x in graph.predecessors(node))


def add_edges(graph: nx.DiGraph, new_edges: Set[Tuple[str, str]]) -> nx.DiGraph:
    """Takes an existing graph and creates a new one with the new edges.

    Only used for testing.

    Args:
        graph: the graph to start with
        new_edges: the edges to add

    Returns:
        A copy of initial graph with new edges
    """

    old_edges = set(graph.edges())
    new_edges = old_edges.union(new_edges)
    return from_edges(new_edges, set(graph.nodes()))
