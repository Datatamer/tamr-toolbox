from tamr_toolbox import utils
import networkx as nx

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

from tamr_toolbox.workflow.concurrent import Graph

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


# Note the pipeline under test is chained like so, with an independent mastering project included:
#
# minimimal_schema_mapping --> chained_minimal_schema_mapping ---\
#                                                                 ----> chained_minimal_mastering
#                              minimal_categorization -----------/
#
#                      minimal_mastering


@mock_api()
def test_get_upstream_projects():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_mastering"
    indep_project_name = "minimal_mastering"
    test_project = tamr.projects.by_name(test_project_name)
    indep_project = tamr.projects.by_name(indep_project_name)
    all_projects = [x for x in tamr.projects.stream()]

    # Test chained project
    upstream_projects = Graph._get_upstream_projects(test_project, all_projects=all_projects)
    assert {x.name for x in upstream_projects} == {
        "chained_minimal_schema_mapping",
        "minimal_categorization",
    }

    # Test independent project
    assert Graph._get_upstream_projects(indep_project, all_projects=all_projects) == []


@mock_api()
def test_get_edges():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_mastering"
    test_project = tamr.projects.by_name(test_project_name)
    all_projects = [x for x in tamr.projects.stream()]
    edges = Graph._build_edges(test_project, tamr, all_projects=all_projects)
    expected_edges = {
        ("chained_minimal_schema_mapping", "chained_minimal_mastering"),
        ("minimal_categorization", "chained_minimal_mastering"),
        ("minimal_schema_mapping", "chained_minimal_schema_mapping"),
    }
    assert expected_edges == edges


@mock_api()
def test_from_project_list():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get intermediate project in pipeline to ensure we are deduping links/paths correctly
    test_middle_project_name = "chained_minimal_schema_mapping"
    test_middle_project = tamr.projects.by_name(test_middle_project_name)
    # Get independent project in to ensure isolated nodes are handled correctly
    test_indep_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_name)

    # Build graph
    test_graph = Graph.from_project_list(
        [test_middle_project, test_output_project, test_indep_project], tamr
    )

    # Now make sure we have the right set of nodes
    expected_nodes = {
        "chained_minimal_mastering",
        "minimal_schema_mapping",
        "chained_minimal_schema_mapping",
        "minimal_categorization",
        "minimal_mastering",
    }
    assert expected_nodes == {x for x in test_graph.nodes()}
    # And the right number (since set dedups for us)
    assert len(expected_nodes) == len([x for x in test_graph.nodes()])

    # Make sure we have the correct set of edges
    expected_edges = {
        ("chained_minimal_schema_mapping", "chained_minimal_mastering"),
        ("minimal_categorization", "chained_minimal_mastering"),
        ("minimal_schema_mapping", "chained_minimal_schema_mapping"),
    }
    assert expected_edges == {x for x in test_graph.edges()}


@mock_api()
def test_downstream_nodes():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Test downstream nodes for a source project
    # The set of downstream_nodes is used to update the plan when a job fails to mark any project
    # that depends on this one as blocked. Start with source project and ensure both next step and
    # the one after come back.
    expected_downstream_nodes = {"chained_minimal_mastering", "chained_minimal_schema_mapping"}
    test_downstream_nodes = Graph.get_all_downstream_nodes(test_graph, "minimal_schema_mapping")
    assert expected_downstream_nodes == test_downstream_nodes

    # Test output project has no downstream nodes
    test_output_downstream_nodes = Graph.get_all_downstream_nodes(
        test_graph, "chained_minimal_mastering"
    )
    assert test_output_downstream_nodes == set()
    # Test independent project has no downstream nodes
    test_indep_downstream_nodes = Graph.get_all_downstream_nodes(test_graph, "minimal_mastering")
    assert test_indep_downstream_nodes == set()


@mock_api()
def test_successors():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Make sure that chained_minimal_schema_mapping only has one successor
    # (i.e. doesn't get confused about existence of minimal_categorization
    expected_successors = {"chained_minimal_mastering"}
    assert expected_successors == Graph.get_successors(
        test_graph, "chained_minimal_schema_mapping"
    )
    # Make sure independent project has no successors
    assert set() == Graph.get_successors(test_graph, "minimal_mastering")


@mock_api()
def test_predecessors():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Make sure chained_minimal_mastering gets both predecessors
    assert Graph.get_predecessors(test_graph, "chained_minimal_mastering") == {
        "chained_minimal_schema_mapping",
        "minimal_categorization",
    }

    # Make sure source projects don't have any predecessors
    assert Graph.get_predecessors(test_graph, "minimal_schema_mapping") == set()
    assert Graph.get_predecessors(test_graph, "minimal_mastering") == set()


@mock_api()
def test_get_projects_by_tier():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # populate expected json - note that 'chained_minimal_mastering' is at tier 3
    # even though there is a shorter path to it via 'minimal_categorization'
    # however it being at tier 3 is the graph realizing that all upstream projects
    # need to be run so it cannot actually start until tier 2 is done
    expected_tier_json = {
        0: {"minimal_schema_mapping", "minimal_categorization", "minimal_mastering"},
        1: {"chained_minimal_schema_mapping"},
        2: {"chained_minimal_mastering"},
    }
    test_tier_json = Graph.get_projects_by_tier(test_graph)
    assert expected_tier_json == test_tier_json


@mock_api()
def test_get_source_nodes():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Turn to set since order of the returned list isn't deterministic
    # but check length to make sure we aren't getting dupes
    expected_source_nodes = {
        "minimal_schema_mapping",
        "minimal_categorization",
        "minimal_mastering",
    }
    test_source_nodes = Graph.get_source_nodes(test_graph)
    assert len(test_source_nodes) == 3
    test_source_nodes = {x for x in test_source_nodes}
    assert expected_source_nodes == test_source_nodes


@mock_api()
def test_get_end_nodes():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Confirm expected end nodes
    expected_end_nodes = {"chained_minimal_mastering", "minimal_mastering"}
    assert expected_end_nodes == Graph.get_end_nodes(test_graph)


@mock_api()
def test_add_edges():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Take the original edges, remove one, and create a graph from the resulting slices
    # Then add the sliced out edge and ensure the project_tier_jsons from the graphs are the same
    original_edges = set(test_graph.edges())
    for e in [  # removing one of these edges will change the tier result -- other edge will not
        ("minimal_schema_mapping", "chained_minimal_schema_mapping"),
        ("chained_minimal_schema_mapping", "chained_minimal_mastering"),
    ]:
        test_edges = original_edges - {e}

        # Build test graph
        new_test_graph = nx.DiGraph(test_edges)
        new_test_graph.add_nodes_from(test_graph.nodes())
        # Make sure the project_tier_jsons don't match at this point
        assert Graph.get_projects_by_tier(test_graph) != Graph.get_projects_by_tier(new_test_graph)

        # Add sliced edge and make sure the project_tier_jsons now match
        new_test_graph.add_edges_from({e})
        assert Graph.get_projects_by_tier(test_graph) == Graph.get_projects_by_tier(new_test_graph)
