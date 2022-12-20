import random

from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

from tamr_toolbox.workflow.concurrent import Graph

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


# Note the pipeline under test is chained like so:
#
# minimimal_schema_mapping --> chained_minimal_schema_mapping ---\
#                                                                 ----> chained_minimal_mastering
#                              minimal_categorization -----------/
#


@mock_api()
def test_get_upstream_projects():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_mastering"
    test_project = tamr.projects.by_name(test_project_name)
    all_projects = [x for x in tamr.projects.stream()]
    upstream_projects = Graph._get_upstream_projects(test_project, all_projects=all_projects)
    assert {x.name for x in upstream_projects} == {
        "chained_minimal_schema_mapping",
        "minimal_categorization",
    }


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


def test_from_edges():
    test_edges = {
        ("chained_minimal_schema_mapping", "chained_minimal_mastering"),
        ("minimal_categorization", "chained_minimal_mastering"),
        ("minimal_schema_mapping", "chained_minimal_schema_mapping"),
    }
    test_graph = Graph.from_edges(test_edges)
    # check nothing got mutated
    assert test_graph.edges == test_edges

    # check we have the right set of nodes
    expected_nodes = {
        "chained_minimal_mastering",
        "minimal_schema_mapping",
        "chained_minimal_schema_mapping",
        "minimal_categorization",
    }
    assert expected_nodes == {x for x in test_graph.directed_graph.nodes}


@mock_api()
def test_from_project_list():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # get intermediate project in pipeline to ensure we are deduping links/paths correctly
    test_middle_project_name = "chained_minimal_schema_mapping"
    test_middle_project = tamr.projects.by_name(test_middle_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_middle_project, test_output_project], tamr)

    # now make sure we have the right set of nodes
    expected_nodes = {
        "chained_minimal_mastering",
        "minimal_schema_mapping",
        "chained_minimal_schema_mapping",
        "minimal_categorization",
    }
    assert expected_nodes == {x for x in test_graph.directed_graph.nodes}
    # and the right number (since set dedups for us)
    assert len(expected_nodes) == len([x for x in test_graph.directed_graph.nodes])


@mock_api()
def test_downstream_nodes():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)

    # test downstream nodes for a source project
    # downstream_nodes is used to update the plan for when a job fails to mark any project that
    # depends on this one as blocked so start with source project and make sure both next step and
    # the one after come back
    expected_downstream_nodes = {"chained_minimal_mastering", "chained_minimal_schema_mapping"}
    test_downstream_nodes = Graph.get_all_downstream_nodes(test_graph, "minimal_schema_mapping")
    assert expected_downstream_nodes == test_downstream_nodes

    # test output project has no downstream nodes
    test_output_downstream_nodes = Graph.get_all_downstream_nodes(
        test_graph, "chained_minimal_mastering"
    )
    assert test_output_downstream_nodes == set()


@mock_api()
def test_successors():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)

    # make sure that chained_minimal_schema_mapping only has one successor
    # (i.e. doesn't get confused about existence of minimal_categorization
    expected_successors = {"chained_minimal_mastering"}
    assert expected_successors == Graph.get_successors(
        test_graph, "chained_minimal_schema_mapping"
    )


@mock_api()
def test_predecessors():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)

    # make sure chained_minimal_mastering gets both predecessors
    assert Graph.get_predecessors(test_graph, "chained_minimal_mastering") == {
        "chained_minimal_schema_mapping",
        "minimal_categorization",
    }

    # make sure source projects don't have any predecessors
    assert Graph.get_predecessors(test_graph, "minimal_schema_mapping") == set()


@mock_api()
def test_get_projects_by_tier():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)

    # populate expected json - note that 'chained_minimal_mastering' is at tier 3
    # even though there is a shorter path to it via 'minimal_categorization'
    # however it being at tier 3 is the graph realizing that all upstream projects
    # need to be run so it cannot actually start until tier 2 is done
    expected_tier_json = {
        0: {"minimal_schema_mapping", "minimal_categorization"},
        1: {"chained_minimal_schema_mapping"},
        2: {"chained_minimal_mastering"},
    }
    test_tier_json = Graph.get_projects_by_tier(test_graph)
    assert expected_tier_json == test_tier_json


@mock_api()
def test_get_source_nodes():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)
    # turn to set since order of the returned list isn't deterministic
    # but check length to make sure we aren't getting dupes
    expected_source_nodes = {"minimal_schema_mapping", "minimal_categorization"}
    test_source_nodes = Graph.get_source_nodes(test_graph)
    assert len(test_source_nodes) == 2
    test_source_nodes = {x for x in test_source_nodes}
    assert expected_source_nodes == test_source_nodes


@mock_api()
def test_get_end_nodes():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)
    expected_end_nodes = ["chained_minimal_mastering"]
    assert expected_end_nodes == Graph.get_end_nodes(test_graph)


@mock_api()
def test_add_edges():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_mastering"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # build graph
    test_graph = Graph.from_project_list([test_output_project], tamr)

    # take the original edges, remove one in the middle
    # create a graph from the resulting slices
    # then add the sliced out edge and ensure the project_tier_jsons from the graphs are the same
    original_edges = test_graph.edges
    test_edges = set(random.sample([x for x in original_edges], len(original_edges) - 1))
    sliced_edge = {x for x in original_edges if x not in test_edges}

    # build test graph
    new_test_graph = Graph.from_edges(test_edges)
    # make sure the project_tier_jsons don't match at this point
    assert Graph.get_projects_by_tier(test_graph) != Graph.get_projects_by_tier(new_test_graph)

    # add sliced edge and make sure the project_tier_jsons now match
    new_test_graph = Graph.add_edges(new_test_graph, sliced_edge)
    assert Graph.get_projects_by_tier(test_graph) == Graph.get_projects_by_tier(new_test_graph)
