"""Tests for creating and executing a Planner object"""

from tamr_toolbox.workflow.concurrent import Graph, Planner, PlanStatus, PlanNodeStatus

from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox import utils
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


# Note the pipeline under test is chained like so:
#
# minimimal_schema_mapping --> chained_minimal_SM ---\
#                                                     ---> chained_minimal_mastering --> chained_GR
#                  minimal_categorization -----------/
#


@mock_api()
def test_from_graph():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_golden_records"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)

    # Build plan
    test_planner = Planner.from_graph(test_graph, tamr_client=tamr)
    # make sure we got the right keys for the plan object created
    test_plan_keys = {x for x in test_planner.plan.keys()}
    assert test_plan_keys == {
        "minimal_mastering",
        "minimal_categorization",
        "minimal_schema_mapping",
        "chained_minimal_schema_mapping",
        "chained_minimal_mastering",
        "chained_minimal_golden_records",
    }


@mock_api()
def test_plan_indep_project():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph
    test_graph = Graph.from_project_list([test_indep_project], tamr)

    # Build plan
    test_planner = Planner.from_graph(test_graph, tamr_client=tamr)
    # Make sure we got the right keys for the plan object created
    assert {"minimal_mastering"} == {x for x in test_planner.plan.keys()}
    assert test_planner.plan["minimal_mastering"].status == PlanNodeStatus.PlanNodeStatus.RUNNABLE


@mock_api(asynchronous=True)
def test_update_plan_for_failed():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Get final project in pipeline
    test_output_project_name = "chained_minimal_golden_records"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    # Get independent project
    test_indep_project_name = "minimal_mastering"
    test_indep_project = tamr.projects.by_name(test_indep_project_name)
    # Build graph and planner
    test_graph = Graph.from_project_list([test_output_project, test_indep_project], tamr)
    test_planner = Planner.from_graph(test_graph, tamr_client=tamr)

    # now grab the plan node for minimal categorization, set its status to failed
    # and update the plan to ensure mastering/GR are blocked but the
    # schema mapping nodes are not
    categorization_node = test_planner.plan["minimal_categorization"]
    categorization_node.status = PlanNodeStatus.PlanNodeStatus.FAILED

    test_planner = Planner.update_plan(test_planner, plan_node=categorization_node)
    mastering_node = test_planner.plan["chained_minimal_mastering"]
    schema_mapping_node = test_planner.plan["minimal_schema_mapping"]
    chained_schema_mapping_node = test_planner.plan["chained_minimal_schema_mapping"]
    golden_records_node = test_planner.plan["chained_minimal_golden_records"]
    categorization_node = test_planner.plan["minimal_categorization"]
    indep_node = test_planner.plan["minimal_mastering"]

    # make sure the node itself is updated
    assert categorization_node.status == PlanNodeStatus.PlanNodeStatus.FAILED
    # now make sure knock-on effects are handled correctly
    assert mastering_node.status == PlanNodeStatus.PlanNodeStatus.BLOCKED
    assert golden_records_node.status == PlanNodeStatus.PlanNodeStatus.BLOCKED
    # and finally that unaffected nodes are really unaffected the first node should be runnable
    assert schema_mapping_node.status == PlanNodeStatus.PlanNodeStatus.RUNNABLE
    assert indep_node.status == PlanNodeStatus.PlanNodeStatus.RUNNABLE
    # the chained one should be planned
    assert chained_schema_mapping_node.status == PlanNodeStatus.PlanNodeStatus.PLANNED


@mock_api(asynchronous=True)
def test_update_plan_for_succeeded():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_golden_records"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    test_graph = Graph.from_project_list([test_output_project], tamr)
    test_planner = Planner.from_graph(test_graph, tamr_client=tamr)

    # now grab the plan node for minimal schema mapping, set its status to successful
    # and update the plan to ensure that the next node can run but other nodes don't change
    categorization_node = test_planner.plan["minimal_schema_mapping"]
    categorization_node.status = PlanNodeStatus.PlanNodeStatus.SUCCEEDED

    # before updating make sure the schema mapping node is PLANNED (i.e. not RUNNABLE)
    assert (
        test_planner.plan["chained_minimal_schema_mapping"].status
        == PlanNodeStatus.PlanNodeStatus.PLANNED
    )

    test_planner = Planner.update_plan(test_planner, plan_node=categorization_node)
    mastering_node = test_planner.plan["chained_minimal_mastering"]
    schema_mapping_node = test_planner.plan["minimal_schema_mapping"]
    chained_schema_mapping_node = test_planner.plan["chained_minimal_schema_mapping"]
    golden_records_node = test_planner.plan["chained_minimal_golden_records"]
    categorization_node = test_planner.plan["minimal_categorization"]

    # make sure the node itself is updated
    assert schema_mapping_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED
    # now make sure no farther downstream nodes are affected
    assert mastering_node.status == PlanNodeStatus.PlanNodeStatus.PLANNED
    assert golden_records_node.status == PlanNodeStatus.PlanNodeStatus.PLANNED
    # and finally that unaffected nodes are really unaffected. categorization should be runnable
    assert categorization_node.status == PlanNodeStatus.PlanNodeStatus.RUNNABLE
    # the chained schema mapping node should now also be runnable
    assert chained_schema_mapping_node.status == PlanNodeStatus.PlanNodeStatus.RUNNABLE


@mock_api(asynchronous=True)
def test_execute():
    """
    This is basically *the* test - try executing a pipeline.
    I haven't figured out a good way to any testing of what is going on
    in the guts other than testing
    the individual pieces (e.g. in the tests for PlanNode).
    """
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # get final project in pipeline
    test_output_project_name = "chained_minimal_golden_records"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    test_graph = Graph.from_project_list([test_output_project], tamr)
    test_planner = Planner.from_graph(test_graph, tamr_client=tamr)
    test_planner = Planner.execute(test_planner, tamr, polling_interval=1)
    assert PlanStatus.from_planner(test_planner) == PlanStatus.PlanStatus.SUCCEEDED
