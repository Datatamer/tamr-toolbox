"""Tests for creating and executing a Planner object"""

from tamr_toolbox.workflow.concurrent import Graph, Planner, PlanStatus

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
    # get final project in pipeline
    test_output_project_name = "chained_minimal_golden_records"
    test_output_project = tamr.projects.by_name(test_output_project_name)
    test_graph = Graph.from_project_list([test_output_project], tamr)
    test_planner = Planner.from_graph(test_graph, tamr_client=tamr)
    # make sure we got the right keys for the plan object created
    test_plan_keys = {x for x in test_planner.plan.keys()}
    assert test_plan_keys == {
        "minimal_categorization",
        "minimal_schema_mapping",
        "chained_minimal_schema_mapping",
        "chained_minimal_mastering",
        "chained_minimal_golden_records",
    }


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
    test_planner = Planner.execute(test_planner, tamr)
    assert PlanStatus.from_planner(test_planner) == PlanStatus.PlanStatus.SUCCEEDED
