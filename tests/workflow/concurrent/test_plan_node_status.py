"""Tests for updating PlanNodeStatus"""
from tamr_unify_client.operation import Operation

from tamr_toolbox.workflow.concurrent import PlanNodeStatus, PlanNode
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox import utils
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_from_op():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    # submit job
    op = project.unified_dataset().refresh(asynchronous=True)
    # immediately check the status should give 'running'
    assert PlanNodeStatus.from_tamr_op(op) == PlanNodeStatus.PlanNodeStatus.RUNNING
    # now wait for it to succeed and ensure we get success back
    op = op.wait()
    assert PlanNodeStatus.from_tamr_op(op) == PlanNodeStatus.PlanNodeStatus.SUCCEEDED


@mock_api()
def test_from_op_failure():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    op_json = {
        "id": "-1",
        "type": "NOOP",
        "description": "test",
        "status": {"state": "FAILED", "startTime": "early", "endTime": "late", "message": ""},
        "created": {"username": "", "time": "early", "version": "-1"},
        "lastModified": {"username": "", "time": "late", "version": "-1"},
        "relativeId": "operations/-1",
    }
    op = Operation.from_json(client, op_json)
    assert PlanNodeStatus.from_tamr_op(op) == PlanNodeStatus.PlanNodeStatus.FAILED


@mock_api()
def test_from_plan_node():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    test_plan_node = PlanNode.PlanNode(
        name=project.name, project=project, priority=0, current_op=None, operations=None
    )
    # check status - should be planned
    assert PlanNodeStatus.from_plan_node(test_plan_node) == PlanNodeStatus.PlanNodeStatus.PLANNED

    # now run the node - in this case it is just update UD and check that it is update to running
    test_plan_node = PlanNode.run_next_step(test_plan_node)
    assert PlanNodeStatus.from_plan_node(test_plan_node) == PlanNodeStatus.PlanNodeStatus.RUNNING

    # wait for the job to complete then make sure we get success
    test_op = test_plan_node.current_op
    test_op.wait()
    assert PlanNodeStatus.from_plan_node(test_plan_node) == PlanNodeStatus.PlanNodeStatus.SUCCEEDED
