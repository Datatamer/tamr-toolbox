"""Tests for tasks related to Tamr operations (or jobs)"""
from tamr_toolbox import utils
from tamr_toolbox.utils.operation import (
    from_resource_id,
    get_latest,
    get_details,
    get_all,
    get_active,
    monitor,
)
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox.models.operation_state import OperationState
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/utils.config.yaml"
)


@mock_api()
def test_from_resource_id():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)
    op_from_resource_id = from_resource_id(client, job_id=op.resource_id)
    assert op.resource_id == op_from_resource_id.resource_id


@mock_api()
def test_get_latest():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)
    op_get_latest = get_latest(client)
    assert op.resource_id == op_get_latest.resource_id


@mock_api()
def test_get_details():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)
    op_details = get_details(operation=op)
    assert (
        f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize views ["
        f"minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING " == op_details
    )


@mock_api()
def test_get_all():
    client = utils.client.create(**CONFIG["my_instance_name"])
    op_get_all = get_all(client)
    assert len(op_get_all) == 814


@mock_api()
def test_get_active():
    client = utils.client.create(**CONFIG["my_instance_name"])
    schema_mapping_project = client.projects.by_resource_id(
        CONFIG["projects"]["minimal_schema_mapping"]
    )
    op_schema_mapping = schema_mapping_project.unified_dataset().refresh(asynchronous=True)
    mastering_project = client.projects.by_resource_id(CONFIG["projects"]["minimal_mastering"])
    op_mastering = mastering_project.unified_dataset().refresh(asynchronous=True)
    op_get_active = get_active(client)

    op_ids = [op_schema_mapping.relative_id, op_mastering.relative_id]
    expected_op_ids = [op.relative_id for op in op_get_active]
    assert all(op_id in expected_op_ids for op_id in op_ids)

    op_statuses = [OperationState(op.state) for op in op_get_active]
    expected_op_statues = [OperationState.PENDING, OperationState.RUNNING]
    assert all(op_status in expected_op_statues for op_status in op_statuses)


@mock_api()
def test_monitor():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_mastering"])
    project = project.as_mastering()
    op = project.pairs().refresh(asynchronous=True)
    status = OperationState[op.state]
    assert status == OperationState.PENDING

    op_running = monitor(op, poll_interval_seconds=1)

    running_status = OperationState[op_running.state]
    assert op.resource_id == op_running.resource_id
    assert running_status == OperationState.RUNNING

    op_succeeded = monitor(op_running, poll_interval_seconds=1)

    succeeded_status = OperationState[op_succeeded.state]
    assert op.resource_id == op_succeeded.resource_id
    assert succeeded_status == OperationState.SUCCEEDED
