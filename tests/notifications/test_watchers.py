"""Tests for tasks related to task watchers"""
from unittest.mock import Mock

import pytest

from tamr_toolbox import utils
from tamr_toolbox.notifications.common import _BaseNotifier
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


@mock_api()
def test_monitor_job_succeeded():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    notifier = _BaseNotifier()
    notifier.recipients = ["test_recipient"]

    notifier.send_message = Mock()

    notifier.monitor_job(tamr=client, operation=op)

    expected = [
        {
            "message": (
                f"Host: {client.host} \n Job: {op.resource_id} \n "
                f"Description: Materialize views "
                f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
            ),
            "title": f"Job {op.resource_id}: OperationState.PENDING",
        },
        {
            "message": (
                f"Host: {client.host} \n Job: {op.resource_id} \n "
                f"Description: Materialize views "
                f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: RUNNING "
            ),
            "title": f"Job {op.resource_id}: OperationState.RUNNING",
        },
        {
            "message": (
                f"Host: {client.host} \n Job: {op.resource_id} \n "
                f"Description: Materialize views "
                f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: SUCCEEDED "
            ),
            "title": f"Job {op.resource_id}: OperationState.SUCCEEDED",
        },
    ]

    assert notifier.send_message.call_count == 3
    for i in range(3):
        assert notifier.send_message.call_args_list[i] == [expected[i]]


@mock_api()
def test_monitor_job_timeout():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    timeout_seconds = 0.2
    poll_interval = 0.1

    notifier = _BaseNotifier()
    notifier.recipients = ["test_recipient"]
    notifier.send_message = Mock()

    notifier.monitor_job(
        tamr=client, operation=op, timeout=timeout_seconds, poll_interval=poll_interval
    )

    expected = [
        {
            "message": (
                f"Host: {client.host} \n Job: {op.resource_id} \n "
                f"Description: Materialize views "
                f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
            ),
            "title": f"Job {op.resource_id}: OperationState.PENDING",
        },
        {
            "message": (
                f"The job {op.resource_id}: {op.description} took longer "
                f"than {timeout_seconds} seconds to resolve."
            ),
            "title": f"Job {op.resource_id}: Timeout",
        }
    ]

    for i in range(2):
        assert notifier.send_message.call_args_list[i] == [expected[i]]


def test_parse_single_recipients():
    notifier = _BaseNotifier()
    notifier.recipients = "test_recipient"

    recipients = notifier._parse_recipients()

    assert recipients == ["test_recipient"]


def test_parse_list_recipient():
    notifier = _BaseNotifier()
    notifier.recipients = ["test_recipient_1", "test_recipient_2"]

    recipients = notifier._parse_recipients()

    assert recipients == ["test_recipient_1", "test_recipient_2"]


def test_parse_dict_recipient():
    notifier = _BaseNotifier()
    notifier.recipients = {"tamr_user": "test_recipient", "tamr_user2": "test_recipient_2"}

    recipients = notifier._parse_recipients()

    assert recipients == ["test_recipient", "test_recipient_2"]


def test_parse_longer_dict_recipient():
    notifier = _BaseNotifier()
    notifier.recipients = {"tamr_user": "test_recipient", "tamr_user_2": "test_recipient_2"}

    recipients = notifier._parse_recipients("tamr_user")

    assert recipients == ["test_recipient"]


def test_parse_user_without_lookup_dict_raises_error():
    notifier = _BaseNotifier()
    notifier.recipients = ["test_recipient_1", "test_recipient_2"]

    with pytest.raises(ValueError):
        notifier._parse_recipients("tamr_user")


def test_call_direct_notimplemented():
    notifier = _BaseNotifier()
    with pytest.raises(NotImplementedError):
        notifier.send_message("message", "title")
