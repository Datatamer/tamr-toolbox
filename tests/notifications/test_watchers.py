"""Tests for tasks related to task watchers"""
import pytest

from tamr_toolbox import utils
from tamr_toolbox.notifications.core import _BaseNotifier
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


class BaseNotifierSkipNotImplementedError(_BaseNotifier):
    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        self.sent_messages += [message]


@mock_api()
def test_monitor_job_succeeded():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    notifier = BaseNotifierSkipNotImplementedError()
    notifier.monitor_job(tamr=client, operation=op)

    expected_messages = [
        (
            f"Host: {client.host} \n Job: {op.resource_id} \n "
            f"Description: Materialize views "
            f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
        ),
        (
            f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize "
            f"views [minimal_schema_mapping_unified_dataset] to Elastic \n "
            f"Status: RUNNING "
        ),
        (
            f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize "
            f"views [minimal_schema_mapping_unified_dataset] to Elastic \n "
            f"Status: SUCCEEDED "
        ),
    ]

    assert notifier.sent_messages == expected_messages


@mock_api()
def test_monitor_job_timeout():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    timeout_seconds = 0.2
    poll_interval = 0.1

    notifier = BaseNotifierSkipNotImplementedError()
    notifier.monitor_job(tamr=client, operation=op, timeout=timeout_seconds, poll_interval=poll_interval)

    expected_messages = [
        (
            f"Host: {client.host} \n Job: {op.resource_id} \n "
            f"Description: Materialize views "
            f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
        ),
        (
            f"The job {op.resource_id}: {op.description} took longer "
            f"than {timeout_seconds} seconds to resolve."
        )
    ]

    assert notifier.sent_messages[:2] == expected_messages


def test_notimplemented():
    notifier = _BaseNotifier()
    with pytest.raises(NotImplementedError):
        notifier.send_message("message", "title")