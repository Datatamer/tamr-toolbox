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
        pass


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

    assert expected_messages == [resp["message"] for resp in notifier.sent_messages]


@mock_api()
def test_monitor_job_timeout():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    timeout_seconds = 0.0002

    notifier = BaseNotifierSkipNotImplementedError()
    with pytest.raises(TimeoutError):
        notifier.monitor_job(tamr=client, operation=op, timeout_seconds=timeout_seconds)
