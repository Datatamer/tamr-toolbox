"""Tests for tasks related to task watchers"""
from typing import Optional
from unittest.mock import patch

import pytest

from tamr_toolbox import utils
from tamr_toolbox.notifications.common import _BaseNotifier
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


class BaseNotifierSkipNotImplementedError(_BaseNotifier):
    def send_message(self, message: str, title: str, tamr_user: Optional[str] = None) -> int:
        times_called = 0
        for times_called, tamr_user in enumerate(self._parse_recipients(tamr_user)):
            self.sent_messages += [message]
            self.sent_message_recipients += [tamr_user]

        return times_called + 1


@mock_api()
def test_monitor_job_succeeded():
    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    notifier = BaseNotifierSkipNotImplementedError()
    notifier.recipients = ["test_recipient"]
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
    notifier.recipients = ["test_recipient"]
    notifier.monitor_job(
        tamr=client, operation=op, timeout=timeout_seconds, poll_interval=poll_interval
    )

    expected_messages = [
        (
            f"Host: {client.host} \n Job: {op.resource_id} \n "
            f"Description: Materialize views "
            f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
        ),
        (
            f"The job {op.resource_id}: {op.description} took longer "
            f"than {timeout_seconds} seconds to resolve."
        ),
    ]

    assert notifier.sent_messages[:2] == expected_messages


def test_message_single_recipient():
    notifier = BaseNotifierSkipNotImplementedError()
    notifier.recipients = "test_recipient"
    messages_sent = notifier.send_message("test_message", "test_title")

    assert messages_sent == 1
    assert notifier.sent_message_recipients == ["test_recipient"]
    assert notifier.sent_messages == ["test_message"]


def test_message_list_recipient():
    notifier = BaseNotifierSkipNotImplementedError()
    notifier.recipients = ["test_recipient"]
    messages_sent = notifier.send_message("test_message", "test_title")

    assert messages_sent == 1
    assert notifier.sent_message_recipients == ["test_recipient"]
    assert notifier.sent_messages == ["test_message"]


def test_message_longer_list_recipient():
    notifier = BaseNotifierSkipNotImplementedError()
    notifier.recipients = ["test_recipient", "test_recipient_2"]
    messages_sent = notifier.send_message("test_message", "test_title")

    assert messages_sent == 2
    assert notifier.sent_message_recipients == ["test_recipient", "test_recipient_2"]
    assert notifier.sent_messages == ["test_message", "test_message"]


def test_message_dict_recipient():
    notifier = BaseNotifierSkipNotImplementedError()
    notifier.recipients = {"tamr_user": "test_recipient"}
    messages_sent = notifier.send_message("test_message", "test_title")

    assert messages_sent == 1
    assert notifier.sent_message_recipients == ["test_recipient"]
    assert notifier.sent_messages == ["test_message"]


def test_message_longer_dict_recipient():
    notifier = BaseNotifierSkipNotImplementedError()
    notifier.recipients = {"tamr_user": "test_recipient", "tamr_user_2": "test_recipient_2"}
    messages_sent = notifier.send_message("test_message", "test_title")

    assert messages_sent == 2
    assert notifier.sent_message_recipients == ["test_recipient", "test_recipient_2"]
    assert notifier.sent_messages == ["test_message", "test_message"]


def test_notimplemented():
    notifier = _BaseNotifier()
    with pytest.raises(NotImplementedError):
        notifier.send_message("message", "title")
