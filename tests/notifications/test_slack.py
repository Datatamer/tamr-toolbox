"""Tests for tasks related to creation of Slack notifications"""

from unittest.mock import patch, MagicMock

import pytest
from requests import HTTPError
from slack import WebClient
from slack.errors import SlackApiError
from slack.web.slack_response import SlackResponse

from tamr_toolbox import utils, notifications
from tamr_toolbox.notifications.slack import SlackNotifier
from tamr_toolbox.utils.operation import get_details, from_resource_id
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


@patch("slack.WebClient", autospec=True)
def test_slack_creation(mock_slack):
    _ = SlackNotifier(token=mock_slack, channels="#test_tbox_messaging")
    mock_slack.assert_called()


@patch("slack.WebClient", autospec=True)
def test_send_message(mock_slack):
    test_message = "This is a test email."
    test_subject_line = "Test 123"

    notifier = SlackNotifier(token=mock_slack, channels="#test_tbox_messaging")
    notifier.send_message(test_message, test_subject_line)

    context = mock_slack.return_value
    context.chat_postMessage.assert_called()
    context.chat_postMessage.assert_called_with(
        channel=notifier.recipients,
        text=test_message,
        username="Tamr Notifications Bot",
        icon_url="https://jdp491bprdv1ar3uk2puw37i-wpengine.netdna-ssl.com/wp-content/uploads/2020/08/Tamr-Square-Dark.png",  # noqa
    )
    assert notifier.sent_messages == [test_message]


def _deprecated_mock_response(channel: str, text: str) -> SlackResponse:
    """
    Simulate response for a call to chat_PostMessage() in the Slack WebClient
    https://python-slackclient.readthedocs.io/en/latest/basic_usage.html

    Args:
        channel: The slack channel to post to (ignored in mock response)
        text: The body of the message in the response
    Returns:
        A JSON SlackResponse object
    """
    mock_client = WebClient()
    response = {
        "bot_id": "BOT_ID",
        "type": "message",
        "text": text,
        "user": "USER",
        "team": "TEAM",
        "bot_profile": {
            "id": "BOT_ID",
            "deleted": False,
            "name": "Tamr Jobs Watch",
            "updated": 1593130492,
            "app_id": "APP_ID",
            "icons": {
                "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
            },
            "team_id": "TEAM",
        },
    }
    response_data = {"message": response}
    mock_response = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="http://localhost:3000/api.test",
        req_args={},
        data=response_data,
        headers={},
        status_code=200,
    )
    return mock_response


@mock_api()
def test_deprecated_monitor_job_succeed():
    mock_client = WebClient()
    mock_client.chat_postMessage = MagicMock(side_effect=_deprecated_mock_response)

    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    list_responses = notifications.slack.monitor_job(
        tamr=client,
        slack_client=mock_client,
        channel="#test_tbox_messaging",
        operation=op,
        poll_interval_seconds=0.01,
    )

    saved_responses = [
        {
            "bot_id": "BOT_ID",
            "type": "message",
            "text": f"Host: {client.host} \n Job: {op.resource_id} \n "
            f"Description: Materialize views"
            f" [minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING ",
            "user": "USER",
            "team": "TEAM",
            "bot_profile": {
                "id": "BOT_ID",
                "deleted": False,
                "name": "Tamr Jobs Watch",
                "updated": 1593130492,
                "app_id": "APP_ID",
                "icons": {
                    "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                    "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                    "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
                },
                "team_id": "TEAM",
            },
        },
        {
            "bot_id": "BOT_ID",
            "type": "message",
            "text": f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize "
            f"views [minimal_schema_mapping_unified_dataset] to Elastic \n "
            f"Status: RUNNING ",
            "user": "USER",
            "team": "TEAM",
            "bot_profile": {
                "id": "BOT_ID",
                "deleted": False,
                "name": "Tamr Jobs Watch",
                "updated": 1593130492,
                "app_id": "APP_ID",
                "icons": {
                    "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                    "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                    "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
                },
                "team_id": "TEAM",
            },
        },
        {
            "bot_id": "BOT_ID",
            "type": "message",
            "text": f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize "
            f"views [minimal_schema_mapping_unified_dataset] to Elastic \n "
            f"Status: SUCCEEDED ",
            "user": "USER",
            "team": "TEAM",
            "bot_profile": {
                "id": "BOT_ID",
                "deleted": False,
                "name": "Tamr Jobs Watch",
                "updated": 1593130492,
                "app_id": "APP_ID",
                "icons": {
                    "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                    "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                    "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
                },
                "team_id": "TEAM",
            },
        },
    ]
    assert saved_responses == [response["message"] for response in list_responses]


@pytest.mark.parametrize("state, op_id", [("CANCELED", "782"), ("FAILED", "818")])
@mock_api()
def test_deprecated_monitor_job_canceled_or_failed(state: str, op_id: str):
    mock_client = WebClient()
    mock_client.chat_postMessage = MagicMock(side_effect=_deprecated_mock_response)

    client = utils.client.create(**CONFIG["my_instance_name"])
    op = from_resource_id(client, job_id=op_id)

    assert op.state == state

    list_responses = notifications.slack.monitor_job(
        tamr=client,
        slack_client=mock_client,
        channel="#test_tbox_messaging",
        operation=op,
        poll_interval_seconds=0.01,
    )

    saved_responses = [
        {
            "bot_id": "BOT_ID",
            "type": "message",
            "text": get_details(operation=op),
            "user": "USER",
            "team": "TEAM",
            "bot_profile": {
                "id": "BOT_ID",
                "deleted": False,
                "name": "Tamr Jobs Watch",
                "updated": 1593130492,
                "app_id": "APP_ID",
                "icons": {
                    "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                    "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                    "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
                },
                "team_id": "TEAM",
            },
        }
    ]
    assert saved_responses == [response["message"] for response in list_responses]


@mock_api()
def test_deprecated_monitor_job_timeout():
    mock_client = WebClient()
    mock_client.chat_postMessage = MagicMock(side_effect=_deprecated_mock_response)

    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    timeout_seconds = 0.2
    list_responses = notifications.slack.monitor_job(
        tamr=client,
        slack_client=mock_client,
        channel="#test_tbox_messaging",
        operation=op,
        poll_interval_seconds=0.1,
        timeout_seconds=timeout_seconds,
    )
    saved_responses = [
        {
            "bot_id": "BOT_ID",
            "type": "message",
            "text": f"Host: {client.host} \n Job: {op.resource_id} \n "
            f"Description: Materialize views [minimal_schema_mapping_unified_dataset] "
            f"to Elastic \n Status: PENDING ",
            "user": "USER",
            "team": "TEAM",
            "bot_profile": {
                "id": "BOT_ID",
                "deleted": False,
                "name": "Tamr Jobs Watch",
                "updated": 1593130492,
                "app_id": "APP_ID",
                "icons": {
                    "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                    "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                    "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
                },
                "team_id": "TEAM",
            },
        },
        {
            "bot_id": "BOT_ID",
            "type": "message",
            "text": f"The job {op.resource_id}: Materialize views [minimal_schema_mapping_"
            f"unified_dataset] to Elastic "
            f"took longer than {timeout_seconds} seconds to resolve.",
            "user": "USER",
            "team": "TEAM",
            "bot_profile": {
                "id": "BOT_ID",
                "deleted": False,
                "name": "Tamr Jobs Watch",
                "updated": 1593130492,
                "app_id": "APP_ID",
                "icons": {
                    "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                    "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                    "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
                },
                "team_id": "TEAM",
            },
        },
    ]
    assert [response["message"] for response in list_responses] == saved_responses


@mock_api()
def test_deprecated_monitor_job_error():
    mock_client = WebClient()
    mock_client.chat_postMessage = MagicMock(side_effect=SlackApiError(message="", response={}))

    client = utils.client.create(**CONFIG["my_instance_name"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    op = project.unified_dataset().refresh(asynchronous=True)

    with pytest.raises(HTTPError):
        notifications.slack.monitor_job(
            tamr=client,
            slack_client=mock_client,
            channel="#test_tbox_messaging",
            operation=0,
            poll_interval_seconds=0.01,
        )

    with pytest.raises(SlackApiError):
        notifications.slack.monitor_job(
            tamr=client,
            slack_client=mock_client,
            channel="#fake_channel_name",
            operation=op,
            poll_interval_seconds=0.01,
        )


@mock_api()
def test_deprecated_send_message():
    mock_client = WebClient()
    mock_client.chat_postMessage = MagicMock(side_effect=_deprecated_mock_response)

    response = notifications.slack.send_message(
        slack_client=mock_client, channel="#test_tbox_messaging", message="This is a test message."
    )

    saved_response = {
        "bot_id": "BOT_ID",
        "type": "message",
        "text": "This is a test message.",
        "user": "USER",
        "team": "TEAM",
        "bot_profile": {
            "id": "BOT_ID",
            "deleted": False,
            "name": "Tamr Jobs Watch",
            "updated": 1593130492,
            "app_id": "APP_ID",
            "icons": {
                "image_36": "https://a.slack-edge.com/80588/img/plugins/app/bot_36.png",
                "image_48": "https://a.slack-edge.com/80588/img/plugins/app/bot_48.png",
                "image_72": "https://a.slack-edge.com/80588/img/plugins/app/service_72.png",
            },
            "team_id": "TEAM",
        },
    }
    assert response["message"] == saved_response


@mock_api()
def test_deprecated_send_message_raise_error_true():
    mock_client = WebClient()
    mock_client.chat_postMessage = MagicMock(side_effect=SlackApiError(message="", response={}))

    with pytest.raises(SlackApiError):
        notifications.slack.send_message(
            slack_client=mock_client,
            channel="#fake_channel_name",
            message="This is a test message.",
        )


@mock_api()
def test_deprecated_send_message_raise_error_false():
    channel = "#fake_channel_name"
    message = "This is a test message."
    response_data = {
        "type": "SlackError",
        "text": f'The message: "{message}" failed to send to channel: {channel}',
        "details": "channel_not_found",
    }
    mock_client = WebClient()
    mock_response = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="http://localhost:3000/api.test",
        req_args={},
        data=response_data,
        headers={},
        status_code=200,
    )
    mock_client.chat_postMessage = MagicMock(return_value=mock_response)
    response = notifications.slack.send_message(
        slack_client=mock_client, channel=channel, message=message, raise_error=False
    )
    assert response == {
        "type": "SlackError",
        "text": f'The message: "{message}" failed to send to channel: {channel}',
        "details": "channel_not_found",
    }
