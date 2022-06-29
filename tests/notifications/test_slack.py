"""Tests for tasks related to creation of Slack notifications"""

from unittest.mock import patch

from tamr_toolbox import utils
from tamr_toolbox.notifications.slack import SlackNotifier
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


@patch("slack.WebClient", autospec=True)
def test_slack_creation(mock_slack):
    notifier = SlackNotifier(token=mock_slack, channel="#test_tbox_messaging")
    mock_slack.assert_called()


@patch("slack.WebClient", autospec=True)
def test_send_message(mock_slack):
    test_message = "This is a test email."
    test_subject_line = "Test 123"

    notifier = SlackNotifier(token=mock_slack, channel="#test_tbox_messaging")
    notifier.send_message(test_message, test_subject_line)

    context = mock_slack.return_value
    context.chat_postMessage.assert_called()
    context.chat_postMessage.assert_called_with(channel=notifier.channel,
                                                text=test_message,
                                                username="Tamr Notifications Bot",
                                                icon_url="https://jdp491bprdv1ar3uk2puw37i-wpengine.netdna-ssl.com/wp-content/uploads/2020/08/Tamr-Square-Dark.png")  # noqa
    assert notifier.sent_messages == [test_message]
