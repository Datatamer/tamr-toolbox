"""Tests for tasks related to creation of MS Teams notifications"""

from unittest.mock import patch

from tamr_toolbox import utils
from tamr_toolbox.notifications.teams import TeamsNotifier
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


@patch("pymsteams.connectorcard", autospec=True)
def test_send_message(mock_teams):
    test_message = "This is a test email."
    test_subject_line = "Test 123"

    notifier = TeamsNotifier(webhooks="test_webhook")
    notifier.send_message(test_message, test_subject_line)

    context = mock_teams.return_value
    assert context.text.call_args[0][0] == test_message
    assert context.title.call_args[0][0] == test_subject_line
    context.send.assert_called_once()

    assert notifier.sent_messages == [test_message]
