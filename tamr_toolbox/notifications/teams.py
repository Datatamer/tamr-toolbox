import logging
from abc import ABC
from dataclasses import dataclass

import pymsteams
from slack.errors import SlackApiError

from tamr_toolbox.notifications.core import _BaseNotifier

LOGGER = logging.getLogger(__name__)


@dataclass
class TeamsNotifier(_BaseNotifier, ABC):
    webhook_url: str
    """Send Microsoft Teams messages based on Tamr eventing.

    Args:
        webhook_url: An incoming webhook for a Teams channel. See 
        `https://docs.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook` # noqa
        for setup instructions
    """

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        message = pymsteams.connectorcard(self.webhook_url)
        message.text(message)
        message.title(title)

        LOGGER.info(f"Sending a Teams notification to webhook {self.webhook_url}")
        message.send()
