import logging
from abc import ABC
from dataclasses import dataclass

import pymsteams

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

    def __post_init__(self):
        super().__init__()

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        message_card = pymsteams.connectorcard(self.webhook_url)
        message_card.text(message)
        message_card.title(title)

        LOGGER.info(f"Sending a Teams notification to webhook {self.webhook_url}")
        message_card.send()

        self.sent_messages += [message]
