import logging
from abc import ABC
from dataclasses import dataclass
from typing import Dict, List, Union, Optional

from tamr_toolbox.notifications.common import _BaseNotifier

try:
    import pymsteams
except ImportError:
    pass

LOGGER = logging.getLogger(__name__)


@dataclass
class TeamsNotifier(_BaseNotifier, ABC):
    webhooks: Union[str, List[str], Dict[str, str]]
    """Send Microsoft Teams messages based on Tamr eventing.

    Args:
        webhooks: A single, list of, or dict connecting Tamr users to,
            incoming webhooks to message. 
            See `https://docs.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook` # noqa
            for setup instructions
    """

    def __post_init__(self):
        super().__init__()
        self.recipients = self.webhooks

    def send_message(self, message: str, title: str, tamr_user: Optional[str] = None) -> None:
        recipients = self._parse_recipients(tamr_user)

        for recipient in recipients:
            message_card = pymsteams.connectorcard(recipient)
            message_card.text(message)
            message_card.title(title)

            LOGGER.info(f"Sending a Teams message to {recipient}")
            try:
                message_card.send()
            except pymsteams.TeamsWebhookException as e:
                LOGGER.error(f"Error posting message: {e}.")
