import logging
from abc import ABC
from dataclasses import dataclass
from typing import Optional, Dict, Union

import slack
from slack.errors import SlackApiError

from tamr_toolbox.notifications.core import _BaseNotifier

LOGGER = logging.getLogger(__name__)


@dataclass
class SlackNotifier(_BaseNotifier, ABC):
    channel: str
    token: str
    proxies: Optional[Union[None, Dict[str, str]]] = None
    base_url: Optional[str] = "https://www.slack.com/api/"
    timeout: Optional[int] = 30
    """Send slack messages based on Tamr eventing.

    Args:
        channel: The (public) Slack channel to send notifications to
        token: An xoxp or xoxb access token.
        proxies: A dict of proxy configs if needed. Default is None.
        base_url: The base URL of the Slack API. Default is 'https://www.slack.com/api/'
        timeout: The maximum number of seconds the client will wait
            to connect and receive a response from Slack. Default is 30 seconds.
    """

    def __post_init__(self):
        super().__init__()
        LOGGER.info("Connecting with the Slack API")
        self.slack = slack.WebClient(token=self.token, base_url=self.base_url,
                                     timeout=self.timeout, proxy=self.proxies)
        LOGGER.info("Successfully authenticated with the Slack API")

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        LOGGER.info(f"Sending a slack message to {self.channel}")
        try:
            self.slack.chat_postMessage(channel=self.channel, text=message,
                                        username="Tamr Notifications Bot",
                                        icon_url="https://jdp491bprdv1ar3uk2puw37i-wpengine.netdna-ssl.com/wp-content/uploads/2020/08/Tamr-Square-Dark.png")    # noqa
        except SlackApiError as e:
            LOGGER.error(f"Error posting message: {e}.")

        self.sent_messages += [message]
