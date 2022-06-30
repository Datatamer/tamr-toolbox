import logging
import warnings
from abc import ABC
from dataclasses import dataclass
from typing import Dict
from typing import Union, List, Optional

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.notifications.common import _monitor_job as monitor_job_common, _BaseNotifier
from tamr_toolbox.utils.operation import get_details

try:
    import slack
    from slack.errors import SlackApiError
except ImportError:
    pass

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
        self.slack = slack.WebClient(
            token=self.token, base_url=self.base_url, timeout=self.timeout, proxy=self.proxies
        )
        LOGGER.info("Successfully authenticated with the Slack API")

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        LOGGER.info(f"Sending a slack message to {self.channel}")
        try:
            self.slack.chat_postMessage(
                channel=self.channel,
                text=message,
                username="Tamr Notifications Bot",
                icon_url="https://jdp491bprdv1ar3uk2puw37i-wpengine.netdna-ssl.com/wp-content/uploads/2020/08/Tamr-Square-Dark.png",  # noqa
            )
        except SlackApiError as e:
            LOGGER.error(f"Error posting message: {e}.")

        self.sent_messages += [message]


def send_message(
    *,
    slack_client: "slack.WebClient",
    channel: str,
    message: str,
    raise_error: bool = True,
    **kwargs,
) -> JsonDict:
    """Sends a message to a pre-defined Slack channel

    Args:
        slack_client: A Slack WebClient
        channel: A (public) Slack channel to receive messages
        message: A message to be sent to a Slack channel
        raise_error: A boolean value to opt out raising Slack API errors

    Returns:
        The data property of the Slack WebClient object

    Raises:
        SlackApiError: if the SlackClient does not post the message correctly
            and `raise_error` is set to True
    """

    # This function requires slack, an optional dependency
    from slack.errors import SlackApiError

    warnings.warn(
        "'This function will be deprecated in a future release. "
        "Use SlackNotifier().send_message() instead.'",
        DeprecationWarning,
    )
    try:
        slack_response = slack_client.chat_postMessage(channel=channel, text=message)
    except SlackApiError as e:
        LOGGER.error(f"Error: {e}")

        if not raise_error:
            return {
                "type": "SlackError",
                "text": f'The message: "{message}" failed to send to channel: {channel}',
                "details": e.response["error"],
            }
        else:
            raise e

    else:
        return slack_response.data


def _send_job_status_message(
    *,
    slack_client: "slack.WebClient",
    channel: str,
    operation: Operation,
    notify_states: List[OperationState],
) -> JsonDict:
    """Checks operation state and if in `notify_states` sends the message.

    Args:
        slack_client: A Slack WebClient
        channel: A (public) Slack channel to receive messages
        operation: A Tamr Operation
        notify_states: States for which notifications should be sent

    Returns:
        The data property of the Slack WebClient object
    """
    warnings.warn(
        "'This function will be deprecated in a future release. "
        "Use slack.SlackNotifier().monitor_job() instead.'",
        DeprecationWarning,
    )
    state = OperationState[operation.state]
    if state in notify_states:
        message = get_details(operation=operation)
        return send_message(slack_client=slack_client, channel=channel, message=message)


def monitor_job(
    tamr: Client,
    *,
    slack_client: "slack.WebClient",
    channel: str,
    operation: Union[int, str, Operation],
    poll_interval_seconds: float = 1,
    timeout_seconds: Optional[float] = None,
    notify_states: Optional[List[OperationState]] = None,
) -> List[JsonDict]:
    """Monitors a Tamr Operation and sends a Slack message to a channel when the job status is updated

    Args:
        tamr: A Tamr client
        slack_client: A Slack WebClient
        channel: A (public) Slack channel to receive messages
        operation: A job ID or a Tamr operation
        poll_interval_seconds: Time interval (in seconds) between subsequent polls
        timeout_seconds: Time (in seconds) to wait
        notify_states : States for which notifications should be sent, use None for all states

    Returns:
        A list of messages with their config sent via the Slack WebClient object
    """

    warnings.warn(
        "'This function will be deprecated in a future release. "
        "Use SlackNotifier().monitor_job() instead.'",
        DeprecationWarning,
    )
    list_responses = monitor_job_common(
        tamr=tamr,
        send_message=send_message,
        send_status_function=_send_job_status_message,
        slack_client=slack_client,
        channel=channel,
        operation=operation,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
        notify_states=notify_states,
    )

    return list_responses
