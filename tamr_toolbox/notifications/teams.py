"""Tasks related to creation of MS Teams notifications"""
import logging
import os
from typing import Union, List, Optional
from tamr_toolbox.models.data_type import JsonDict

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation
from tamr_toolbox.notifications.common import _monitor_job as monitor_job_common

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import get_details

LOGGER = logging.getLogger(__name__)

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    import pymsteams


def send_message(
    *,
    teams_connector_card: "pymsteams.connectorcard",
    message: str,
    raise_error: bool = True,
    **kwargs,
) -> JsonDict:
    """Sends a message to a pre-defined Slack channel

    Args:
        teams_connector_card: A pymsteams connectorcard, instantiated with
            a Teams webhook
        message: A message to be sent to an MS Teams channel
        raise_error: Optional. If True, errors should be raised, if False
            fail silenty. Default True

    Returns:
        The data of the Teams request

    See Also:
        https://docs.microsoft.com/en-us/microsoftteams/ \
        platform/webhooks-and-connectors/how-to/add-incoming-webhook

    Raises:
        TeamsWebhookException: If the message does not send properly, and
            `raise_error' is True.
    """

    # This function requires slack, an optional dependency
    import pymsteams

    try:
        teams_connector_card.text(message)
        teams_connector_card.send()
    except pymsteams.TeamsWebhookException as e:
        LOGGER.error(f"Error: {e}")

        if not raise_error:
            return {
                "type": "TeamsError",
                "text": f'The message: "{message}" failed to '
                        f'send to webhook: {teams_connector_card.hookurl}',
                "details": e,
            }
        else:
            raise e

    else:
        return teams_connector_card.payload


def _send_job_status_message(
    *,
    teams_connector_card: "pymsteams.connectorcard",
    operation: Operation,
    notify_states: List[OperationState],
) -> JsonDict:
    """Checks operation state and if in `notify_states` sends the message.

    Args:
        teams_connector_card: A pymsteams connectorcard, instantiated with
            a Teams webhook
        operation: A Tamr operation
        notify_states: States for which notifications should be sent

    See Also:
        https://docs.microsoft.com/en-us/microsoftteams/ \
        platform/webhooks-and-connectors/how-to/add-incoming-webhook

    Returns:
        The data of the Teams request
    """
    state = OperationState[operation.state]
    if state in notify_states:
        message = get_details(operation=operation)
        return send_message(teams_connector_card=teams_connector_card, message=message)


def monitor_job(
    tamr: Client,
    *,
    teams_connector_card: "pymsteams.connectorcard",
    operation: Union[int, str, Operation],
    poll_interval_seconds: float = 1,
    timeout_seconds: Optional[float] = None,
    notify_states: Optional[List[OperationState]] = None,
) -> List[JsonDict]:
    """Monitors a Tamr Operation and sends a Slack message to a channel when the job status is updated

    Args:
        tamr: A Tamr client
        teams_connector_card: A pymsteams connectorcard, instantiated with
            a Teams webhook
        operation: A job ID or a Tamr operation
        poll_interval_seconds: Time interval (in seconds) between subsequent polls
        timeout_seconds: Time (in seconds) to wait
        notify_states : States for which notifications should be sent, use None for all states

    See Also:
        https://docs.microsoft.com/en-us/microsoftteams/ \
        platform/webhooks-and-connectors/how-to/add-incoming-webhook

    Returns:
        A list of messages with their config sent via the Slack WebClient object
    """
    list_responses = monitor_job_common(
        tamr=tamr,
        send_message=send_message,
        send_status_function=_send_job_status_message,
        teams_connector_card=teams_connector_card,
        operation=operation,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
        notify_states=notify_states,
    )

    return list_responses

