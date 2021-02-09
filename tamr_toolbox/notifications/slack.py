"""Tasks related to creation of Slack notifications"""
import logging
import time
import os
from typing import Union, List, Optional

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import get_details, from_resource_id

LOGGER = logging.getLogger(__name__)

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    import slack


def send_message(
    *, slack_client: "slack.WebClient", channel: str, message: str, raise_error: bool = True,
) -> dict:
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
) -> dict:
    """
    Checks operation state and if in `notify_states` sends the message.

    Args:
        slack_client: A Slack WebClient
        channel: A (public) Slack channel to receive messages
        operation: A Tamr Operation
        notify_states: States for which notifications should be sent

    Returns:
        The data property of the Slack WebClient object
    """
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
) -> List[dict]:
    """

    Monitors a Tamr Operation and sends a Slack message to a channel when the job status is updated

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
    if notify_states is None:
        notify_states = [
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELED,
            OperationState.PENDING,
            OperationState.RUNNING,
        ]
    started = time.time()
    list_responses = []
    status = None

    if isinstance(operation, Operation):
        op = operation
    else:
        op = from_resource_id(tamr=tamr, job_id=operation)

    while (timeout_seconds is None or time.time() - started < timeout_seconds) and status not in [
        OperationState.SUCCEEDED,
        OperationState.FAILED,
        OperationState.CANCELED,
    ]:
        # Check the status of the current operation.
        # If the state is updated such that status!=new_status then send a message and
        # update `status`.
        # The loop exits if hits the timeout or if the jobs is in
        # a final state (SUCCEEDED/FAILED/CANCELED).
        # Note that it will always send a message about any status that has been
        # updated before exiting
        op = op.poll()
        new_status = OperationState[op.state]
        if status != new_status:
            message = _send_job_status_message(
                slack_client=slack_client,
                channel=channel,
                operation=op,
                notify_states=notify_states,
            )
            list_responses.append(message)
            status = new_status
        time.sleep(poll_interval_seconds)

    if status not in [
        OperationState.SUCCEEDED,
        OperationState.FAILED,
        OperationState.CANCELED,
    ]:
        # If the operation was not in a final state then assume it timed out
        message = send_message(
            slack_client=slack_client,
            channel=channel,
            message=(
                f"The job {op.resource_id}: {op.description} took longer "
                f"than {timeout_seconds} seconds to resolve."
            ),
        )
        list_responses.append(message)

    return list_responses
