"""Tasks related to creation of notifications"""
import warnings
from typing import Union, List, Optional, Callable

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import monitor, from_resource_id, get_details


class _BaseNotifier(object):
    def __init__(self):
        self.recipients = None

    def send_message(self, message: str, title: str, tamr_user: Optional[str] = None) -> None:
        """
        Sends a notification

        Args:
            message: The message to send
            title: The title of the message to send
            tamr_user:
                An optional single user to send the message to.
                If None (default), message everybody

        Raises:
            NotImplementedError: If called directly from the `_BaseNotifier` class,
                instead of a pre-defined notification type
        """
        raise NotImplementedError(
            "Do not call `_BaseNotifier` directly. " "Use e.g. `EmailNotifier` instead."
        )

    def monitor_job(
        self,
        tamr: Client,
        operation: Union[int, str, Operation],
        notify_states: Optional[List[OperationState]] = None,
        poll_interval: Optional[float] = 1,
        timeout: Optional[float] = None,
    ) -> None:
        """Monitors a Tamr Operation and sends a message when the job status is updated

        Args:
            tamr: A Tamr client
            operation: A job ID or a Tamr operation
            notify_states : States for which notifications should be sent, use None for all states
            poll_interval: Time interval (in seconds) between subsequent polls
            timeout: Time (in seconds) to wait before timing out

        Returns:
            A list of messages with their response codes
        """
        if not notify_states:
            notify_states = [
                OperationState.SUCCEEDED,
                OperationState.FAILED,
                OperationState.CANCELED,
                OperationState.PENDING,
                OperationState.RUNNING,
            ]

        if isinstance(operation, Operation):
            op = operation
        else:
            op = from_resource_id(tamr=tamr, job_id=operation)

        # Send message for initial state
        status = OperationState[op.state]
        if status in notify_states:
            self.send_message(
                message=get_details(operation=op), title=f"Job {operation.resource_id}: {status}"
            )

        while status not in [
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELED,
        ]:

            # Send message on state change
            try:
                op = monitor(
                    operation=op, poll_interval_seconds=poll_interval, timeout_seconds=timeout
                )

                status = OperationState[op.state]
                self.send_message(
                    message=get_details(operation=op),
                    title=f"Job {operation.resource_id}: {status}",
                )
            except TimeoutError:
                timeout_message = (
                    f"The job {op.resource_id}: {op.description} took longer "
                    f"than {timeout} seconds to resolve."
                )
                self.send_message(message=timeout_message, title=f"Job {op.resource_id}: Timeout")

    def _parse_recipients(self, tamr_user: Optional[str] = None) -> List[str]:
        """Look up Tamr users & parse recipients into an iterable list"""
        if type(self.recipients) in [str, list]:
            if tamr_user:
                raise ValueError(
                    "If specifying a Tamr user, the recipients class input"
                    " must take the form of a lookup dictionary"
                    " relating Tamr usernames to message destinations"
                )
            else:
                if type(self.recipients) == str:
                    return [self.recipients]
                else:
                    return self.recipients
        elif type(self.recipients) == dict:
            if tamr_user:
                return [self.recipients[tamr_user]]
            else:
                return list(self.recipients.values())
        else:
            return []


def _monitor_job(
    tamr: Client,
    *,
    send_message: Callable[..., JsonDict],
    send_status_function: Callable[..., JsonDict],
    operation: Union[int, str, Operation],
    poll_interval_seconds: float = 1,
    timeout_seconds: Optional[float] = None,
    notify_states: Optional[List[OperationState]] = None,
    **kwargs,
) -> List[JsonDict]:
    """Monitors a Tamr Operation and sends a message when the job status is updated

    Args:
        tamr: A Tamr client
        send_message: either send_email from emails or send_message function from slack
        send_status_function: _send_job_status_message function from
        notifications.slack or notifications.email
        operation: A job ID or a Tamr operation
        poll_interval_seconds: Time interval (in seconds) between subsequent polls
        timeout_seconds: Time (in seconds) to wait
        notify_states : States for which notifications should be sent, use None for all states

    Returns:
        A list of messages with their response codes
    """
    warnings.warn(
        "'This function will be deprecated in a future release. "
        "Use e.g. SlackNotifier().monitor_job() instead.'",
        DeprecationWarning,
    )
    if notify_states is None:
        notify_states = [
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELED,
            OperationState.PENDING,
            OperationState.RUNNING,
        ]
    list_responses = []

    if isinstance(operation, Operation):
        op = operation
    else:
        op = from_resource_id(tamr=tamr, job_id=operation)

    # send message for initial state
    status = OperationState[op.state]
    if status in notify_states:
        response = send_status_function(operation=op, notify_states=notify_states, **kwargs)
        list_responses.append(response)

    while status not in [OperationState.SUCCEEDED, OperationState.FAILED, OperationState.CANCELED]:
        try:
            op = monitor(
                operation=op,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
            )
            status = OperationState[op.state]
            # send message on state change
            response = send_status_function(operation=op, notify_states=notify_states, **kwargs)
            list_responses.append(response)
        except TimeoutError:
            timeout_message = (
                f"The job {op.resource_id}: {op.description} took longer "
                f"than {timeout_seconds} seconds to resolve."
            )
            response = send_message(
                message=timeout_message, subject_line=f"Job {op.resource_id}: Timeout", **kwargs
            )
            list_responses.append(response)
            return list_responses

    return list_responses
