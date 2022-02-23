"""Tasks related to creation of Email notifications"""
from typing import Union, List, Optional, Callable
from tamr_toolbox.models.data_type import JsonDict
from tamr_unify_client import Client
from tamr_unify_client.operation import Operation
from tamr_toolbox.utils.operation import monitor

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import from_resource_id


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
        response = send_status_function(operation=op, notify_states=notify_states, **kwargs,)
        list_responses.append(response)

    while status not in [
        OperationState.SUCCEEDED,
        OperationState.FAILED,
        OperationState.CANCELED,
    ]:
        try:
            op = monitor(
                operation=op,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
            )
            status = OperationState[op.state]
            # send message on state change
            response = send_status_function(operation=op, notify_states=notify_states, **kwargs,)
            list_responses.append(response)
        except TimeoutError:
            timeout_message = (
                f"The job {op.resource_id}: {op.description} took longer "
                f"than {timeout_seconds} seconds to resolve."
            )
            response = send_message(
                message=timeout_message, subject_line=f"Job {op.resource_id}: Timeout", **kwargs,
            )
            list_responses.append(response)
            return list_responses

    return list_responses
