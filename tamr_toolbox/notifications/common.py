"""Tasks related to creation of Email notifications"""
import time

from typing import Union, List, Optional
from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import from_resource_id


def monitor_job(
    tamr: Client,
    *,
    send_message: object,
    send_status_function: object,
    operation: Union[int, str, Operation],
    poll_interval_seconds: float = 1,
    timeout_seconds: Optional[float] = None,
    notify_states: Optional[List[OperationState]] = None,
    **kwargs,
) -> List[dict]:
    """

    Monitors a Tamr Operation and sends an email when the job status is updated

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
            response = send_status_function(operation=op, notify_states=notify_states, **kwargs,)
            list_responses.append(response)
            status = new_status
        time.sleep(poll_interval_seconds)

    if status not in [
        OperationState.SUCCEEDED,
        OperationState.FAILED,
        OperationState.CANCELED,
    ]:
        # If the operation was not in a final state then assume it timed out
        timeout_message = (
            f"The job {op.resource_id}: {op.description} took longer "
            f"than {timeout_seconds} seconds to resolve."
        )
        response = send_message(
            message=timeout_message, subject_line=f"Job {op.resource_id}: Timeout", **kwargs,
        )
        list_responses.append(response)

    return list_responses
