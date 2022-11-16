"""Tasks related to Tamr operations (or jobs)"""
import logging
from typing import Union, List, Optional
from time import sleep, time as now

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation
from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.models.operation_state import OperationState

LOGGER = logging.getLogger(__name__)


def enforce_success(operation: Operation) -> None:
    """Raises an error if an operation fails

    Args:
        operation: A Tamr operation
    """
    if not operation.succeeded():
        raise RuntimeError(
            f"Operation {operation.resource_id} failed. Description: {operation.description}."
            f"Status: {operation.status}"
        )


def from_resource_id(tamr: Client, *, job_id: Union[int, str]) -> Operation:
    """Create an operation from a job id

    Args:
        tamr: A Tamr client
        job_id: A job ID

    Returns:
        A Tamr operation
    """
    # Tamr sometimes returns a job id of -1 if everthing is up-to-date so the requested task
    # would result in no change -- make a dummy operation in this case
    if str(job_id) == "-1":
        return Operation.from_json(tamr, _dummy_no_op_response())

    return Operation.from_resource_id(tamr, str(job_id))


def _dummy_no_op_response(code: str = "job ID -1") -> JsonDict:
    """
    Create a dict to represent 'no-operation operation', in cases where all results that would be
    produced by an operation are already up-to-date so there's nothing to do.

    Args:
        code: response code include in description, Default 'job ID -1'

    Returns:
        dict from which dummy operation object can be constructed
    """

    _never = "0000-00-00T00:00:00.000Z"
    _description = f"""Tamr returned {code} for this operation, indicating that all
        results that would be produced by the operation are already up-to-date."""
    dummy_dict = {
        "id": "-1",
        "type": "NOOP",
        "description": _description,
        "status": {"state": "SUCCEEDED", "startTime": _never, "endTime": _never, "message": ""},
        "created": {"username": "", "time": _never, "version": "-1"},
        "lastModified": {"username": "", "time": _never, "version": "-1"},
        "relativeId": "operations/-1",
    }
    return dummy_dict


def get_latest(tamr: Client) -> Operation:
    """Get the latest operation

    Args:
        tamr: A Tamr client

    Returns:
        The latest job
    """
    op = get_all(tamr)[0]
    return op


def get_details(*, operation: Operation) -> str:
    """Return a text describing the information of a job

    Args:
        operation: A Tamr operation

    Returns:
        A text describing the information of a job
    """
    return (
        f"Host: {operation.client.host} \n Job: {operation.resource_id} \n "
        f"Description: {operation.description} \n Status: {operation.state} "
    )


def get_all(tamr: Client) -> List[Operation]:
    """Get a list of all jobs or operations.

    Args:
        tamr: A Tamr client

    Returns:
        A list of Operation objects.

    """
    response = tamr.get(
        "/api/versioned/v1/operations", headers={"Accept": "application/json"}, stream=True
    ).json()
    ops = [Operation.from_json(tamr, item) for item in response]

    return ops


def get_active(tamr: Client) -> List[Operation]:
    """Get a list of pending and running jobs.

    Args:
        tamr: A Tamr client

    Returns:
        A list of Operations objects

    """
    ops = get_all(tamr)
    active_states = [OperationState.PENDING, OperationState.RUNNING]
    active_ops = [op for op in ops if OperationState(op.state) in active_states]

    return active_ops


def wait(
    operation: Operation, *, poll_interval_seconds: int = 3, timeout_seconds: Optional[int] = None
) -> Operation:
    """Continuously polls for this operation's server-side state.

    Args:
        operation: Operation to be polled.
        poll_interval_seconds: Time interval (in seconds) between subsequent polls.
        timeout_seconds: Time (in seconds) to wait for operation to resolve.

    Raises:
        TimeoutError: If operation takes longer than `timeout_seconds` to resolve.
    """
    started = now()
    while timeout_seconds is None or now() - started < timeout_seconds:
        if operation.status is None:
            return operation
        elif operation.status["state"] in [OperationState.PENDING, OperationState.RUNNING]:
            sleep(poll_interval_seconds)
        elif operation.status["state"] in [
            OperationState.CANCELED,
            OperationState.SUCCEEDED,
            OperationState.FAILED,
        ]:
            return operation
        operation = operation.poll()
    raise TimeoutError(f"Waiting for operation took longer than {timeout_seconds} seconds.")


def monitor(
    operation: Operation, *, poll_interval_seconds: float = 1, timeout_seconds: float = 300
) -> Operation:
    """Continuously polls for this operation's server-side state and returns operation
    when there is a state change

    Args:
        operation: Operation to be monitored.
        poll_interval_seconds: Time interval (in seconds) between subsequent polls.
        timeout_seconds: Time (in seconds) to wait for operation to resolve.

    Raises:
        TimeoutError: If operation takes longer than `timeout_seconds` to resolve.
    """
    status = OperationState[operation.state]
    started = now()
    while timeout_seconds is None or now() - started < timeout_seconds:
        operation = operation.poll()
        new_status = OperationState[operation.state]
        if operation.status is None:
            return operation
        elif new_status == status:
            sleep(poll_interval_seconds)
        else:
            return operation
    raise TimeoutError(f"Waiting for operation took longer than {timeout_seconds} seconds.")


def safe_estimate_counts(project) -> Operation:
    """
    Run the estimate counts job of project that works if it is the first
    Args:
        project: A Tamr project object
    Returns:
        An operation object for the estimate pairs job
    """
    project = project.as_mastering()
    response = project.client.post(f"{project.api_path}/estimatedPairCounts:refresh")

    return Operation.from_response(project.client, response).wait()
