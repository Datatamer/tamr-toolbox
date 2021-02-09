"""Tasks related to Tamr operations (or jobs)"""
import logging
from typing import Union, List

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation
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
    job_response = tamr.get(f"/api/versioned/v1/operations/{job_id}")
    return Operation.from_response(tamr, job_response)


def get_latest(tamr: Client) -> Operation:
    """ Get the latest operation

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
    """
    Get a list of all jobs or operations.

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
    """
    Get a list of pending and running jobs.

    Args:
        tamr: A Tamr client

    Returns:
        A list of Operations objects

    """
    ops = get_all(tamr)
    active_states = [OperationState.PENDING, OperationState.RUNNING]
    active_ops = [op for op in ops if OperationState(op.state) in active_states]

    return active_ops
