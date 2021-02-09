"""Enum representing operation states in Tamr"""
from enum import Enum


class OperationState(Enum):
    """A dataclass representing job state codes in Tamr

    Args:
        SUCCEEDED: Status when a job has completed successfully
        FAILED: Status when a job has been terminated due to an error
        CANCELED: Status when a job been terminated due to cancelation by a user or the job service
        PENDING: Status when a job has been submitted and is waiting to begin running
        RUNNING: Status when a job is active
    """

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
