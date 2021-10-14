"""Enum representing plan status"""
from enum import Enum
from tamr_toolbox.workflow.concurrent.PlanNodeStatus import PlanNodeStatus


class PlanStatus(Enum):
    """A class representing the status of a tamr_toolbox.workflow.concurrent.Planner plan"""

    PLANNED = "planned"
    RUNNING = "running"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


def from_planner(planner) -> PlanStatus:
    """
    Create a PlanStatus object from a planner object
    Args:
        planner: the planner to pass in

    Returns:
        the plan's status  as a PlanStatus object
    """

    plan = planner.plan

    # check for failures first and make sure nothing can be run or is running
    if any(v.status == PlanNodeStatus.FAILED for k, v in plan.items()) and not any(
        (v.status == PlanNodeStatus.RUNNABLE or v.status == PlanNodeStatus.RUNNING)
        for k, v in plan.items()
    ):
        return PlanStatus.FAILED
    # then check to see if any job is running
    elif any(v.status == PlanNodeStatus.RUNNING for k, v in plan.items()):
        return PlanStatus.RUNNING
    # if not failed or running check if successful
    elif all(
        (v.status == PlanNodeStatus.SUCCEEDED) or (v.status == PlanNodeStatus.SKIPPABLE)
        for k, v in plan.items()
    ):
        return PlanStatus.SUCCEEDED
    # else assume still in planned state
    else:
        return PlanStatus.PLANNED
