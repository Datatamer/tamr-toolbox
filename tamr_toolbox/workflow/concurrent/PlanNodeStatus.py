"""Enum representing job status"""
from enum import IntEnum
from tamr_unify_client.operation import Operation
from tamr_toolbox.workflow.concurrent.PlanNode import PlanNode


class PlanNodeStatus(IntEnum):
    """
    A class representing job status for executing a concurrent pipeline
    """

    FAILED = -2
    CANCELLED = -1
    PLANNED = 0
    SKIPPABLE = 1
    RUNNABLE = 2
    BLOCKED = 3
    RUNNING = 4
    SUCCEEDED = 5


def from_tamr_op(op: Operation) -> PlanNodeStatus:
    # handle special no-op jobs made for skippable jobs
    if op.description.find("No-op") != -1:
        return PlanNodeStatus.SUCCEEDED

    # now do real operations
    if op.state == "PENDING" or op.state == "RUNNING":
        return PlanNodeStatus.RUNNING
    elif op.state == "SUCCEEDED":
        return PlanNodeStatus.SUCCEEDED
    elif op.state == "FAILED":
        return PlanNodeStatus.FAILED
    elif op.state == "CANCELLED":
        return PlanNodeStatus.CANCELLED
    else:
        raise ValueError(f"{op.state} can't be translated to a PlanNodeStatus object")


def from_plan_node(plan_node: PlanNode) -> PlanNodeStatus:
    # get the status of all operations for this node and convert to status
    all_statuses = [from_tamr_op(x) for x in plan_node.operations]

    # since the enum is ordered just send the minimum status of all operations back
    # this means any failures/cancellations result in failure/cancelled
    # and then any operations that are less than succeeded represent the overall
    # status of this node, which is correct (remember that each node is a project with
    # multiple operations associated with updating it)
    return min(all_statuses)
