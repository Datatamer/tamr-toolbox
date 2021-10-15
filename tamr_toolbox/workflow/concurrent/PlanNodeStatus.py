"""Enum representing job status"""
from enum import IntEnum
from tamr_unify_client.operation import Operation

import time

class PlanNodeStatus(IntEnum):
    """
    A class representing job status for executing a concurrent pipeline
    """

    FAILED = -2
    CANCELLED = -1
    PLANNED = 0
    SKIPPABLE = 1
    BLOCKED = 2
    RUNNABLE = 3
    PENDING = 4
    RUNNING = 5
    SUCCEEDED = 6


def from_tamr_op(op: Operation) -> PlanNodeStatus:
    print(f"checking state of operation {op}")
    # check each operation
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


def from_plan_node(
    plan_node: "tamr_toolbox.workflow.concurrent.PlanNode.PlanNode",
) -> PlanNodeStatus:
    # get the status of all operations for this node and convert to status
    # if there are none then and it hasn't been updated by other nodes set to PLANNED
    # TODO: update logic for above comment about it being being updated from other nodes failing
    if plan_node.operations is None:
        return PlanNodeStatus.PLANNED
    print(f"polling plan node at step {plan_node.current_step}")
    print(f"plan node is {plan_node}")
    # otherwise check the operations
    pre_poll_time = time.time()
    operations_to_poll = [x for x in plan_node.operations]
    print(f"operations to poll {operations_to_poll}")
    all_statuses = [from_tamr_op(x.my_poll()) for x in operations_to_poll]
    print(f"polling ops took {time.time() - pre_poll_time}")
    # since the enum is ordered just send the minimum status of all operations back
    # this means any failures/cancellations result in failure/cancelled
    # and then any operations that are less than succeeded represent the overall
    # status of this node, which is correct (remember that each node is a project with
    # multiple operations associated with updating it)

    # if the plan node has next steps and its min is succeeded then mark as pending
    min_status = min(all_statuses)
    if plan_node.steps_to_run and min_status == PlanNodeStatus.SUCCEEDED:
        return PlanNodeStatus.PENDING
    else:
        return min_status
