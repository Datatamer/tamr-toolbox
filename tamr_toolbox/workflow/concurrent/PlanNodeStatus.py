"""Enum representing job status"""
from enum import IntEnum
import logging
from tamr_unify_client.operation import Operation

import tamr_toolbox

LOGGER = logging.getLogger(__name__)


class PlanNodeStatus(IntEnum):
    """
    A class representing job status for executing a concurrent pipeline
    """

    FAILED = -2  # one or more of the jobs for the project failed
    CANCELED = -1  # one or more of the jobs for the project was cancelled
    BLOCKED = 0  # running this project is blocked by upstream dependencies failing
    PLANNED = 1  # default state - planned but not yet runnable due to upstream dependencies
    SKIPPABLE = 2  # used when, for example, starting later in the plan
    RUNNABLE = 3  # all dependencies met (e.g. starting tier project) - the project can be run
    PENDING_NEXT_STEP = 4  # project has run some steps successfully and has more steps remaining
    RUNNING = 5  # a job for the project is currently running
    SUCCEEDED = 6  # all project steps completed successfully


def from_tamr_op(op: Operation) -> PlanNodeStatus:
    LOGGER.debug(f"checking state of operation {op}")
    # check each operation
    if op.state == "PENDING" or op.state == "RUNNING":
        return PlanNodeStatus.RUNNING
    elif op.state == "SUCCEEDED":
        return PlanNodeStatus.SUCCEEDED
    elif op.state == "FAILED":
        return PlanNodeStatus.FAILED
    elif op.state == "CANCELED" or op.state == "CANCELING":
        return PlanNodeStatus.CANCELED
    else:
        raise ValueError(f"{op.state} can't be translated to a PlanNodeStatus object")


def from_plan_node(
    plan_node: "tamr_toolbox.workflow.concurrent.PlanNode.PlanNode",
) -> PlanNodeStatus:
    """
    Return a PlanNode Status from a PlanNode object.

    Args:
        plan_node: the plan node object to get the status of

    Returns:
        Latest view of the status of the PlanNode passed in
    """
    # get the status of all operations for this node and convert to status
    # if there are none then and there is nothing to check - just return the status
    if plan_node.operations is None:
        return plan_node.status

    # if there are operations check the operations poll them and see the latest state
    LOGGER.debug(f"getting plan node status at plan node at step {plan_node.current_step}")
    operations_to_poll = [x for x in plan_node.operations]
    all_statuses = [from_tamr_op(x.poll()) for x in operations_to_poll]

    # since the enum is ordered just send the minimum status of all operations back
    # this means any failures/cancellations result in failure/cancelled
    # and then any operations that are less than succeeded represent the overall
    # status of this node, which is correct (remember that each node is a project with
    # multiple operations associated with updating it)

    # if the plan node has next steps and its min is succeeded then mark as pending
    min_status = min(all_statuses)
    if plan_node.steps_to_run and min_status == PlanNodeStatus.SUCCEEDED:
        return PlanNodeStatus.PENDING_NEXT_STEP
    else:
        return min_status
