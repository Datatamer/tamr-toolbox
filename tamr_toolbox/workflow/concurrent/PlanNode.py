from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List, Union
import time
import logging

from tamr_unify_client.project.resource import Project
from tamr_unify_client.operation import Operation
from tamr_toolbox.workflow.concurrent.PlanNodeStatus import (
    PlanNodeStatus,
    from_plan_node,
)
from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.models.project_steps import (
    SchemaMappingSteps,
    CategorizationSteps,
    MasteringSteps,
    GoldenRecordsSteps,
)
from tamr_toolbox.project import schema_mapping, categorization, mastering, golden_records

LOGGER = logging.getLogger(__name__)

WORKFLOW_MAP = {
    ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS: {
        SchemaMappingSteps.UPDATE_UNIFIED_DATASET: schema_mapping.jobs.update_unified_dataset
    },
    ProjectType.DEDUP: {
        MasteringSteps.UPDATE_UNIFIED_DATASET: mastering.jobs.update_unified_dataset,
        MasteringSteps.GENERATE_PAIRS: mastering.jobs.generate_pairs,
        MasteringSteps.APPLY_FEEDBACK: mastering.jobs.apply_feedback,
        MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS: mastering.jobs.update_pair_predictions,
        MasteringSteps.UPDATE_CLUSTERS: mastering.jobs.update_clusters,
        MasteringSteps.PUBLISH_CLUSTERS: mastering.jobs.publish_clusters,
    },
    ProjectType.CATEGORIZATION: {
        CategorizationSteps.UPDATE_UNIFIED_DATASET: categorization.jobs.update_unified_dataset,
        CategorizationSteps.APPLY_FEEDBACK: categorization.jobs.apply_feedback,
        CategorizationSteps.UPDATE_RESULTS_ONLY: categorization.jobs.update_results_only,
    },
    ProjectType.GOLDEN_RECORDS: {
        GoldenRecordsSteps.PROFILE_GOLDEN_RECORDS: golden_records.jobs.update_profiling_info,
        GoldenRecordsSteps.UPDATE_GOLDEN_RECORDS: golden_records.jobs.update_golden_records,
        GoldenRecordsSteps.PUBLISH_GOLDEN_RECORDS: golden_records.jobs.publish_golden_records,
    },
}


@dataclass_json
@dataclass
class PlanNode:
    """
    Dataclass for the node of a Planner object
    - contains method for setting list of operations based on project type
    """

    name: str
    operations: List[Operation]
    project: Project
    priority: int
    current_op: Operation
    status: PlanNodeStatus = PlanNodeStatus.PLANNED
    train: bool = False
    project_type: ProjectType = field(init=False)
    project_steps: Union[
        List[SchemaMappingSteps],
        List[MasteringSteps],
        List[GoldenRecordsSteps],
        List[CategorizationSteps],
    ] = field(init=False)
    current_step: Union[
        SchemaMappingSteps, MasteringSteps, CategorizationSteps, GoldenRecordsSteps
    ] = None
    steps_to_run: Union[
        List[SchemaMappingSteps],
        List[MasteringSteps],
        List[GoldenRecordsSteps],
        List[CategorizationSteps],
    ] = None

    def __post_init__(self):
        # set the project type
        self.project_type = ProjectType[self.project.type]

        # then set project steps based on project type
        # first for schema mapping projects
        if self.project_type == ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS:
            self.project_steps = [SchemaMappingSteps.UPDATE_UNIFIED_DATASET]
        # now for mastering projects
        elif self.project_type == ProjectType.DEDUP:
            if self.train:
                self.project_steps = [
                    MasteringSteps.UPDATE_UNIFIED_DATASET,
                    MasteringSteps.GENERATE_PAIRS,
                    MasteringSteps.APPLY_FEEDBACK,
                    MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
                    MasteringSteps.UPDATE_CLUSTERS,
                    MasteringSteps.PUBLISH_CLUSTERS,
                ]
            else:
                self.project_steps = [
                    MasteringSteps.UPDATE_UNIFIED_DATASET,
                    MasteringSteps.GENERATE_PAIRS,
                    MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
                    MasteringSteps.UPDATE_CLUSTERS,
                    MasteringSteps.PUBLISH_CLUSTERS,
                ]
        # now for categorization projects
        elif self.project_type == ProjectType.CATEGORIZATION:
            if self.train:
                self.project_steps = [
                    CategorizationSteps.UPDATE_UNIFIED_DATASET,
                    CategorizationSteps.APPLY_FEEDBACK,
                    CategorizationSteps.UPDATE_RESULTS_ONLY,
                ]
            else:
                self.project_steps = [
                    CategorizationSteps.UPDATE_UNIFIED_DATASET,
                    CategorizationSteps.UPDATE_RESULTS_ONLY,
                ]
        # finally for golden record projects
        elif self.project_type == ProjectType.GOLDEN_RECORDS:
            self.project_steps = [
                GoldenRecordsSteps.PROFILE_GOLDEN_RECORDS,
                GoldenRecordsSteps.UPDATE_GOLDEN_RECORDS,
                GoldenRecordsSteps.PUBLISH_GOLDEN_RECORDS,
            ]

        else:
            raise NotImplementedError(
                f"Don't know how to create a PlanNode object for project type {self.project_type}"
            )


def poll(plan_node: PlanNode) -> PlanNode:
    """
    Polls the status of the current_op object and returns a new PlanNode with updated status

    Args:
        plan_node: the PlanNode object to poll

    Returns:
        Copy of the original plan node with status updated based on the status of the current_op
    """
    # get current op and see if it is None (i.e. if plannode hasn't been triggered)
    current_op = plan_node.current_op

    if current_op is None:
        # if this node hasn't been triggered just return the current status in case
        # it has been updated by upstream actions
        updated_plan_node_status = plan_node.status
        updated_current_op = None
        updated_operations = None
    else:
        # current op exists so update it
        updated_current_op = plan_node.current_op.poll()
        # also update the list of all ops - rigamarole is to avoid updating current op twice
        # since it already exists in plan_node.operations list
        updated_operations = [x.poll() for x in plan_node.operations if x != current_op]
        updated_operations.append(updated_current_op)

        # if the op status changed set the plan node's current op to the updated one
        # and use from_plan_node to capture logic around in progress
        if updated_current_op.state != current_op.state:
            # update the current op
            plan_node.current_op = updated_current_op
            plan_node.operations = updated_operations
            updated_plan_node_status = from_plan_node(plan_node)
        # if the op didn't change status neither will the node so just pass through
        else:
            updated_plan_node_status = plan_node.status

    return PlanNode(
        name=plan_node.name,
        operations=updated_operations,
        project=plan_node.project,
        priority=plan_node.priority,
        current_op=updated_current_op,
        status=updated_plan_node_status,
        current_step=plan_node.current_step,
        steps_to_run=plan_node.steps_to_run,
    )


def run_next_step(plan_node: PlanNode) -> PlanNode:
    """
    Takes a plan node and runs the project

    Args:
        plan_node: the node to run

    Returns:
        updated plan node
    """
    # if current_step is None this node has never been run,
    # so set it and the steps_to_run from project_steps
    current_step = plan_node.current_step
    if current_step is None:
        steps_to_run = plan_node.project_steps[1:]
        next_step = plan_node.project_steps[0]
    # else we are on step 2+ so user and trim list of steps_to_run
    else:
        LOGGER.debug(f"plan node in trigger next step: {plan_node}")
        next_step = plan_node.steps_to_run[0]
        steps_to_run = plan_node.steps_to_run[1:]

    LOGGER.info(f"running step {next_step.value} for project {plan_node.name}")
    # We only call methods that return a list with one and only one operation
    current_op = WORKFLOW_MAP[plan_node.project_type][next_step](
        plan_node.project, process_asynchronously=True
    )[0]
    # handle case where operations is empty list (nothing has been run)
    if plan_node.operations is None:
        operations_list = [current_op]
    else:
        operations_list = [x for x in plan_node.operations]
        if current_op not in operations_list:
            operations_list.append(current_op)
    # don't forget to update the status - and first update the current op and operations list
    plan_node.current_op = current_op
    plan_node.operations = operations_list
    plan_node.current_step = next_step
    status = from_plan_node(plan_node)
    return PlanNode(
        name=plan_node.name,
        operations=operations_list,
        current_op=current_op,
        priority=plan_node.priority,
        project=plan_node.project,
        steps_to_run=steps_to_run,
        status=status,
        current_step=next_step,
    )


def monitor(
    nodes: List[PlanNode], *, timeout: int = 12, polling_interval: int = 30
) -> List[PlanNode]:
    """
    Monitors the status of a list of PlanNodes, when one on that list changes to
        either failed/succeeded/cancelled it returns the list

    Args:
        nodes: list of nodes to monitor
        timeout: number of hours to poll before timing out for change in job status
        polling_interval: the amount of time in seconds to wait between polling

    Returns:
        the same list of planNodes with updated statuses
    """
    # if empty then return immediate and log a warning
    if not nodes:
        LOGGER.warning("Jobs list is empty! Something is probably wrong.")
        return []

    start_time = time.time()
    timeout_in_seconds = 3600 * timeout

    # get initial statuses
    initial_statuses = {x.name: x.status for x in nodes}

    # get project names for logging
    running_projects = "\n".join([x.name for x in nodes])
    LOGGER.info(
        f"starting to monitor projects:\n {running_projects}\n with timeout of {timeout} hours"
    )
    # poll only as long as we haven't gotten past timeout
    while (time.time() - start_time) < timeout_in_seconds:

        # sleep at beginning of loop for polling interval
        time.sleep(polling_interval)

        # poll the jobs and get updated nodes
        updated_nodes = [poll(x) for x in nodes]
        # generate updated statuses
        updated_statuses = {x.name: x.status for x in updated_nodes}

        # if status has changed return list
        if initial_statuses != updated_statuses:
            return updated_nodes

    # if we got here it timed out and raise runtime error
    error_message = f"Monitoring jobs:\n {running_projects}\n timed out after {timeout} hours!"
    LOGGER.error(error_message)
    raise RuntimeError(error_message)
