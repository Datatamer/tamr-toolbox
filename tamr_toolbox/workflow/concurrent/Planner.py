from datetime import datetime
import json
import os
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Dict, List
import logging

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation
from tamr_toolbox.models.data_type import JsonDict

from tamr_toolbox.workflow.concurrent.Graph import (
    Graph,
    get_projects_by_tier,
    get_all_downstream_nodes,
    get_successors,
    get_predecessors,
)
from tamr_toolbox.workflow.concurrent import PlanNodeStatus
from tamr_toolbox.workflow.jobs import monitor
from tamr_toolbox.workflow.concurrent.PlanStatus import PlanStatus, from_planner
from tamr_toolbox.workflow.concurrent.PlanNode import PlanNode


LOGGER = logging.getLogger(__name__)

# TODO Refactor to have the plan be about executing plan nodes (projects)
# TODO and move the machinery of executing a project to the plan node

@dataclass_json
@dataclass
class Planner:
    """
    A dataclass to hold the plan, the starting tier, and the mode of execution.
    The plan is a json dict where each key is a project name and the value is a json like so:
    {'priority': xxx, 'status': <planned, runnable, running, failed, skippable, succeeded>,
    'name': project_name, id: job_id}

    The starting tier is the tier at which to start execution. All jobs at lower tiers are marked as skippable.

    The graph is the graph that contains the backing project dependencies.
    """

    plan: Dict[str, PlanNode]
    starting_tier: int
    graph: Graph
    output_config: JsonDict


def from_graph(
    graph: Graph,
    *,
    starting_tier: int = 0,
    output_config: JsonDict = None,
) -> Planner:
    """
    Creates a Planner class from a Graph. The plan object is a json dict specifying how
    the plan can be executed and its status.

    Args:
        graph: the dataset dependency graph to use to create the planner
        starting_tier: the tier at which to start executing the plan, every job at lower tiers is skipped and marked
        as skippable
        mode: the execution mode for this plan. One of 'greedy' or 'standard'. Not currently used - everything is greedy
        output_config: a dict for how to configure output jobs

    Returns:
        Planner instance
    """

    # start with the project tier dict from the graph
    tier_graph = get_projects_by_tier(graph)

    # start building the plan
    plan = {}
    for tier, project_list in tier_graph.items():
        for num, project in enumerate(project_list):
            # mark things as skippable if the tier is less than the starting tier
            if tier < starting_tier:
                status = PlanNodeStatus.PlanNodeStatus.SKIPPABLE
            elif tier == starting_tier:
                status = PlanNodeStatus.PlanNodeStatus.RUNNABLE
            else:
                status = PlanNodeStatus.PlanNodeStatus.PLANNED
            plan[project] = PlanNode(
                priority=(100 * tier) + num, status=status, name=project
            )

    return Planner(
        plan=plan,
        starting_tier=starting_tier,
        graph=graph,
        output_config=output_config
    )


def update_plan(planner: Planner, *, plan_node: PlanNode) -> Planner:

    # first just update the status of that node
    original_plan = planner.plan
    updated_plan = dict(original_plan)
    LOGGER.info(f"Updating plan with changed project status: {plan_node}")
    plan_node_name = plan_node.name
    node_status = PlanNodeStatus.from_plan_node(plan_node)
    updated_plan[plan_node_name].status = node_status
    updated_plan[plan_node_name].operations = plan_node.operations

    # now find downstream affects
    downstream_nodes = get_all_downstream_nodes(planner.graph, plan_node_name)
    # if status == failed then easy to update them all to blocked
    if node_status == PlanNodeStatus.PlanNodeStatus.FAILED:
        for node in downstream_nodes:
            updated_plan[node].status = PlanNodeStatus.PlanNodeStatus.BLOCKED
    # else if update is skippable or successful then need to see if we can mark them as runnable
    elif node_status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED or node_status == PlanNodeStatus.PlanNodeStatus.SKIPPABLE:
        # first get immediate downstream nodes
        successor_nodes = get_successors(planner.graph, plan_node_name)
        # for each of these get the predecessors and if all predecessors are now succeeded/skippable mark as runnable
        for successor in successor_nodes:
            predecessor_nodes = get_predecessors(planner.graph, successor)
            if all(
                    updated_plan[x].status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED
                    or updated_plan[x].status == PlanNodeStatus.PlanNodeStatus.SKIPPABLE
                for x in predecessor_nodes
            ):
                updated_plan[successor].status = PlanNodeStatus.PlanNodeStatus.RUNNABLE

    # no other status should change things
    # so do nothing else

    return Planner(
        plan=updated_plan,
        graph=planner.graph,
        starting_tier=planner.starting_tier,
        output_config=planner.output_config
    )


def _submit_job_maybe(planner: Planner, tamr: Client, node_name: str) -> Operation:
    """Submit a job to update a unified dataset asynchronously. If dataset is streamable
    then just returns empty Op with successful status

    Args:
        planner: The planner object to use to submit the job
        tamr: the tamr client
        node_name: the name of a project OR the project name prepended with 'EXPORT-' (e.g. 'EXPORT-Truven-PERSON')

    """
    #TODO: refactor to use run the whole project
    # if it is a project_name then just find it
    try:
        project = tamr.projects.by_name(node_name)
        if project.unified_dataset().status().is_streamable:
            op = Operation(
                tamr,
                data={
                    "relativeId": -1,
                    "state": "SUCCEEDED",
                    "description": f"No-op since {project.unified_dataset().name} is streamable.",
                },
            )
        else:
            req = tamr.post(
                f"/api/versioned/v1/projects/{project.resource_id}/unifiedDataset:refresh"
            )
            op = Operation.from_response(tamr, req)
    # if it is not a project name then see if it is an export job
    #TODO Clean up this handling of errors
    except KeyError:
        raise RuntimeError

    return op


def execute(
    planner: Planner, tamr: Client, *, concurrency_level: int = 2, save_state: bool = False
) -> Planner:
    """
    Executes the plan

    Args:
        planner: The planner object whose plan will be executed
        tamr: the tamr client to use
        concurrency_level: the number of concurrent jobs to run at once
        save_state: whether or not to save the plan state to json after each update

    Returns:
        the planner object after execution
    """

    # get the plan and sort by priority
    plan = planner.plan
    sorted_jobs = [v for k, v in sorted(plan.items(), key=lambda x: x[1].priority)]
    # assume you could be given a partially executed plan so create both running and runnable
    runnable_jobs = [x for x in sorted_jobs if x.status == PlanNodeStatus.PlanNodeStatus.RUNNABLE]
    running_jobs = [x for x in sorted_jobs if x.status == PlanNodeStatus.PlanNodeStatus.RUNNING]

    # check status and run if runnable or planned
    plan_status = from_planner(planner)
    if plan_status == PlanStatus.PLANNED or plan_status == PlanStatus.RUNNING:
        LOGGER.info(
            f"currently running jobs: {','.join([x.name for x in running_jobs]) or 'None'}"
        )
        # make sure there are fewer jobs running than concurrency specified
        num_to_submit = concurrency_level - len(running_jobs)
        LOGGER.info(f"Have room in queue for {num_to_submit} jobs:")

        # slice runnable jobs to get the ones to submit
        # this line is for type hinting
        jobs_to_submit: List[PlanNode] = []
        if len(runnable_jobs) >= num_to_submit:
            jobs_to_submit = [x for x in runnable_jobs[0:num_to_submit]]
        else:
            jobs_to_submit = [x for x in runnable_jobs]

        LOGGER.info(f"submitting jobs: [{','.join(x.name for x in jobs_to_submit)}]")
        # get tamr projects and update their unified dataset or export
        projects_to_run = [x.name for x in jobs_to_submit]
        jobs_to_monitor = [
            PlanNode(
                name=x,
                status=PlanNodeStatus.PlanNodeStatus.RUNNABLE,
                priority=0,
            )
            for x in projects_to_run
        ]
        # extend jobs_to_monitor to include running jobs
        LOGGER.info(f"Adding currently running jobs to queue: {running_jobs}")
        jobs_to_monitor.extend(
            [
                PlanNode(
                    name=x.name,
                    status=PlanNodeStatus.PlanNodeStatus.RUNNING,
                    priority=0,
                )
                for x in running_jobs
            ]
        )

        # there are potentially jobs that were not submitted because the dataset is already streamable
        # for these simply filter out and update plan
        # first find them
        noop_jobs = [x for x in jobs_to_monitor if x.operations.description.find("No-op") != -1]
        # then filter these out
        jobs_to_monitor = [x for x in jobs_to_monitor if x not in noop_jobs]
        # now update the plan for the no-ops
        for noop_job in noop_jobs:
            planner = update_plan(planner, plan_node=noop_job)

        # now monitor the ones that really submit a job
        # this function returns when there is any change in state
        jobs_to_monitor = monitor(jobs_to_monitor)
        LOGGER.info(f"Got updated set of jobs: {jobs_to_monitor}")
        # now update the plan - only monitored jobs should have a change in status
        for job in jobs_to_monitor:
            planner = update_plan(planner, plan_node=job)

        LOGGER.info(f"after recent update plan status is {from_planner(planner)}")

        # if save state then save a copy of the plan:
        # todo: refactor save state to its own function
        if save_state:
            now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            basedir = os.path.dirname(os.path.abspath(__file__))
            with open(f"{basedir}/../../logs/planner_{now}.json", "w") as outfile:
                outfile.write(
                    json.dumps(
                        [
                            {"name": v.name, "status": v.status, "priority": v.priority}
                            for k, v in planner.plan.items()
                        ]
                    )
                )
        # planner is updated so now try to execute it again
        planner = execute(planner, tamr=tamr, concurrency_level=concurrency_level)
        return planner

    # if planner isn't runnable and there were no export processes then exit
    else:
        LOGGER.info(f"plan status is {plan_status} so returning")
        return planner
