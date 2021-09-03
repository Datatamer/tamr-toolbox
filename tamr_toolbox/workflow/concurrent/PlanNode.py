from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import List

from tamr_unify_client.project.resource import Project
from tamr_unify_client.operation import Operation
from tamr_toolbox.workflow.concurrent.PlanNodeStatus import PlanNodeStatus, from_plan_node
from tamr_toolbox.models.project_type import ProjectType
import tamr_toolbox.project

@dataclass_json
@dataclass
class PlanNode:
    """
    Simple wrapper for the contents of a node of a plan
    """

    name: str
    operations: List[Operation]
    project: Project
    status: PlanNodeStatus
    priority: int


def run(plan_node: PlanNode) -> PlanNode:
    """
    Takes a plan node and runs the project

    Args:
        plan_node: the node to run

    Returns:
        updated plan node
    """

    project = plan_node.project
    project_type = ProjectType(project.type)
    if project_type == ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS:
        operations = tamr_toolbox.project.schema_mapping.jobs.run(project)
    elif project_type == ProjectType.CATEGORIZATION:
        operations = tamr_toolbox.project.categorization.jobs.run(project.as_categorization())
    elif project_type == ProjectType.DEDUP:
        operations = tamr_toolbox.project.mastering.jobs.run(project.as_mastering())
    elif project_type == ProjectType.GOLDEN_RECORDS:
        operations = tamr_toolbox.project.golden_records.jobs.run(project)
    else:
        raise NotImplementedError(f"Don't know how to run jobs for project type {project_type}")

    # now update the plan node
    # TODO: ensure the above are async and can be passed to monitor_jobs
    # TODO: update the current plan node or return new one