"""Tasks related to running jobs for Tamr Schema Mapping projects"""
from typing import List
import logging

from tamr_unify_client.operation import Operation
from tamr_unify_client.project.resource import Project

from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.utils import operation

LOGGER = logging.getLogger(__name__)


def _run_custom(project: Project, *, run_update_unified_dataset=False) -> List[Operation]:
    """Executes specified steps of a schema mapping project.

    Args:
        project: Target schema mapping project
        run_update_unified_dataset: Whether refresh should be called on the unified dataset

    Returns:
        The operations that were run

    Raises:
        TypeError: if the `project` is not a Schema Mapping project
    """
    if ProjectType[project.type] != ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS:
        error_msg = f"Cannot use as a schema mapping project. Project type: {project.type}"
        LOGGER.error(error_msg)
        raise TypeError(error_msg)

    completed_operations = []
    if run_update_unified_dataset:
        LOGGER.info(
            f"Updating the unified dataset for project {project.name} (id={project.resource_id})."
        )
        op = project.unified_dataset().refresh()
        operation.enforce_success(op)
        completed_operations.append(op)

    return completed_operations


def run(project: Project) -> List[Operation]:
    """Run the project

    Args:
        project: Target schema mapping project

    Returns:
        The operations that were run
    """
    return _run_custom(project, run_update_unified_dataset=True)


def update_unified_dataset(project: Project) -> List[Operation]:
    """Updates the unified dataset for a schema mapping project

    Args:
        project: Target schema mapping project

    Returns:
        The operations that were run
    """
    return _run_custom(project, run_update_unified_dataset=True)
