"""Tasks related to running jobs for Tamr Categorization projects"""
import logging
from typing import List

from tamr_unify_client.categorization.project import CategorizationProject
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.utils import operation

LOGGER = logging.getLogger(__name__)


def _run_custom(
    project: CategorizationProject,
    *,
    run_update_unified_dataset: bool = False,
    run_apply_feedback: bool = False,
    run_update_results: bool = False,
) -> List[Operation]:
    """Executes specified steps of a categorization project.

    Args:
        project: The target categorization project
        run_update_unified_dataset: Whether refresh should be called on the unified dataset
        run_apply_feedback: Whether train should be called on the pair matching model
        run_update_results: Whether predict should be called on the pair matching model

    Returns:
        Operations that were run

    Raises:
        TypeError: if `project` is not a categorization project
    """
    if ProjectType[project.type] != ProjectType.CATEGORIZATION:
        error_msg = f"Cannot use as a categorization project. Project type: {project.type}"
        LOGGER.error(error_msg)
        raise TypeError(error_msg)
    else:
        project = project.as_categorization()

    completed_operations = []
    if run_update_unified_dataset:
        LOGGER.info(
            f"Updating the unified dataset for project {project.name} (id={project.resource_id})."
        )
        op = project.unified_dataset().refresh()
        operation.enforce_success(op)
        completed_operations.append(op)
    if run_apply_feedback:
        LOGGER.info(
            f"Applying feedback to the categorization model for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.model().train()
        operation.enforce_success(op)
        completed_operations.append(op)
    if run_update_results:
        LOGGER.info(
            f"Updating categorization results for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.model().predict()
        operation.enforce_success(op)
        completed_operations.append(op)

    return completed_operations


def run(project: CategorizationProject, *, run_apply_feedback: bool = False) -> List[Operation]:
    """Run the project

    Args:
        project: The target categorization project
        run_apply_feedback: Whether train should be called on the categorization model

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=True,
        run_apply_feedback=run_apply_feedback,
        run_update_results=True,
    )


def update_unified_dataset(project: CategorizationProject) -> List[Operation]:
    """Updates the unified dataset for a categorization project

    Args:
        project: Target categorization project

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=True,
        run_apply_feedback=False,
        run_update_results=False,
    )


def apply_feedback_and_update_results(project: CategorizationProject,) -> List[Operation]:
    """Trains the model and updates the categorization predictions of a categorization project

    Args:
        project: Target categorization project

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_apply_feedback=True,
        run_update_results=True,
    )


def update_results_only(project: CategorizationProject) -> List[Operation]:
    """Updates the categorization predictions based on the existing model of a
    categorization project

    Args:
        project: Target categorization project

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_apply_feedback=False,
        run_update_results=True,
    )
