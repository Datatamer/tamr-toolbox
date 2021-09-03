"""Tasks related to running jobs for Tamr Categorization projects"""
import logging
from typing import List

from tamr_unify_client.categorization.project import CategorizationProject
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.utils import operation
from tamr_toolbox.workflow.jobs import monitor

LOGGER = logging.getLogger(__name__)


def _run_custom(
    project: CategorizationProject,
    *,
    run_update_unified_dataset: bool = False,
    run_apply_feedback: bool = False,
    run_update_results: bool = False,
    process_asynchronously: bool = False,
    optimize: bool = False
) -> List[Operation]:
    """Executes specified steps of a categorization project.

    Args:
        project: The target categorization project
        run_update_unified_dataset: Whether refresh should be called on the unified dataset
        run_apply_feedback: Whether train should be called on the pair matching model
        run_update_results: Whether predict should be called on the pair matching model
        process_asynchronously: Whether ot not to 'enforce_success' must be set to True for concurrent workflow
        optimize: Whether or not to check to see if UD needs to be updated before trying to update it

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
        # if optimization is turned on return no-op if the UD is streamable
        if optimize and project.unified_dataset().status().is_streamable:
            op = Operation(
                project.client,
                data={
                    "relativeId": -1,
                    "state": "SUCCEEDED",
                    "description": f"No-op since {project.unified_dataset().name} is streamable.",
                },
            )
        else:
            op = project.unified_dataset().refresh()
            updated_op = monitor(op)

        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_apply_feedback:
        LOGGER.info(
            f"Applying feedback to the categorization model for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.model().train()
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_update_results:
        LOGGER.info(
            f"Updating categorization results for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.model().predict()
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)

    return completed_operations


def run(project: CategorizationProject, *, run_apply_feedback: bool = False, **kwargs) -> List[Operation]:
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
        **kwargs
    )


def update_unified_dataset(project: CategorizationProject, **kwargs) -> List[Operation]:
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
        **kwargs
    )


def apply_feedback_and_update_results(project: CategorizationProject, **kwargs) -> List[Operation]:
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
        **kwargs
    )


def update_results_only(project: CategorizationProject, **kwargs) -> List[Operation]:
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
        **kwargs
    )
