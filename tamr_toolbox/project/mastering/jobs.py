"""Tasks related to running jobs for Tamr Mastering projects"""
from typing import List
import logging

from tamr_unify_client.mastering.project import MasteringProject
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.utils import operation

LOGGER = logging.getLogger(__name__)


def _run_custom(
    project: MasteringProject,
    *,
    run_update_unified_dataset: bool = False,
    run_estimate_pair_counts: bool = False,
    run_generate_pairs: bool = False,
    run_apply_feedback: bool = False,
    run_update_pair_results: bool = False,
    run_update_high_impact_pairs: bool = False,
    run_update_cluster_results: bool = False,
    run_publish_clusters: bool = False,
    process_asynchronously: bool = False,
) -> List[Operation]:
    """Executes specified steps of a mastering project.

    Args:
        project: The target mastering project
        run_update_unified_dataset: Whether refresh should be called on the unified dataset
        run_estimate_pair_counts: Whether an estimate pairs job should be run
        run_generate_pairs: Whether refresh should be called on the pairs dataset
        run_apply_feedback: Whether train should be called on the pair matching model
        run_update_pair_results: Whether predict should be called on the pair matching model
        run_update_high_impact_pairs: Whether refresh should be called on the high impact pairs
            dataset
        run_update_cluster_results: Whether refresh should be called on the record clusters dataset
        run_publish_clusters: Whether refresh should be called on the published record clusters
            dataset
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run

    Raises:
        TypeError: if the `project` is not a Mastering project
    """
    if ProjectType[project.type] != ProjectType.DEDUP:
        error_msg = f"Cannot use as a mastering project. Project type: {project.type}"
        LOGGER.error(error_msg)
        raise TypeError(error_msg)
    else:
        project = project.as_mastering()

    completed_operations = []
    if run_update_unified_dataset:
        LOGGER.info(
            f"Updating the unified dataset for project {project.name} (id={project.resource_id})."
        )
        op = project.unified_dataset().refresh(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_estimate_pair_counts:
        LOGGER.info(f"Estimate pair counts for project {project.name} (id={project.resource_id}).")
        op = project.estimate_pairs().refresh(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_generate_pairs:
        LOGGER.info(f"Generating pairs for project {project.name} (id={project.resource_id}).")
        op = project.pairs().refresh(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_apply_feedback:
        LOGGER.info(
            f"Applying feedback to the pairs model for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.pair_matching_model().train(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_update_pair_results:
        LOGGER.info(
            f"Updating pair prediction results for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.pair_matching_model().predict(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_update_high_impact_pairs:
        LOGGER.info(
            f"Refreshing high impact pairs for project {project.name} (id={project.resource_id})."
        )
        op = project.high_impact_pairs().refresh(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_update_cluster_results:
        LOGGER.info(
            f"Updating cluster prediction results for project {project.name} "
            f"(id={project.resource_id})."
        )
        op = project.record_clusters().refresh(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)
    if run_publish_clusters:
        LOGGER.info(f"Publishing clusters for project {project.name} (id={project.resource_id}).")
        op = project.published_clusters().refresh(asynchronous=process_asynchronously)
        if not process_asynchronously:
            operation.enforce_success(op)
        completed_operations.append(op)

    return completed_operations


def run(
    project: MasteringProject,
    *,
    run_estimate_pair_counts: bool = False,
    run_apply_feedback: bool = False,
    process_asynchronously: bool = False,
) -> List[Operation]:
    """Run the existing pipeline without training

    Args:
        project: Target mastering project
        run_estimate_pair_counts: Whether an estimate pairs job should be run
        run_apply_feedback: Whether train should be called on the pair matching model
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=True,
        run_estimate_pair_counts=run_estimate_pair_counts,
        run_generate_pairs=True,
        run_apply_feedback=run_apply_feedback,
        run_update_pair_results=True,
        run_update_high_impact_pairs=True,
        run_update_cluster_results=True,
        run_publish_clusters=True,
        process_asynchronously=process_asynchronously,
    )


def update_unified_dataset(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Updates the unified dataset for a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=True,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=False,
        run_update_pair_results=False,
        run_update_cluster_results=False,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def estimate_pair_counts(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Estimates the number of pairs for a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=True,
        run_generate_pairs=False,
        run_apply_feedback=False,
        run_update_pair_results=False,
        run_update_cluster_results=False,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def generate_pairs(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Generates the pairs for a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=True,
        run_apply_feedback=False,
        run_update_pair_results=False,
        run_update_cluster_results=False,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def apply_feedback(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """
    Applies feedback to update the model for a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=True,
        run_update_pair_results=False,
        run_update_cluster_results=False,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def update_pair_predictions(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """
    Updates pair predictions only.

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=False,
        run_update_pair_results=True,
        run_update_cluster_results=False,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def update_clusters(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """
    Re-runs clustering only.

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=False,
        run_update_pair_results=False,
        run_update_cluster_results=True,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def apply_feedback_and_update_results(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Trains the model, predicts the pair labels, and updates the draft clusters of
    a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=True,
        run_update_pair_results=True,
        run_update_high_impact_pairs=True,
        run_update_cluster_results=True,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def update_results_only(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Predicts the pair labels based on the existing pair model and updates the draft clusters
    of a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=False,
        run_update_pair_results=True,
        run_update_high_impact_pairs=True,
        run_update_cluster_results=True,
        run_publish_clusters=False,
        process_asynchronously=process_asynchronously,
    )


def publish_clusters(
    project: MasteringProject, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Publishes the clusters of a mastering project

    Args:
        project: Target mastering project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_update_unified_dataset=False,
        run_estimate_pair_counts=False,
        run_generate_pairs=False,
        run_apply_feedback=False,
        run_update_pair_results=False,
        run_update_cluster_results=False,
        run_publish_clusters=True,
        process_asynchronously=process_asynchronously,
    )
