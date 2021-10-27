"""Tasks related to running jobs for Tamr Golden Records projects"""
from typing import List
import logging

from tamr_unify_client.operation import Operation
from tamr_unify_client.project.resource import Project

from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.utils import operation
from tamr_toolbox.utils import version

LOGGER = logging.getLogger(__name__)


def _run_custom(
    project: Project,
    *,
    run_profile_golden_records: bool = False,
    run_update_golden_records: bool = False,
    run_publish_golden_records: bool = False,
    process_asynchronously: bool = False,
) -> List[Operation]:
    """Executes specified steps of a golden records project.

    Args:
        project: The target golden records project
        run_profile_golden_records: Whether reprofile should be called on the golden records
        project
        run_update_golden_records: Whether refresh should be called on the draft golden records
            dataset
        run_publish_golden_records: Whether refresh should be called on the published golden
            records dataset
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run

    Raises:
        TypeError: if the `project` is not a Golden Record project
    """
    version.enforce_after_or_equal(project.client, compare_version="2020.004.0")

    if ProjectType[project.type] != ProjectType.GOLDEN_RECORDS:
        error_msg = f"Cannot use as a golden records project. Project type: {project.type}"
        LOGGER.error(error_msg)
        raise TypeError(error_msg)

    completed_operations = []
    if run_profile_golden_records:
        LOGGER.info(
            f"Updating all profiling information for golden records project {project.name} "
            f"(id={project.resource_id})."
        )
        resp = project.client.post(
            f"/api/versioned/v1/projects/{project.resource_id}/goldenRecordsProfile:refresh"
        ).successful()

        op = Operation.from_response(client=project.client, response=resp)
        if not process_asynchronously:
            op = op.wait()
            operation.enforce_success(op)
        completed_operations.append(op)

    if run_update_golden_records:
        LOGGER.info(
            f"Updating the draft golden records for project {project.name} "
            f"(id={project.resource_id})."
        )
        resp = project.client.post(
            f"/api/versioned/v1/projects/{project.resource_id}/goldenRecords:refresh"
        ).successful()

        op = Operation.from_response(client=project.client, response=resp)
        if not process_asynchronously:
            op = op.wait()
            operation.enforce_success(op)
        completed_operations.append(op)

    if run_publish_golden_records:
        LOGGER.info(
            f"Publishing golden records for project {project.name} (id={project.resource_id})."
        )
        resp = project.client.post(
            f"/api/versioned/v1/projects/{project.resource_id}/publishedGoldenRecords:refresh"
            f"?validate=true&version=CURRENT"
        ).successful()

        op = Operation.from_response(client=project.client, response=resp)
        if not process_asynchronously:
            op = op.wait()
            operation.enforce_success(op)
        completed_operations.append(op)

    return completed_operations


def run(project: Project, *, process_asynchronously: bool = False) -> List[Operation]:
    """Run the project

    Args:
        project: Target golden records project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_profile_golden_records=True,
        run_update_golden_records=True,
        run_publish_golden_records=True,
        process_asynchronously=process_asynchronously,
    )


def update_profiling_info(
    project: Project, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Updating all profiling information for golden records project

    Args:
        project: Target golden records project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_profile_golden_records=True,
        run_update_golden_records=False,
        run_publish_golden_records=False,
        process_asynchronously=process_asynchronously,
    )


def update_golden_records(
    project: Project, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Update the draft golden records of a project

    Args:
        project: Target golden records project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_profile_golden_records=False,
        run_update_golden_records=True,
        run_publish_golden_records=False,
        process_asynchronously=process_asynchronously,
    )


def publish_golden_records(
    project: Project, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Publish the  golden records of a project

    Args:
        project: Target golden records project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_profile_golden_records=False,
        run_update_golden_records=False,
        run_publish_golden_records=True,
        process_asynchronously=process_asynchronously,
    )


def update_and_publish(
    project: Project, *, process_asynchronously: bool = False
) -> List[Operation]:
    """Update the draft golden records and publish the golden records

    Args:
        project: Target golden records project
        process_asynchronously: Whether or not to wait for the job to finish before returning
            - must be set to True for concurrent workflow

    Returns:
        The operations that were run
    """
    return _run_custom(
        project,
        run_profile_golden_records=False,
        run_update_golden_records=True,
        run_publish_golden_records=True,
        process_asynchronously=process_asynchronously,
    )
