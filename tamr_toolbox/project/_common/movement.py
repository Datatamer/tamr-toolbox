"""Tasks related to project movement as part of Tamr projects"""
from typing import List, Optional
import logging
from urllib import request

from tamr_unify_client.project.resource import Project
from tamr_unify_client import Client

from tamr_toolbox import utils
from tamr_toolbox.utils.operation import Operation

LOGGER = logging.getLogger(__name__)


def export_artifacts(
    *,
    project: Project,
    artifact_directory_path: str,
    exclude_artifacts: Optional[List[str]] = None,
    asynchronous: bool = False,
) -> Operation:
    """Export project artifacts for project movement

    Version:
        Requires Tamr 2021.005.0 or later

    Args:
        project: a tamr project object
        artifact_directory_path: export directory for project artifacts
        exclude_artifacts: list of artifacts to exclude
        asynchronous: flag to run function asynchronously

    Returns:
        operation for project export api call
    """
    # initializing empty lists if any
    if exclude_artifacts is None:
        exclude_artifacts = []

    # check version compatibility for project movement
    utils.version.enforce_after_or_equal(client=project.client, compare_version="2021.005.0")

    # make project export api request
    body = {"artifactDirectory": artifact_directory_path, "excludeArtifacts": exclude_artifacts}
    LOGGER.info(f"Starting to export {project.name} of type {project.type}.")
    response = project.client.post(
        f"/api/versioned/v1/projects/{project.resource_id}:export", json=body
    )
    # Raise error if export api call not successful
    if not response.ok:
        error_message = f"Error exporting project artifacts: {response.json()}"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    # get operation
    job_id = response.json()["id"]
    operation = utils.operation.from_resource_id(tamr=project.client, job_id=job_id)
    if not asynchronous:
        # periodically check export job until completion
        LOGGER.info(f"Waiting for export to be created.")
        operation = operation.wait()
        utils.operation.enforce_success(operation)
        LOGGER.info(f"Export complete: {operation}.")
    else:
        LOGGER.info(f"Export called asynchronously: {operation}.")

    return operation


def import_artifacts(
    *,
    project_artifact_path: str,
    tamr_client: Client,
    target_project: Project = None,
    new_project_name: str = None,
    new_unified_dataset_name: Optional[str] = None,
    exclude_artifacts: Optional[List[str]] = None,
    include_additive_artifacts: Optional[List[str]] = None,
    include_destructive_artifacts: Optional[List[str]] = None,
    fail_if_not_present: bool = False,
    asynchronous: bool = False,
    overwrite_existing: bool = False,
) -> Operation:
    """Import project artifacts into a tamr instance

    Version:
        Requires Tamr 2021.005.0 or later

    Args:
        tamr_client: a tamr client
        project_artifact_path: project artifacts zip filepath
        target_project: an optional target project for migration
        new_project_name: new project name
        new_unified_dataset_name: new unified dataset name
        exclude_artifacts: list of artifacts to exclude in import
        include_additive_artifacts: list of artifacts to import only additively
        include_destructive_artifacts: list of artifacts to import destructively
        fail_if_not_present: flag to fail project if not already present in instance
        asynchronous: flag to run function asynchronously
        overwrite_existing: flag to overwrite existing project artifacts

    Returns:
        operation for project import api call
    """
    # initializing empty lists if any
    if exclude_artifacts is None:
        exclude_artifacts = []
    if include_additive_artifacts is None:
        include_additive_artifacts = []
    if include_destructive_artifacts is None:
        include_destructive_artifacts = []

    # handle spaces in project artifact path
    if " " in project_artifact_path:
        project_artifact_path = request.pathname2url(project_artifact_path)

    # check version compatibility for project movement
    utils.version.enforce_after_or_equal(client=tamr_client, compare_version="2021.005.0")

    # make project import api request
    body = {
        "projectArtifact": project_artifact_path,
        "excludeArtifacts": exclude_artifacts,
        "includeAdditiveArtifacts": include_additive_artifacts,
        "includeDestructiveArtifacts": include_destructive_artifacts,
        "failIfNotPresent": fail_if_not_present,
    }

    if target_project:
        if new_project_name or new_unified_dataset_name:
            error_message = (
                "Cannot import to existing project and simultaneously set "
                "new_project_name or new_unified_dataset_name."
            )
            LOGGER.error(error_message)
            raise KeyError(error_message)
        if overwrite_existing:
            LOGGER.info(
                f"Starting to import artifacts from {project_artifact_path} "
                f"into existing project {target_project.name}"
                f"with id {target_project.resource_id}."
            )
            response = tamr_client.post(
                f"/api/versioned/v1/projects/{target_project.resource_id}:import", json=body
            )
        else:
            error_message = "Unable to overwrite existing project; overwrite flag is off."
            LOGGER.error(error_message)
            raise KeyError(error_message)
    else:
        if new_project_name in [p.name for p in tamr_client.projects]:
            error_message = f"New project name {new_project_name} already exists."
            LOGGER.error(error_message)
            raise ValueError(error_message)
        if new_unified_dataset_name in [ds.name for ds in tamr_client.datasets]:
            # otherwise response will be ok, but operation 'state'='FAILED'
            # and artifacts will be partially migrated : creates issues on re-run.
            error_message = f"New unified dataset name {new_unified_dataset_name} already exists."
            LOGGER.error(error_message)
            raise ValueError(error_message)
        body["newProjectName"] = new_project_name
        body["newUnifiedDatasetName"] = new_unified_dataset_name
        LOGGER.info(
            f"Starting to import artifacts from {project_artifact_path} "
            f"into new project {new_project_name}."
        )
        response = tamr_client.post("/api/versioned/v1/projects:import", json=body)

    # Raise exception if import was not successful
    if not response.ok:
        error_message = f"Error importing project artifacts: {response.json()}"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    # get operation
    job_id = response.json()["id"]
    operation = utils.operation.from_resource_id(tamr=tamr_client, job_id=job_id)
    if not asynchronous:
        # periodically check export job until completion
        LOGGER.info(f"Waiting for project to be imported.")
        operation = operation.wait()
        utils.operation.enforce_success(operation)
        LOGGER.info(f"Import complete: {operation}.")
    else:
        LOGGER.info(f"Import called asynchronously: {operation}.")

    return operation
