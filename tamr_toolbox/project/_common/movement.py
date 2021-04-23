"""Tasks related to project movement as part of Tamr projects"""
from typing import List, Optional
from time import sleep
import logging

from tamr_unify_client.project.resource import Project
from tamr_unify_client import Client

from tamr_toolbox import utils
from tamr_toolbox.utils.operation import Operation

LOGGER = logging.getLogger(__name__)


def export_project(
        project: Project, *, artifact_directory_path: str, exclude_artifacts: Optional[List[str]] = []
) -> Operation:
    """Export project artifacts for project movement

    Version:
        Requires Tamr 2021.005.0 or later

    Args:
        project: a tamr project object
        artifact_directory_path: export directory for project artifacts
        exclude_artifacts: list of artifacts to exclude

    Returns:
        operation for project export api call
    """
    # check version compatibility for project movement
    tamr_client = project.client
    utils.version.enforce_after_or_equal(
        client=tamr_client, compare_version="2021.005.0"
    )
    logging.info("Tamr version has movement compatibility.")

    # make project export api request
    body = {"artifactDirectory": artifact_directory_path, "excludeArtifacts": exclude_artifacts}
    logging.info(f"Sending query to export project, {project.name}.")
    response = tamr_client.post(
        f"/api/versioned/v1/projects/{project.resource_id}:export", json=body
    )
    response.successful()

    # periodically check export job until completion
    job_id = response.json()["id"]
    operation = utils.operation.from_resource_id(tamr=tamr_client, job_id=job_id)
    logging.info(f"Waiting for export to be created.")
    while operation.state == "RUNNING":
        sleep(3)
        operation = utils.operation.from_resource_id(
            tamr=tamr_client, job_id=job_id
        )
    utils.operation.enforce_success(operation)
    logging.info("Export completed successfully.")

    return operation


def import_project(
        *,
        tamr_client: Client,
        project_artifact_path: str,
        new_project_name: str = None,
        new_unified_dataset_name: Optional[str] = None,
        exclude_artifacts: Optional[List[str]] = [],
        include_additive_artifacts: Optional[List[str]] = [],
        include_destructive_artifacts: Optional[List[str]] = [],
) -> Operation:
    """Import project artifacts into a tamr instance

    Version:
        Requires Tamr 2021.005.0 or later

    Args:
        tamr_client: : a tamr client
        project_artifact_path: project artifacts zip filepath
        new_project_name: new project name
        new_unified_dataset_name: new unified dataset name
        exclude_artifacts: list of artifacts to exclude in import
        include_additive_artifacts: list of artifacts to import only additively
        include_destructive_artifacts: list of artifacts to import destructively

    Returns:
        operation for project import api call
    """
    # check version compatibility for project movement
    utils.version.enforce_after_or_equal(
        client=tamr_client, compare_version="2021.005.0"
    )
    logging.info("Tamr version has movement compatibility.")

    # make project import api request
    body = {
        "projectArtifact": project_artifact_path,
        "newProjectName": new_project_name,
        "newUnifiedDatasetName": new_unified_dataset_name,
        "excludeArtifacts": exclude_artifacts,
        "includeAdditiveArtifacts": include_additive_artifacts,
        "includeDestructiveArtifacts": include_destructive_artifacts,
        "failIfNotPresent": False,
    }
    logging.info(f"Sending query to import project, {new_project_name}.")
    response = tamr_client.post('/v1/projects:import', json=body)
    response.successful()

    # periodically check import job until completion
    job_id = response.json()["id"]
    operation = utils.operation.from_resource_id(tamr=tamr_client, job_id=job_id)
    logging.info(f"Waiting for project to be imported.")
    while operation.state == "RUNNING":
        sleep(3)
        operation = utils.operation.from_resource_id(
            tamr=tamr_client, job_id=job_id
        )
    utils.operation.enforce_success(operation)
    logging.info("Project imported successfully.")

    return operation
