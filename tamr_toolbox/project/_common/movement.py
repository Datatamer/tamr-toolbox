from typing import List, Optional
from time import sleep
import logging

from tamr_unify_client.project.resource import Project
from tamr_unify_client import Client

from tamr_toolbox import utils
from tamr_toolbox.utils.operation import Operation

LOGGER = logging.getLogger(__name__)


def export_project(
        tamr_project: Project, *, directory_path: str, exclude_artifacts: Optional[List[str]] = []
) -> Operation:
    tamr_client = tamr_project.client
    utils.version.enforce_after_or_equal(
        client=tamr_client, compare_version="2021.005.0"
    )
    logging.info("Tamr version has movement compatibility.")

    body = {"artifactDirectory": directory_path, "excludeArtifacts": exclude_artifacts}
    logging.info(f"Sending query to export project, {tamr_project.name}.")
    response = tamr_client.post(
        f"/api/versioned/v1/projects/{tamr_project.resource_id}:export", json=body
    )
    response.successful()
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


def import_new_project(
        tamr_client: Client,
        project_artifact_path: str,
        new_project_name: str = None,
        new_unified_dataset_name: Optional[str] = None,
        exclude_artifacts: Optional[List[str]] = [],
        include_additive_artifacts: Optional[List[str]] = [],
        include_destructive_artifacts: Optional[List[str]] = [],
) -> Operation:
    utils.version.enforce_after_or_equal(
        client=tamr_client, compare_version="2021.005.0"
    )
    logging.info("Tamr version has movement compatibility.")

    body = {
        "excludeArtifacts": exclude_artifacts,
        "failIfNotPresent": False,
        "includeAdditiveArtifacts": include_additive_artifacts,
        "includeDestructiveArtifacts": include_destructive_artifacts,
        "newProjectName": new_project_name,
        "newUnifiedDatasetName": new_unified_dataset_name,
        "projectArtifact": project_artifact_path,
    }

    response = tamr_client.post('/v1/projects:import', json=body)
    response.successful()
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
